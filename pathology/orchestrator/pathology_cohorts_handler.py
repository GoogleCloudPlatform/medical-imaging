# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""Handler for PathologyCohorts rpc methods of DigitalPathology service.

Implementation of PathologyCohorts rpc methods for DPAS API.
"""

import base64
import collections
import re
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

from absl import flags
from google.api_core import exceptions
from google.cloud import spanner
from google.cloud import storage as gcs
from google.cloud.spanner_v1 import snapshot as s
from google.cloud.spanner_v1 import transaction as tr
from google.cloud.spanner_v1.streamed import StreamedResultSet
from googleapiclient import discovery
import grpc

from google.longrunning import operations_pb2
from google.protobuf import any_pb2
from pathology.orchestrator import filter_file_generator
from pathology.orchestrator import gcs_util
from pathology.orchestrator import healthcare_api_const
from pathology.orchestrator import id_generator
from pathology.orchestrator import logging_util
from pathology.orchestrator import pathology_resources_util
from pathology.orchestrator import pathology_slides_handler
from pathology.orchestrator import pathology_users_handler
from pathology.orchestrator import rpc_status
from pathology.orchestrator.spanner import schema_resources
from pathology.orchestrator.spanner import spanner_resource_converters
from pathology.orchestrator.spanner import spanner_util
from pathology.orchestrator.v1alpha import cohorts_pb2
from pathology.orchestrator.v1alpha import digital_pathology_pb2
from pathology.orchestrator.v1alpha import slides_pb2
from pathology.orchestrator.v1alpha import users_pb2
from pathology.shared_libs.flags import flag_utils
from pathology.shared_libs.flags import secret_flag_utils
from pathology.shared_libs.logging_lib import cloud_logging_client
from pathology.shared_libs.spanner_lib import cloud_spanner_client
from pathology.transformation_pipeline.ingestion_lib.dicom_util import dicom_standard


COHORT_EXPIRE_TIME_FLG = flags.DEFINE_integer(
    'cohort_expire_time',
    int(secret_flag_utils.get_secret_or_env('COHORT_EXPIRE_TIME', 30)),
    'Number of days after suspension that the cohort will be fully deleted. '
    'Default: 30 days.',
    lower_bound=1,
)

DEID_TAG_KEEP_LIST_FLG = flags.DEFINE_string(
    'deid_tag_keep_list',
    secret_flag_utils.get_secret_or_env('DEID_TAG_KEEP_LIST', ''),
    'GCS path (e.g., gs://bucket/file) to the .txt file or path to local GKE '
    'mounted configmap (e.g., /config/configuration_file) containing a comma '
    'or new line delimetered list of DICOM tag keywords to keep in a DeId '
    'operation.',
)

_DISABLE_KEEP_LIST_DICOM_TAG_KEYWORD_VALIDATION_FLG = flags.DEFINE_boolean(
    'disable_keep_list_tag_validation',
    flag_utils.env_value_to_bool(
        'DISABLE_KEEP_LIST_DICOM_TAG_KEYWORD_VALIDATION'
    ),
    'Disable DICOM tag keep list validation.',
)

_EMAIL_REGEX_FLG = flags.DEFINE_string(
    'valid_email_regex',
    r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$',
    'Regular expression to validate user emails against.',
)


_IMMUTABLE_STATE_FIELDS_FOR_UPDATE = [
    'is_deid',
    'cohort_stage',
    'cohort_behavior_constraints',
    'cohort_access',
    'update_time',
    'expire_time',
]
_METADATA_FIELDS = [
    field.name
    for field in cohorts_pb2.PathologyCohortMetadata.DESCRIPTOR.fields
]

ORCHESTRATOR_TABLE_NAMES = schema_resources.ORCHESTRATOR_TABLE_NAMES

_COHORTS_TABLE = ORCHESTRATOR_TABLE_NAMES[
    schema_resources.OrchestratorTables.COHORTS
]


def _validate_dicom_tag_keyword(
    keyword: str, disable_validation: bool
) -> Optional[str]:
  """Validates DICOM tag keyword is defined in DICOM standard.

  Args:
    keyword: DICOM tag keyword.
    disable_validation: If true disable validation test.

  Returns:
    DICOM tag keyword string or None if tag doesn't define valid keyword.
  """
  keyword = keyword.strip()  # remove white space surrounding keyword.
  if not keyword:
    return None
  if disable_validation:
    return keyword
  if dicom_standard.dicom_standard_util().get_keyword_address(keyword) is None:
    cloud_logging_client.warning(
        'DeID keeplist DICOM tag keyword is not recognized; ignoring.',
        {'keyword': keyword},
    )
    return None
  return keyword


def _validate_email_format(email: str) -> bool:
  """Validates that a string fits an email format.

  Args:
    email: Email string to validate.

  Returns:
    True if email is valid, False otherwise.
  """
  regex = re.compile(_EMAIL_REGEX_FLG.value)
  return bool(regex.fullmatch(email))


def _parse_deid_keep_list(deid_keep_list: str) -> List[str]:
  """Parses DeID tag keyword keep list string into list of DICOM keywords.

  Args:
    deid_keep_list: String containing comma or line separated DICOM tag keywords

  Returns:
    List of validated keywords.
  """
  keyword_list = []
  disable_validation = _DISABLE_KEEP_LIST_DICOM_TAG_KEYWORD_VALIDATION_FLG.value
  # DICOM tag keywords do not contain commas, treat all commas like \n
  for keyword in deid_keep_list.replace(',', '\n').split('\n'):
    keyword = _validate_dicom_tag_keyword(keyword, disable_validation)
    if keyword is None:
      continue
    keyword_list.append(keyword)
  return keyword_list


def _read_deid_keep_list_from_container_file(path: str) -> Optional[str]:
  """Reads DICOM container keep list from local file and returns contents.

  Args:
    path: Path to file to read.

  Returns:
    Contents of file.

  Raises:
    RpcFailureError: If file cannot be found.
  """
  if not path:
    return None
  cloud_logging_client.info(
      'Reading in Deid Tag Keep List from local file.', {'path': path}
  )
  try:
    with open(path, 'rt') as infile:
      return infile.read()
  except FileNotFoundError as exc:
    raise rpc_status.RpcFailureError(
        rpc_status.build_rpc_method_status_and_log(
            grpc.StatusCode.NOT_FOUND,
            f'Could not find local file {path}.',
            exp=exc,
        )
    ) from exc


def _read_deid_keep_list_from_gcs(path: str) -> Optional[str]:
  """Reads DICOM container keep list from GCS.

  Args:
    path: Format gs://bucket-name/blob-name.

  Returns:
    Contents of file.

  Raises:
    RpcFailureError: If file cannot be found.
  """
  cloud_logging_client.info(
      'Reading DICOM Deid Tag keyword Keep List from GCS.', {'path': path}
  )
  blob = gcs.Blob.from_string(path, gcs.Client())
  try:
    return blob.download_as_text()
  except exceptions.NotFound as exc:
    raise rpc_status.RpcFailureError(
        rpc_status.build_rpc_method_status_and_log(
            grpc.StatusCode.NOT_FOUND,
            f'Could not find GCS file {path}.',
            exp=exc,
        )
    ) from exc


def _read_deid_tag_keep_list() -> List[str]:
  """Returns list of tag keywords to keep in DICOM with all others redacted.

  Raises:
    rpc_status.RpcFailureError: Unable or empty tag keep list.
  """
  keeplist_path = DEID_TAG_KEEP_LIST_FLG.value.strip()
  if keeplist_path.startswith('gs://'):
    deid_keep_list = _read_deid_keep_list_from_gcs(keeplist_path)
  elif keeplist_path:
    deid_keep_list = _read_deid_keep_list_from_container_file(keeplist_path)
  else:
    raise rpc_status.RpcFailureError(
        rpc_status.build_rpc_method_status_and_log(
            grpc.StatusCode.INVALID_ARGUMENT,
            (
                'DEID_TAG_KEEP_LIST environmental variable is undefined;'
                ' specify a path to GCS (e.g., gs://bucket/file.txt) or a path'
                ' to GKE container volume mounted configmap (e.g.,'
                ' /config/my_config).'
            ),
        )
    )
  deid_keep_list = _parse_deid_keep_list(deid_keep_list)
  if not deid_keep_list:
    raise rpc_status.RpcFailureError(
        rpc_status.build_rpc_method_status_and_log(
            grpc.StatusCode.INVALID_ARGUMENT, 'DEID_TAG_KEEP_LIST is empty.'
        )
    )

  cloud_logging_client.info(
      'DeID DICOM tag keyword keep list.',
      {'DICOM tag keyword keep list': str(deid_keep_list)},
  )
  return deid_keep_list


def _get_cohort_owner_id(cohort_name: str) -> int:
  """Returns the cohort owner's UserId from the cohort resource name.

  Args:
    cohort_name: Cohort resource name
  """
  return int(cohort_name.split('/')[1])


def _get_is_active(cohort: cohorts_pb2.PathologyCohort) -> bool:
  """Returns True if the cohort is active.

  Args:
    cohort: Cohort instance to check stage of.

  Returns:
    True if active.
  """
  return (
      cohort.cohort_metadata.cohort_stage
      == cohorts_pb2.PATHOLOGY_COHORT_LIFECYCLE_STAGE_ACTIVE
      or cohort.cohort_metadata.cohort_stage
      == cohorts_pb2.PATHOLOGY_COHORT_LIFECYCLE_STAGE_UNSPECIFIED
  )


def _get_is_suspended(cohort: cohorts_pb2.PathologyCohort) -> bool:
  """Returns True if the cohort is in a suspended state.

  Args:
    cohort: Cohort instance to check stage of.

  Returns:
    True if active.
  """
  return (
      cohort.cohort_metadata.cohort_stage
      == cohorts_pb2.PATHOLOGY_COHORT_LIFECYCLE_STAGE_SUSPENDED
  )


def _get_dicom_store_from_dicom_slide_uri(dicom_slide_uri: str) -> str:
  """Returns the DICOM store path given a DICOM slide uri.

  Args:
    dicom_slide_uri: URI of DICOM slide instance.
  """
  return dicom_slide_uri.split('/dicomWeb')[0]


def _get_dicom_store_url(cohort: cohorts_pb2.PathologyCohort) -> str:
  """Returns the DICOM store url of the slides in a cohort.

  Args:
    cohort: Cohort to get DICOM store from.

  Raises:
    RpcFailureError if cohort contains no slides.
  """
  try:
    slide_uri = cohort.slides[0].dicom_uri
    return _get_dicom_store_from_dicom_slide_uri(slide_uri)
  except IndexError as ex:
    raise rpc_status.RpcFailureError(
        rpc_status.build_rpc_method_status_and_log(
            grpc.StatusCode.INVALID_ARGUMENT,
            f'Cohort {cohort.name} does not contain any slides to execute'
            ' operation on.',
            {'resource_name': cohort.name},
        )
    ) from ex


def _get_series_paths_from_instances(instances: List[str]) -> Set[str]:
  """Returns a set of series paths for a list of instances.

  Args:
    instances: List of DICOM instance paths.
  """
  return set(i.split('/instances')[0] for i in instances)


def _get_slides_in_dicom_store(
    cohort: cohorts_pb2.PathologyCohort,
) -> List[slides_pb2.PathologySlide]:
  """Returns a list of slides in the same DICOM store as the cohort.

  Args:
    cohort: Cohort to get slides for.
  """
  source_dicom_url = _get_dicom_store_url(cohort)
  slides = []
  for slide in cohort.slides:
    if (
        _get_dicom_store_from_dicom_slide_uri(slide.dicom_uri)
        == source_dicom_url
    ):
      slides.append(slide)
    else:
      cloud_logging_client.info(
          'Slide is from a different Dicom Store than other'
          'slides in the cohort. Skipping slide.',
          {'dicom_uri': slide.dicom_uri},
      )

  return slides


def _get_access_role_value(
    access_filter: str,
) -> Optional['users_pb2.PathologyUserAccessRole']:
  """Returns the access role to filter on in list cohorts or None.

  Args:
    access_filter: String of access role.

  Raises:
    ValueError if string doesn't match enum PathologyUserAccessRole.
  """
  if not access_filter:
    return None
  else:
    return users_pb2.PathologyUserAccessRole.Value(access_filter)


def _add_slide_to_cohort(
    transaction, slide: slides_pb2.PathologySlide, cohort_id: int
) -> int:
  """Adds a PathologySlide to a cohort and mapping in Spanner.

  Args:
    transaction: Transaction to run Spanner read/writes on.
    slide: PathologySlide to add.
    cohort_id: Id of cohort to add slide to.

  Returns:
    ScanUniqueId of slide.

  Raises:
    exceptions.NotFound if slide does not exist.
  """
  if not slide.scan_unique_id:
    # Lookup scan_uid if not provided.
    dicom_hash = id_generator.generate_hash_id(slide.dicom_uri)
    slide_row = transaction.read(
        ORCHESTRATOR_TABLE_NAMES[schema_resources.OrchestratorTables.SLIDES],
        schema_resources.SLIDES_BY_LOOKUP_HASH_COLS,
        spanner.KeySet(keys=[[dicom_hash, slide.dicom_uri]]),
        schema_resources.SLIDES_BY_LOOKUP_HASH,
    ).one()
    slide.scan_unique_id = str(
        slide_row[
            schema_resources.SLIDES_BY_LOOKUP_HASH_COLS.index(
                schema_resources.SCAN_UNIQUE_ID
            )
        ]
    )

  transaction.insert_or_update(
      ORCHESTRATOR_TABLE_NAMES[
          schema_resources.OrchestratorTables.COHORT_SLIDES
      ],
      schema_resources.COHORT_SLIDES_COLS,
      [[cohort_id, int(slide.scan_unique_id)]],
  )
  return int(slide.scan_unique_id)


def _build_cohort_from_row(
    cohort_row: Dict[str, Any],
) -> cohorts_pb2.PathologyCohort:
  """Builds a PathologyCohort from a spanner row.

  Args:
    cohort_row: Spanner row with cohorts data.

  Returns:
    PathologyCohort
  """
  resource_name = pathology_resources_util.convert_cohort_id_to_name(
      cohort_row[schema_resources.CREATOR_USER_ID],
      cohort_row[schema_resources.COHORT_ID],
  )
  metadata = cohorts_pb2.PathologyCohortMetadata(
      display_name=cohort_row[schema_resources.DISPLAY_NAME],
      description=cohort_row[schema_resources.DESCRIPTION],
      cohort_stage=cohort_row[schema_resources.COHORT_STAGE],
      is_deid=cohort_row[schema_resources.IS_DEID],
      cohort_behavior_constraints=cohort_row[
          schema_resources.COHORT_BEHAVIOR_CONSTRAINTS
      ],
      cohort_access=cohort_row[schema_resources.COHORT_ACCESS],
      update_time=cohort_row[
          schema_resources.COHORT_UPDATE_TIME
      ].timestamp_pb(),
  )
  if cohort_row[schema_resources.EXPIRE_TIME] is not None:
    metadata.expire_time.CopyFrom(
        cohort_row[schema_resources.EXPIRE_TIME].timestamp_pb()
    )
  return cohorts_pb2.PathologyCohort(
      name=resource_name, cohort_metadata=metadata
  )


def _copy_cohort(
    transaction,
    cohort: cohorts_pb2.PathologyCohort,
    dest_dicom_store: Optional[str],
    creator_id: int,
    is_deid: bool,
    cohort_stage: cohorts_pb2.PathologyCohortLifecycleStage,
    slides_to_include_filter: Optional[Set[str]] = None,
) -> int:
  """Copies a cohort to a destination dicom store.

  Args:
    transaction: Transaction to run spanner read/writes on.
    cohort: Cohort to copy.
    dest_dicom_store: Url of destination DICOM store to copy to.
    creator_id: User Id of user issuing copy.
    is_deid: Specifies whether copy of cohort will contain deidentified slides.
    cohort_stage: Cohort stage for new copy that is created.
    slides_to_include_filter: Slides to include in the cohort result. If None,
      all slides are included by default.

  Returns:
    int - Cohort id of new copy.
  """
  target_cohort_id = (
      id_generator.OrchestratorIdGenerator.generate_new_id_for_table(
          transaction,
          ORCHESTRATOR_TABLE_NAMES[schema_resources.OrchestratorTables.COHORTS],
          schema_resources.COHORTS_COLS,
      )
  )

  transaction.insert(
      ORCHESTRATOR_TABLE_NAMES[schema_resources.OrchestratorTables.COHORTS],
      schema_resources.COHORTS_COLS,
      [[
          target_cohort_id,
          creator_id,
          cohort.cohort_metadata.display_name,
          cohort.cohort_metadata.description,
          cohort_stage,
          is_deid,
          cohort.cohort_metadata.cohort_behavior_constraints,
          spanner.COMMIT_TIMESTAMP,
          cohorts_pb2.PATHOLOGY_COHORT_ACCESS_RESTRICTED,
          None,
      ]],
  )
  transaction.insert(
      ORCHESTRATOR_TABLE_NAMES[
          schema_resources.OrchestratorTables.COHORT_USER_ACCESS
      ],
      schema_resources.COHORT_USER_ACCESS_COLS,
      [[
          target_cohort_id,
          creator_id,
          users_pb2.PATHOLOGY_USER_ACCESS_ROLE_OWNER,
      ]],
  )

  # If slides are present, add mappings for slides.
  if cohort.slides:
    source_dicom_store = _get_dicom_store_url(cohort)
    # If destination is not provided, assume it is the same as source.
    if dest_dicom_store is None:
      dest_dicom_store = source_dicom_store

    for slide in cohort.slides:
      # Skip adding slide if filter exists and not included in slides filter.
      if (
          slides_to_include_filter is not None
          and filter_file_generator.get_full_path_from_dicom_uri(
              slide.dicom_uri
          )
          not in slides_to_include_filter
      ):
        cloud_logging_client.warning(
            'Skipping copying slide that is not included in slides filter.'
        )
        continue

      if dest_dicom_store == source_dicom_store:
        slide.scan_unique_id = str(
            _add_slide_to_cohort(transaction, slide, target_cohort_id)
        )
        continue
      target_dicom_uri = (
          f'{dest_dicom_store}/dicomWeb/{slide.dicom_uri.split("dicomWeb/")[1]}'
      )
      target_slide = slides_pb2.PathologySlide(dicom_uri=target_dicom_uri)
      try:
        # Creates a new scan unique id for slide.
        target_slide.scan_unique_id = str(
            pathology_slides_handler.insert_slide_in_table(
                transaction, target_slide
            )
        )
      except rpc_status.RpcFailureError as ex:
        if ex.status.code != grpc.StatusCode.ALREADY_EXISTS:
          raise ex
        # Continue to add existing slide if slide already exists.
        cloud_logging_client.info(
            'Row already exists for DICOM URI in table. Adding existing'
            f' slide. ScanUniqueId: {target_slide.scan_unique_id}.'
        )
      _add_slide_to_cohort(transaction, target_slide, target_cohort_id)

  return target_cohort_id


def _insert_cohort_in_table(
    transaction, creator_id: int, cohort: cohorts_pb2.PathologyCohort
) -> int:
  """Inserts cohort and mappings to slides into respective spanner tables.

  Args:
    transaction: Transaction to run spanner read/writes on.
    creator_id: User Id of cohort creator.
    cohort: Cohort to insert.

  Returns:
    int cohort_id of new cohort row
  """
  # Generate new cohort id.
  cohort_id = id_generator.OrchestratorIdGenerator.generate_new_id_for_table(
      transaction, _COHORTS_TABLE, schema_resources.COHORTS_COLS
  )
  # Insert row in cohorts table.
  transaction.insert(
      _COHORTS_TABLE,
      schema_resources.COHORTS_COLS,
      [[
          cohort_id,
          creator_id,
          cohort.cohort_metadata.display_name,
          cohort.cohort_metadata.description,
          cohort.cohort_metadata.cohort_stage,
          cohort.cohort_metadata.is_deid,
          cohort.cohort_metadata.cohort_behavior_constraints,
          spanner.COMMIT_TIMESTAMP,
          cohort.cohort_metadata.cohort_access,
          None,
      ]],
  )

  if cohort.slides:
    dicom_store_slides = _get_slides_in_dicom_store(cohort)
    cohort.ClearField('slides')
    cohort.slides.extend(dicom_store_slides)
    # Add mappings for slides.
    for slide in cohort.slides:
      try:
        # Try adding existing slide to cohort.
        slide.scan_unique_id = str(
            _add_slide_to_cohort(transaction, slide, cohort_id)
        )
      except exceptions.NotFound:
        # Slide does not exist. Create slide and add to cohort.
        slide.scan_unique_id = str(
            pathology_slides_handler.insert_slide_in_table(transaction, slide)
        )
        _add_slide_to_cohort(transaction, slide, cohort_id)
  # Add access permissions for cohort.
  transaction.insert(
      ORCHESTRATOR_TABLE_NAMES[
          schema_resources.OrchestratorTables.COHORT_USER_ACCESS
      ],
      schema_resources.COHORT_USER_ACCESS_COLS,
      [[cohort_id, creator_id, users_pb2.PATHOLOGY_USER_ACCESS_ROLE_OWNER]],
  )
  return cohort_id


def _get_cohort_access_for_caller(
    snapshot: s.Snapshot, cohort_ids: List[int], caller_id: int
) -> Dict[int, List[users_pb2.PathologyUserAccess]]:
  """Retrieves only caller's access permissions for a list of cohorts.

  Args:
    snapshot: Read-only snapshot of database.
    cohort_ids: List of Cohort Ids to retrieve permissions for.
    caller_id: UserId of caller to look up.

  Returns:
    Dict[cohort_id, List of access permissions].
  """
  result = {}
  caller_email = spanner_util.get_email_by_user_id(snapshot, caller_id)

  if caller_email is None:
    cloud_logging_client.error(
        f'User {caller_id} has no associated email alias.'
    )
    return result

  table_name = ORCHESTRATOR_TABLE_NAMES[
      schema_resources.OrchestratorTables.COHORT_USER_ACCESS
  ]
  rows = snapshot.execute_sql(
      f'SELECT * FROM {table_name} AS c WHERE '
      f'c.{schema_resources.COHORT_ID} IN UNNEST(@key_list)'
      f' AND {schema_resources.USER_ID}=@user_id',
      params={'key_list': cohort_ids, 'user_id': caller_id},
      param_types={
          'key_list': spanner.param_types.Array(spanner.param_types.INT64),
          'user_id': spanner.param_types.INT64,
      },
  )
  for r in rows:
    access_role = users_pb2.PathologyUserAccessRole.Name(
        r[
            schema_resources.COHORT_USER_ACCESS_COLS.index(
                schema_resources.ACCESS_ROLE
            )
        ]
    )

    result[
        r[
            schema_resources.COHORT_USER_ACCESS_COLS.index(
                schema_resources.COHORT_ID
            )
        ]
    ] = [
        users_pb2.PathologyUserAccess(
            user_email=caller_email, access_role=access_role
        )
    ]

  return result


def _get_full_cohorts_access(
    snapshot: s.Snapshot, cohort_ids: Set[int]
) -> Mapping[int, List[users_pb2.PathologyUserAccess]]:
  """Performs a Spanner look up to retrieve access permissions for cohorts.

  Args:
    snapshot: Read-only snapshot of database.
    cohort_ids: Cohort Ids of cohorts to look up.

  Returns:
    Map of cohort id to list of access permissions.
  """
  if not cohort_ids:
    return {}

  # Fetch user access
  table_name = ORCHESTRATOR_TABLE_NAMES[
      schema_resources.OrchestratorTables.COHORT_USER_ACCESS
  ]
  rows = snapshot.execute_sql(
      f'SELECT {schema_resources.COHORT_ID}, {schema_resources.USER_ID}, '
      f'{schema_resources.ACCESS_ROLE} FROM {table_name} '
      f'WHERE {schema_resources.COHORT_ID} IN UNNEST(@cohort_list)',
      params={
          'cohort_list': list(cohort_ids),
      },
      param_types={
          'cohort_list': spanner.param_types.Array(spanner.param_types.INT64),
      },
  )
  rows = list(rows)

  # Fetch corresponding user emails
  user_ids = set()
  for _, user_id, _ in rows:
    user_ids.add(user_id)
  user_id_to_email = spanner_util.get_emails_by_user_ids(
      snapshot, list(user_ids)
  )

  # Create permissions
  permissions = collections.defaultdict(list)
  for cohort_id, user_id, access_role in rows:
    if user_id in user_id_to_email:
      permissions[cohort_id].append(
          users_pb2.PathologyUserAccess(
              user_email=user_id_to_email[user_id],
              access_role=users_pb2.PathologyUserAccessRole.Name(access_role),
          )
      )

  # TODO: Remove once data is backfilled.
  # Temporary fix for old data that doesn't have CohortUserAccess populated.
  # Adds owner access for CreatorUserId to permissions list.
  if not permissions:
    for cohort_id in cohort_ids:
      owner_id = spanner_util.get_cohort_owner(snapshot, cohort_id)
      owner_email = spanner_util.get_email_by_user_id(snapshot, owner_id)
      if owner_email:
        permissions[cohort_id].append(
            users_pb2.PathologyUserAccess(
                user_email=owner_email,
                access_role=users_pb2.PathologyUserAccessRole.Name(
                    users_pb2.PATHOLOGY_USER_ACCESS_ROLE_OWNER
                ),
            )
        )

  if not permissions:
    cloud_logging_client.warning(
        f'Cohorts {cohort_ids} have no users with access permissions.'
    )
  return permissions


def _get_full_cohort_access(
    snapshot: s.Snapshot, cohort_id: int
) -> List[users_pb2.PathologyUserAccess]:
  """Performs a Spanner look up to retrieve access permissions for cohort.

  Args:
    snapshot: Read-only snapshot of database.
    cohort_id: Cohort Id of cohort to look up.

  Returns:
    List of access permissions.
  """
  id_to_permissions = _get_full_cohorts_access(snapshot, {cohort_id})
  return id_to_permissions.get(cohort_id, [])


def _get_slides_for_cohorts(
    snapshot: s.Snapshot, cohort_ids: List[int]
) -> Dict[int, List[slides_pb2.PathologySlide]]:
  """Returns a map of cohort ids to the slides in them.

  Args:
    snapshot: Read-only snapshot of database.
    cohort_ids: Cohort Ids to look up.
  """
  result = collections.defaultdict(list)
  cohort_slides_dict = collections.defaultdict(list)
  cohort_slides_tbl = ORCHESTRATOR_TABLE_NAMES[
      schema_resources.OrchestratorTables.COHORT_SLIDES
  ]
  cohort_slides_rows = snapshot.execute_sql(
      f'SELECT * FROM {cohort_slides_tbl} as c WHERE'
      f' c.{schema_resources.COHORT_ID} IN'
      ' UNNEST(@key_list);',
      params={'key_list': cohort_ids},
      param_types={
          'key_list': spanner.param_types.Array(spanner.param_types.INT64)
      },
  )
  for row in cohort_slides_rows:
    cohort_slide_row = dict(zip(schema_resources.COHORT_SLIDES_COLS, row))
    cohort_id = cohort_slide_row[schema_resources.COHORT_ID]
    cohort_slides_dict[cohort_id].append(
        cohort_slide_row[schema_resources.SCAN_UNIQUE_ID]
    )

  for cohort_id, scan_ids in cohort_slides_dict.items():
    slides_tbl = ORCHESTRATOR_TABLE_NAMES[
        schema_resources.OrchestratorTables.SLIDES
    ]
    slide_rows = snapshot.execute_sql(
        f'SELECT * FROM {slides_tbl} as s WHERE '
        f's.{schema_resources.SCAN_UNIQUE_ID} IN UNNEST(@key_list);',
        params={'key_list': scan_ids},
        param_types={
            'key_list': spanner.param_types.Array(spanner.param_types.INT64)
        },
    )
    for slide_row in slide_rows:
      slide_dict = dict(zip(schema_resources.SLIDES_COLS, slide_row))
      slide = pathology_slides_handler.build_slide_resource(
          slide_dict[schema_resources.SCAN_UNIQUE_ID],
          slide_dict[schema_resources.DICOM_URI],
      )
      result[cohort_id].append(slide)
  return result


def _get_cohort_by_id(
    snapshot: s.Snapshot, alias: str, cohort_id: int, include_slides: bool
) -> cohorts_pb2.PathologyCohort:
  """Performs a Spanner look up of a cohort by id.

  Args:
    snapshot: Read-only snapshot of database
    alias: Alias of rpc caller.
    cohort_id: Id of cohort to look up.
    include_slides: True if slides are included in the returned cohort.

  Returns:
    PathologyCohort.

  Raises:
    RpcFailureError on failure.
  """
  row = snapshot.read(
      ORCHESTRATOR_TABLE_NAMES[schema_resources.OrchestratorTables.COHORTS],
      schema_resources.COHORTS_COLS,
      spanner.KeySet(keys=[[cohort_id]]),
  )

  try:
    cohort = _build_cohort_from_row(
        dict(zip(schema_resources.COHORTS_COLS, row.one()))
    )
  except exceptions.NotFound as exc:
    raise rpc_status.RpcFailureError(
        rpc_status.build_rpc_method_status_and_log(
            grpc.StatusCode.NOT_FOUND,
            f'Could not retrieve cohort {cohort_id}.',
            exp=exc,
        )
    ) from exc
  try:
    caller_id = spanner_util.search_user_by_alias(alias, snapshot)
    if spanner_util.check_if_cohort_owner_or_admin(
        snapshot, caller_id, cohort_id
    ):
      cohort.user_access.extend(_get_full_cohort_access(snapshot, cohort_id))
    else:
      cohort_user_access = _get_cohort_access_for_caller(
          snapshot, [cohort_id], caller_id
      )
      if cohort_id in cohort_user_access:
        cohort.user_access.extend(cohort_user_access[cohort_id])
  except exceptions.NotFound:
    cloud_logging_client.error(f'Could not find user with alias {alias}.')

  if include_slides:
    slides_dict = _get_slides_for_cohorts(snapshot, [cohort_id])
    if cohort_id in slides_dict:
      cohort.slides.extend(slides_dict[cohort_id])
  return cohort


def _get_cohorts_by_id(
    snapshot: s.Snapshot,
    user_id: int,
    cohort_ids: List[int],
    include_slides: bool,
) -> List[cohorts_pb2.PathologyCohort]:
  """Performs a Spanner retrieval of multiple active cohorts.

  Args:
    snapshot: Read only snapshot of database
    user_id: User id of user to retrieve cohorts for.
    cohort_ids: Ids of cohorts to retrieve.
    include_slides: True if slides are included in the returned cohort.

  Returns:
    List[PathologyCohort].

  Raises:
    RpcFailureError on failure.
  """
  cohorts_dict = {}
  # Retrieve cohort rows.
  cohorts_tbl = ORCHESTRATOR_TABLE_NAMES[
      schema_resources.OrchestratorTables.COHORTS
  ]
  rows = snapshot.execute_sql(
      f'SELECT * FROM {cohorts_tbl} AS c WHERE'
      f' c.{schema_resources.COHORT_ID} IN UNNEST(@key_list) AND'
      f' c.{schema_resources.COHORT_STAGE}!= @suspended',
      params={
          'key_list': cohort_ids,
          'suspended': cohorts_pb2.PATHOLOGY_COHORT_LIFECYCLE_STAGE_SUSPENDED,
      },
      param_types={
          'key_list': spanner.param_types.Array(spanner.param_types.INT64),
          'suspended': spanner.param_types.INT64,
      },
  )

  for row in rows:
    cohort_row = dict(zip(schema_resources.COHORTS_COLS, row))
    cohort_id = cohort_row[schema_resources.COHORT_ID]
    try:
      cohort = _build_cohort_from_row(cohort_row)
    except exceptions.NotFound as exc:
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.NOT_FOUND,
              f'Could not retrieve cohort {cohort_id}.',
              exp=exc,
          )
      ) from exc

    cohorts_dict[cohort_id] = cohort

  # Retrieve permissions.
  caller_permissions_dict = _get_cohort_access_for_caller(
      snapshot, cohort_ids, user_id
  )
  owner_or_admin_cohort_ids = spanner_util.check_if_cohorts_owner_or_admin(
      snapshot, user_id, cohort_ids
  )
  owner_or_admin_cohort_access_dict = _get_full_cohorts_access(
      snapshot, owner_or_admin_cohort_ids
  )

  # Retrieve cohort slide rows if included.
  cohort_slides_dict = {}
  if include_slides:
    cohort_slides_dict = _get_slides_for_cohorts(snapshot, cohort_ids)

  # Add to cohort fields.
  result = []
  for cohort_id, cohort in cohorts_dict.items():
    # Add permissions.
    if cohort_id in owner_or_admin_cohort_access_dict:
      cohort.user_access.extend(owner_or_admin_cohort_access_dict[cohort_id])
    elif cohort_id in caller_permissions_dict:
      cohort.user_access.extend(caller_permissions_dict[cohort_id])

    # Add slides.
    if include_slides and cohort_id in cohort_slides_dict:
      cohort.slides.extend(cohort_slides_dict[cohort_id])
    result.append(cohort)

  return result


def _record_operation_in_table(
    transaction,
    operation_name: str,
    operation_metadata: digital_pathology_pb2.PathologyOperationMetadata,
    emails: Optional[List[str]],
) -> int:
  """Records an operation in the Operations table.

  Args:
    transaction: Transaction to run Spanner read/writes on.
    operation_name: Cloud resource name of Operation to be recorded.
    operation_metadata: Spanner representation of metadata for operation.
    emails: Notification Emails to store in table.

  Returns:
    int - DpasOperationId of recorded operation.
  """
  # Generate DpasOperationId.
  dpas_operation_id = (
      id_generator.OrchestratorIdGenerator.generate_new_id_for_table(
          transaction,
          ORCHESTRATOR_TABLE_NAMES[
              schema_resources.OrchestratorTables.OPERATIONS
          ],
          schema_resources.OPERATIONS_COLS,
      )
  )
  # Insert row in Operations table.
  if not emails:
    emails_array = None
  else:
    emails_array = list(emails)
  metadata_bytes = base64.b64encode(
      spanner_resource_converters.to_stored_operation_metadata(
          operation_metadata
      ).SerializeToString(deterministic=True)
  )
  transaction.insert(
      ORCHESTRATOR_TABLE_NAMES[schema_resources.OrchestratorTables.OPERATIONS],
      schema_resources.OPERATIONS_COLS,
      [[
          dpas_operation_id,
          operation_name,
          emails_array,
          metadata_bytes,
          schema_resources.OperationStatus.IN_PROGRESS.value,
          None,
          spanner.COMMIT_TIMESTAMP,
      ]],
  )

  return dpas_operation_id


def _remove_duplicate_slides(cohort: cohorts_pb2.PathologyCohort) -> None:
  """Removes duplicate slides in cohort proto repeated field.

  Args:
    cohort: Cohort proto instance to remove duplicate slides from.

  Returns:
    None
  """
  slide_hashes = set()
  unique_slides = []
  for slide in cohort.slides:
    slide_hash = id_generator.generate_hash_id(slide.dicom_uri)
    if slide_hash in slide_hashes:
      continue
    unique_slides.append(slide)
    slide_hashes.add(slide_hash)

  cohort.ClearField('slides')
  cohort.slides.extend(unique_slides)


def _convert_permissions_to_map(
    permissions: List[users_pb2.PathologyUserAccess],
) -> Dict[str, int]:
  """Converts list of user permissions to a map.

  Args:
    permissions: List of access permissions.

  Returns:
    Dict[user_email, access_role]
  """
  permissions_map = {}
  for user_permission in permissions:
    permissions_map[user_permission.user_email] = user_permission.access_role
  return permissions_map


def _convert_map_to_permissions(
    permissions_map: Dict[str, int],
) -> List[users_pb2.PathologyUserAccess]:
  """Converts map of user permissions to a list.

  Args:
    permissions_map: Map of user access permissions.

  Returns:
    List[access permissions]
  """
  permissions = []
  for user_email in permissions_map:
    permissions.append(
        users_pb2.PathologyUserAccess(
            user_email=user_email,
            access_role=users_pb2.PathologyUserAccessRole.Name(
                permissions_map[user_email]
            ),
        )
    )
  return permissions


def _remove_deleted_permissions_from_table(
    transaction: tr.Transaction, cohort_id: int, permissions_map: Dict[str, int]
) -> None:
  """Checks and removes any deleted permissions from Spanner.

  Args:
    transaction: Transaction to run spanner read/writes on.
    cohort_id: Cohort Id of cohort.
    permissions_map: Map of new permissions after updates.

  Returns:
    None
  """
  cohort_access_tbl = ORCHESTRATOR_TABLE_NAMES[
      schema_resources.OrchestratorTables.COHORT_USER_ACCESS
  ]
  cohort_access_rows = transaction.execute_sql(
      (
          f'SELECT {schema_resources.USER_ID} FROM {cohort_access_tbl} WHERE'
          f' {schema_resources.COHORT_ID}=@cohort_id;'
      ),
      params={'cohort_id': cohort_id},
      param_types={'cohort_id': spanner.param_types.INT64},
  )
  user_ids = spanner_util.search_users_by_aliases(
      list(permissions_map.keys()), transaction
  )
  for row in cohort_access_rows:
    user_id = row[0]
    if user_id not in user_ids:
      transaction.delete(
          cohort_access_tbl, spanner.KeySet(keys=[[cohort_id, user_id]])
      )
      cloud_logging_client.info(
          f'Removed access to cohort {cohort_id} for user {user_id}.'
      )


def _process_share_request(
    transaction: tr.Transaction,
    cohort_id: int,
    user_access: List[users_pb2.PathologyUserAccess],
) -> Tuple[Dict[str, int], List[str]]:
  """Processes a share request.

  1. Validates that users exist in database.
  2. Ensures cohort owner is not being modified in request.
  3. If user does not exist, creates a new user if email matches valid format.

  Args:
    transaction: Transaction to run Spanner read/writes on.
    cohort_id: Id of cohort to share.
    user_access: List of user permissions from share request.

  Returns:
    Tuple[Dict[user_email, user_id], List[invalid_emails]]

  Raises:
    RpcFailureError if cohort does not exist or owner is modified.
  """
  invalid_emails = []
  user_email_to_id = {}
  try:
    creator_id = spanner_util.get_cohort_owner(transaction, cohort_id)
  except exceptions.NotFound as exc:
    raise rpc_status.RpcFailureError(
        rpc_status.build_rpc_method_status_and_log(
            grpc.StatusCode.NOT_FOUND,
            f'Could not retrieve cohort {cohort_id} or it does not exist.',
            exp=exc,
        )
    )
  for permission in user_access:
    # Look up user in Spanner database.
    try:
      user_id = spanner_util.search_user_by_alias(
          permission.user_email, transaction
      )
      if (
          permission.access_role == users_pb2.PATHOLOGY_USER_ACCESS_ROLE_OWNER
          and user_id != creator_id
      ):
        raise rpc_status.RpcFailureError(
            rpc_status.build_rpc_method_status_and_log(
                grpc.StatusCode.INVALID_ARGUMENT,
                (
                    f'Could not process request to share cohort {cohort_id}.'
                    ' Owner role for cohorts cannot be modified.'
                ),
            )
        )
      # Add user id to map to store value.
      user_email_to_id[permission.user_email] = user_id
    except exceptions.NotFound:
      # Validate email string and create user.
      if _validate_email_format(permission.user_email):
        created_user_id = int(
            pathology_users_handler.insert_user_in_table(
                transaction,
                users_pb2.PathologyUser(
                    aliases=[
                        users_pb2.PathologyUserAlias(
                            alias=permission.user_email,
                            alias_type=users_pb2.PATHOLOGY_USER_ALIAS_TYPE_EMAIL,
                        )
                    ]
                ),
            ).user_id
        )
        user_email_to_id[permission.user_email] = created_user_id
      else:
        cloud_logging_client.warning(
            f'Skipping user with the email {permission.user_email} that '
            'does not match valid email format.'
        )
        invalid_emails.append(permission.user_email)

  return (user_email_to_id, invalid_emails)


def _process_user_access_changes(
    transaction: tr.Transaction,
    cohort: cohorts_pb2.PathologyCohort,
    cohort_id: int,
    user_access: List[users_pb2.PathologyUserAccess],
) -> Tuple[Dict[str, 'users_pb2.PathologyUserAccessRole'], List[str]]:
  """Processes changes to direct user access for cohorts.

  Args:
    transaction: Transaction to run spanner read/writes on.
    cohort: Cohort resource with existing permissions.
    cohort_id: Id of cohort to process.
    user_access: List of permissions to overwrite.

  Returns:
    Tuple[Dict[email, AccessRole],List[invalid_emails]]
  """
  user_map, invalid_emails = _process_share_request(
      transaction, cohort_id, user_access
  )
  existing_permissions = _convert_permissions_to_map(list(cohort.user_access))
  permissions_map = {}
  for user_permission in user_access:
    # Only valid emails are keys in user_map.
    if user_permission.user_email in user_map:
      user_id = user_map[user_permission.user_email]
      # Check if permission is already stored.
      if (
          user_permission.user_email in existing_permissions
          and existing_permissions[user_permission.user_email]
          == user_permission.access_role
      ):
        cloud_logging_client.info(
            f'User {user_permission.user_email} already has'
            f' {users_pb2.PathologyUserAccessRole.Name(user_permission.access_role)} for'
            f' cohort {cohort_id}.'
        )
        permissions_map[user_permission.user_email] = (
            user_permission.access_role
        )
      else:
        transaction.insert_or_update(
            ORCHESTRATOR_TABLE_NAMES[
                schema_resources.OrchestratorTables.COHORT_USER_ACCESS
            ],
            schema_resources.COHORT_USER_ACCESS_COLS,
            [[int(cohort_id), user_id, user_permission.access_role]],
        )
        cloud_logging_client.info(
            'Granted user'
            f' {user_permission.user_email} {users_pb2.PathologyUserAccessRole.Name(user_permission.access_role)} for'
            f' cohort {cohort_id}.'
        )
        permissions_map[user_permission.user_email] = (
            user_permission.access_role
        )

  _remove_deleted_permissions_from_table(
      transaction, cohort_id, permissions_map
  )
  return (permissions_map, invalid_emails)


def _suspend_cohort_in_table(
    transaction: tr.Transaction, cohort: cohorts_pb2.PathologyCohort
) -> None:
  """Suspends a cohort and marks it for deletion in Spanner.

  Args:
    transaction: Transaction to run spanner read/writes on.
    cohort: Updated cohort data.
  """
  cohort_id = pathology_resources_util.get_id_from_name(cohort.name)
  try:
    transaction.execute_update(
        f'UPDATE {_COHORTS_TABLE} SET '
        f'{schema_resources.COHORT_STAGE}=@cohort_stage, '
        f'{schema_resources.COHORT_UPDATE_TIME}=PENDING_COMMIT_TIMESTAMP(), '
        f'{schema_resources.EXPIRE_TIME}=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), '
        'INTERVAL @expire_time DAY) WHERE '
        f'{schema_resources.COHORT_ID}=@cohort_id;',
        params={
            'cohort_stage': cohort.cohort_metadata.cohort_stage,
            'expire_time': COHORT_EXPIRE_TIME_FLG.value,
            'cohort_id': cohort_id,
        },
        param_types={
            'cohort_stage': spanner.param_types.INT64,
            'expire_time': spanner.param_types.INT64,
            'cohort_id': spanner.param_types.INT64,
        },
    )
  except exceptions.FailedPrecondition as exc:
    raise rpc_status.RpcFailureError(
        rpc_status.build_rpc_method_status_and_log(
            grpc.StatusCode.FAILED_PRECONDITION,
            f'Failed to delete cohort {cohort_id}.'
            f'COHORT_EXPIRE_TIME {COHORT_EXPIRE_TIME_FLG.value} is invalid.',
            exp=exc,
        )
    ) from exc


def _share_cohort(
    transaction: tr.Transaction,
    cohort: cohorts_pb2.PathologyCohort,
    user_access: List[users_pb2.PathologyUserAccess],
    cohort_access: cohorts_pb2.PathologyCohortAccess,
) -> Tuple[cohorts_pb2.PathologyCohort, List[str]]:
  """Updates UserAccess table in spanner with permissions to add or remove.

  Looks up user id by email and logs an error if user does not exist.

  Args:
    transaction: Transaction to run spanner read/writes on.
    cohort: Cohort resource with existing permissions.
    user_access: List of permissions to overwrite.
    cohort_access: Cohort access constraint to overwrite.

  Returns:
   Tuple[PathologyCohort with updated permissions added, List[invalid_emails]]

  Raises:
    RpcFailureError if trying to modify owner.
  """
  cohort_id = pathology_resources_util.get_id_from_name(cohort.name)
  if cohort_access != cohorts_pb2.PATHOLOGY_COHORT_ACCESS_UNSPECIFIED:
    transaction.update(
        ORCHESTRATOR_TABLE_NAMES[schema_resources.OrchestratorTables.COHORTS],
        schema_resources.COHORTS_UPDATE_SHARING_COLS,
        [[cohort_id, cohort_access]],
    )
    cloud_logging_client.info(
        f'Updated CohortAccess for cohort {cohort_id} to'
        f' {cohorts_pb2.PathologyCohortAccess.Name(cohort_access)}.'
    )
    cohort.cohort_metadata.cohort_access = cohort_access

  if not user_access:
    return (cohort, [])
  permissions_map, invalid_emails = _process_user_access_changes(
      transaction, cohort, cohort_id, user_access
  )
  # Overwrite cohort resource with only added permissions.
  cohort.ClearField('user_access')
  cohort.user_access.extend(_convert_map_to_permissions(permissions_map))
  return (cohort, invalid_emails)


def _update_cohort_in_table(
    transaction, cohort: cohorts_pb2.PathologyCohort, update_slides: bool
) -> None:
  """Updates cohort row and slide mappings in Spanner.

  Args:
    transaction: Transaction to run spanner read/writes on.
    cohort: Updated cohort data.
    update_slides: True if slide mappings should be updated.

  Returns:
    None
  """
  cohort_id = pathology_resources_util.get_id_from_name(cohort.name)

  transaction.update(
      ORCHESTRATOR_TABLE_NAMES[schema_resources.OrchestratorTables.COHORTS],
      schema_resources.COHORTS_UPDATE_COLS,
      [[
          cohort_id,
          cohort.cohort_metadata.display_name,
          cohort.cohort_metadata.description,
          cohort.cohort_metadata.cohort_stage,
          cohort.cohort_metadata.is_deid,
          cohort.cohort_metadata.cohort_behavior_constraints,
          spanner.COMMIT_TIMESTAMP,
      ]],
  )

  if update_slides:
    _update_slide_mappings_in_table(transaction, cohort)


def _update_slide_mappings_in_table(
    transaction, cohort: cohorts_pb2.PathologyCohort
) -> None:
  """Updates the slides contained by the cohort in Spanner.

  Args:
    transaction: Transaction to run spanner read/writes on.
    cohort: Cohort containing updated slides.

  Returns:
    None
  """
  cohort_id = pathology_resources_util.get_id_from_name(cohort.name)
  if cohort.slides:
    dicom_store_slides = _get_slides_in_dicom_store(cohort)
    cohort.ClearField('slides')
    cohort.slides.extend(dicom_store_slides)
    for slide in cohort.slides:
      try:
        # Try adding existing slide to cohort.
        slide.scan_unique_id = str(
            _add_slide_to_cohort(transaction, slide, cohort_id)
        )
      except exceptions.NotFound:
        # Slide does not exist. Create slide and add to cohort.
        slide.scan_unique_id = str(
            pathology_slides_handler.insert_slide_in_table(transaction, slide)
        )
        _add_slide_to_cohort(transaction, slide, cohort_id)
      slide.name = pathology_resources_util.convert_scan_uid_to_name(
          slide.scan_unique_id
      )
  # Check for any removed slides.
  cohort_slides_tbl = ORCHESTRATOR_TABLE_NAMES[
      schema_resources.OrchestratorTables.COHORT_SLIDES
  ]
  cohort_slides_rows = transaction.execute_sql(
      f'SELECT * FROM {cohort_slides_tbl} WHERE '
      f'{schema_resources.COHORT_ID}=@cohort_id;',
      params={'cohort_id': cohort_id},
      param_types={'cohort_id': spanner.param_types.INT64},
  )
  scan_ids = [int(slide.scan_unique_id) for slide in cohort.slides]
  for row in cohort_slides_rows:
    scan_uid = row[
        schema_resources.COHORT_SLIDES_COLS.index(
            schema_resources.SCAN_UNIQUE_ID
        )
    ]
    if scan_uid not in scan_ids:
      transaction.delete(
          cohort_slides_tbl, spanner.KeySet(keys=[[cohort_id, scan_uid]])
      )


def _undelete_cohort_in_table(
    transaction: tr.Transaction, cohort: cohorts_pb2.PathologyCohort
) -> None:
  """Undeletes a cohort and returns it to an active state.

  Args:
    transaction: Transaction to run spanner read/writes on.
    cohort: Updated cohort data to apply in Spanner.
  """
  cohort_id = pathology_resources_util.get_id_from_name(cohort.name)

  transaction.update(
      ORCHESTRATOR_TABLE_NAMES[schema_resources.OrchestratorTables.COHORTS],
      schema_resources.COHORTS_SUSPEND_COLS,
      [[
          cohort_id,
          cohort.cohort_metadata.cohort_stage,
          spanner.COMMIT_TIMESTAMP,
          None,
      ]],
  )


def _list_cohorts_for_user(
    snapshot: s.Snapshot,
    user_id: int,
    include_slides: bool,
    access_role: Optional['users_pb2.PathologyUserAccessRole'] = None,
) -> cohorts_pb2.ListPathologyCohortsResponse:
  """Performs a Spanner look up of a cohort by id.

  Returns all roles if no filter is specified.

  Args:
    snapshot: Read-only snapshot of database.
    user_id: User Id of user to retrieve cohorts for.
    include_slides: True if slides should be included in the response.
    access_role: Access role to filter results on.

  Returns:
    ListPathologyCohortsResponse.

  Raises:
    RpcFailureError on failure.
  """
  response = cohorts_pb2.ListPathologyCohortsResponse()

  if access_role is None:
    filtered_cohorts = spanner_util.get_all_cohorts_for_user(snapshot, user_id)
  else:
    filtered_cohorts = spanner_util.get_cohorts_for_user(
        snapshot, user_id, access_role
    )

  response.pathology_cohorts.extend(
      _get_cohorts_by_id(
          snapshot, user_id, list(filtered_cohorts), include_slides
      )
  )

  return response


def _verify_dicom_store_full_format(dicom_store_url: str) -> bool:
  """Verifies that a dicom store url matches the expected full path format.

  Ex. projects/.../locations/.../datasets/.../dicomStores/...

  Args:
    dicom_store_url: DICOM store url to verify.

  Returns:
    True if url matches format.
  """
  try:
    dicom_store_url_parts = dicom_store_url.split('/')
    assert len(dicom_store_url_parts) == 8
    assert dicom_store_url_parts[0] == 'projects'
    assert dicom_store_url_parts[2] == 'locations'
    assert dicom_store_url_parts[4] == 'datasets'
    assert dicom_store_url_parts[6] == 'dicomStores'
    return True
  except AssertionError:
    return False


def _verify_gcs_bucket_path(gcs_bucket_path: str) -> bool:
  """Verifies that the gcs bucket path matches the expected format.

  Ex. gs://export/output

  Args:
    gcs_bucket_path: GCS bucket path to verify.

  Returns:
    True if path matches format.
  """
  try:
    assert gcs_bucket_path.startswith('gs://')
    return True
  except AssertionError:
    return False


class PathologyCohortsHandler:
  """Handler for PathologyCohorts rpc methods of DigitalPathology service."""

  def __init__(self):
    super().__init__()
    self._gcs_client = gcs_util.GcsUtil()
    self._healthcare_api_client = discovery.build(
        healthcare_api_const.HEALTHCARE_SERVICE_NAME,
        healthcare_api_const.HEALTHCARE_API_VERSION,
        discoveryServiceUrl=healthcare_api_const.get_healthcare_api_discovery_url(
            healthcare_api_const.HEALTHCARE_API_BASE_URL_FLG.value
        ),
        cache_discovery=False,
    )
    self._spanner_client = cloud_spanner_client.CloudSpannerClient()
    self._users_handler = pathology_users_handler.PathologyUsersHandler()

  def _execute_export_request(
      self,
      cohort_to_export: cohorts_pb2.PathologyCohort,
      source_dicom_store: str,
      gcs_dest_path: str,
  ) -> Dict[str, Any]:
    """Builds and executes a dicomStores.export request.

    Args:
      cohort_to_export: Cohort to perform export on.
      source_dicom_store: Source DicomWebUrl for slides.
      gcs_dest_path: Destination GCS bucket for output from operation.

    Returns:
      Json Dict of Operation
    """
    cohort_id = pathology_resources_util.get_id_from_name(cohort_to_export.name)
    # Generate filter file with series identifier.
    slide_paths = filter_file_generator.get_slides_for_deid_filter_list(
        cohort_to_export
    )

    # Write filter file to GCS.
    filter_file_path = self._gcs_client.write_filter_file(
        slide_paths, f'{gcs_util.EXPORT_FILES_PREFIX}{cohort_id}'
    )
    request_body = {
        'gcsDestination': {'uriPrefix': f'{gcs_dest_path}'},
        'filterConfig': {'resourcePathsGcsUri': f'{filter_file_path}'},
    }

    export_request = (
        self._healthcare_api_client.projects()
        .locations()
        .datasets()
        .dicomStores()
        .export(name=source_dicom_store, body=request_body)
    )

    return export_request.execute()

  def _execute_transfer_deid_request(
      self,
      cohort_to_transfer: cohorts_pb2.PathologyCohort,
      source_dicom_store: str,
      dest_dicom_store: str,
      filtered_paths: List[str],
  ) -> Dict[str, Any]:
    """Builds and executes a dicomStores.Deidentify request.

    Args:
      cohort_to_transfer: Cohort to perform transfer deid on.
      source_dicom_store: Source DicomWebUrl for slides.
      dest_dicom_store: Destination DicomWebUrl for slides.
      filtered_paths: DICOM instances to deid.

    Returns:
      Json Dict of Operation
    """
    dicom_tags_to_keep_for_deid = _read_deid_tag_keep_list()
    cohort_id = pathology_resources_util.get_id_from_name(
        cohort_to_transfer.name
    )

    # Write filter file to GCS.
    filter_file_path = self._gcs_client.write_filter_file(
        filtered_paths, f'{gcs_util.DEID_FILES_PREFIX}{cohort_id}'
    )

    # Deidentify metadata only with default info types.
    request_body = {
        'destinationStore': dest_dicom_store,
        'config': {
            'dicom': {
                'skipIdRedaction': 'True',
                'keepList': {'tags': dicom_tags_to_keep_for_deid},
            },
            'image': {'textRedactionMode': 'REDACT_NO_TEXT'},
        },
        'filterConfig': {'resourcePathsGcsUri': f'{filter_file_path}'},
    }

    deid_request = (
        self._healthcare_api_client.projects()
        .locations()
        .datasets()
        .dicomStores()
        .deidentify(sourceStore=source_dicom_store, body=request_body)
    )

    return deid_request.execute()

  def _read_cohort_row(
      self, cohort_id: int, columns: Optional[List[str]] = None
  ) -> StreamedResultSet:
    """Returns the cohort row given the id.

    Args:
      cohort_id: Id of cohort row to return.
      columns: Optional list of columns to read.

    Returns:
      StreamedResultSet
    """
    if columns is None:
      columns = schema_resources.COHORTS_COLS
    return self._spanner_client.read_data(
        ORCHESTRATOR_TABLE_NAMES[schema_resources.OrchestratorTables.COHORTS],
        columns,
        spanner.KeySet(keys=[[cohort_id]]),
    )

  def create_pathology_cohort(
      self, request: cohorts_pb2.CreatePathologyCohortRequest, alias: str
  ) -> cohorts_pb2.PathologyCohort:
    """Creates a PathologyCohort.

    Args:
      request: A CreatePathologyCohortRequest with cohort to create.
      alias: Alias of rpc caller.

    Returns:
      PathologyCohort.

    Raises:
      RpcFailureError on failure.
    """
    current_user = self._users_handler.identify_current_user(alias)
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.CREATE_PATHOLOGY_COHORT
        )
    )
    cohort_metadata = cohorts_pb2.PathologyCohortMetadata(
        cohort_stage=cohorts_pb2.PATHOLOGY_COHORT_LIFECYCLE_STAGE_ACTIVE,
        is_deid=False,
        cohort_behavior_constraints=cohorts_pb2.PATHOLOGY_COHORT_BEHAVIOR_CONSTRAINTS_FULLY_FUNCTIONAL,
    )
    cohort = cohorts_pb2.PathologyCohort(cohort_metadata=cohort_metadata)
    cohort.MergeFrom(request.pathology_cohort)
    _remove_duplicate_slides(cohort)

    # Insert cohort and map slides in spanner tables.
    try:
      cohort_id = self._spanner_client.run_in_transaction(
          _insert_cohort_in_table, int(current_user.user_id), cohort
      )
      resource_name = pathology_resources_util.convert_cohort_id_to_name(
          current_user.user_id, cohort_id
      )
      log_struct = {'resource_name': resource_name}
      cloud_logging_client.info(
          f'Created PathologyCohort {resource_name} and inserted into table.',
          log_struct,
      )
    except Exception as exc:
      if exc.__class__ is rpc_status.RpcFailureError:
        raise exc
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INTERNAL, 'Insert for cohort failed.'
          )
      ) from exc

    cohort.name = resource_name

    # Read UpdateTime value only.
    try:
      result = self._read_cohort_row(
          cohort_id, [schema_resources.COHORT_UPDATE_TIME]
      ).one()
      cohort.cohort_metadata.update_time.CopyFrom(result[0].timestamp_pb())
    except exceptions.NotFound as exc:
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.NOT_FOUND,
              f'Could not find cohort {resource_name} in database.',
              exp=exc,
          )
      ) from exc
    return cohort

  def get_pathology_cohort(
      self, request: cohorts_pb2.GetPathologyCohortRequest, alias: str
  ) -> cohorts_pb2.PathologyCohort:
    """Gets a PathologyCohort.

    Args:
      request: A GetPathologyCohortRequest with cohort to retrieve.
      alias: Alias of rpc caller.

    Returns:
      PathologyCohort.

    Raises:
      RpcFailureError on failure.
    """
    resource_name = request.name
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.GET_PATHOLOGY_COHORT, resource_name
        )
    )
    cohort_id = pathology_resources_util.get_id_from_name(resource_name)
    with self._spanner_client.get_database_snapshot() as snapshot:
      if not spanner_util.check_caller_access(
          snapshot, alias, cohort_id, spanner_util.ResourceAccessPermissions.GET
      ):
        raise rpc_status.RpcFailureError(
            rpc_status.build_rpc_method_status_and_log(
                grpc.StatusCode.PERMISSION_DENIED,
                f'User does not have permission to access cohort {cohort_id}'
                ' or it does not exist.',
            )
        )
      cloud_logging_client.info(f'Retrieving PathologyCohort {resource_name}.')
      result = _get_cohort_by_id(
          snapshot,
          alias,
          cohort_id,
          request.view != cohorts_pb2.PATHOLOGY_COHORT_VIEW_METADATA_ONLY,
      )
      return result

  def delete_pathology_cohort(
      self, request: cohorts_pb2.DeletePathologyCohortRequest, alias: str
  ) -> cohorts_pb2.PathologyCohort:
    """Soft deletes a PathologyCohort and marks it for deletion.

    Cohorts are soft deleted (state modified to suspended) and marked for
    automatic deletion at the expire time.

    Args:
      request: A DeletePathologyCohortRequest with cohort to delete.
      alias: Alias of the rpc caller.

    Returns:
      PathologyCohort.

    Raises:
      RpcFailureError on failure.
    """
    resource_name = request.name
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.DELETE_PATHOLOGY_COHORT, resource_name
        )
    )
    cohort_id = pathology_resources_util.get_id_from_name(resource_name)
    with self._spanner_client.get_database_snapshot() as snapshot:
      if not spanner_util.check_caller_access(
          snapshot,
          alias,
          cohort_id,
          spanner_util.ResourceAccessPermissions.DELETE,
      ):
        raise rpc_status.RpcFailureError(
            rpc_status.build_rpc_method_status_and_log(
                grpc.StatusCode.PERMISSION_DENIED,
                f'User does not have permission to delete cohort {cohort_id}.',
            )
        )
    cohort_to_delete = self.get_pathology_cohort(
        cohorts_pb2.GetPathologyCohortRequest(name=resource_name), alias
    )

    cloud_logging_client.info(f'Deleting PathologyCohort {cohort_id}.')

    # Ensure cohort is active, mark cohort as suspended.
    if _get_is_active(cohort_to_delete):
      cohort_to_delete.cohort_metadata.cohort_stage = (
          cohorts_pb2.PATHOLOGY_COHORT_LIFECYCLE_STAGE_SUSPENDED
      )
    else:
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.NOT_FOUND,
              f'Cohort {cohort_id} is not in an active state.',
              {'cohort_state': cohort_to_delete.cohort_metadata.cohort_stage},
          )
      )

    try:
      self._spanner_client.run_in_transaction(
          _suspend_cohort_in_table, cohort_to_delete
      )
      cloud_logging_client.info(
          f'Cohort {cohort_id} suspended and '
          'marked for deletion in '
          f'{COHORT_EXPIRE_TIME_FLG.value} '
          'days.'
      )
    except Exception as exc:
      if exc.__class__ is rpc_status.RpcFailureError:
        raise exc
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INTERNAL,
              f'Failed to delete cohort {resource_name}.',
          )
      ) from exc

    # Read UpdateTime and ExpireTime values only.
    try:
      result = self._read_cohort_row(
          cohort_id, schema_resources.COHORTS_SUSPEND_COLS
      )
      row_dict = dict(zip(schema_resources.COHORTS_SUSPEND_COLS, result.one()))
      cohort_to_delete.cohort_metadata.update_time.CopyFrom(
          row_dict[schema_resources.COHORT_UPDATE_TIME].timestamp_pb()
      )
      cohort_to_delete.cohort_metadata.expire_time.CopyFrom(
          row_dict[schema_resources.EXPIRE_TIME].timestamp_pb()
      )
    except exceptions.NotFound as exc:
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.NOT_FOUND,
              f'Could not find cohort {resource_name} in database.',
              exp=exc,
          )
      ) from exc

    return cohort_to_delete

  def list_pathology_cohorts(
      self, request: cohorts_pb2.ListPathologyCohortsRequest, alias: str
  ) -> cohorts_pb2.ListPathologyCohortsResponse:
    """Lists PathologyCohorts owned and shared.

    Args:
      request: A ListPathologyCohortRequest.
      alias: Alias of rpc caller.

    Returns:
      ListPathologyCohortsResponse.

    Raises:
      RpcFailureError on failure.
    """
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.LIST_PATHOLOGY_COHORTS
        )
    )
    with self._spanner_client.get_database_snapshot() as snapshot:
      try:
        caller_id = spanner_util.search_user_by_alias(alias, snapshot)
      except exceptions.NotFound as exc:
        raise rpc_status.RpcFailureError(
            rpc_status.build_rpc_method_status_and_log(
                grpc.StatusCode.NOT_FOUND,
                f'Could not find user associated with alias {alias}.',
                exp=exc,
            )
        )
      if caller_id != pathology_resources_util.get_id_from_name(request.parent):
        raise rpc_status.RpcFailureError(
            rpc_status.build_rpc_method_status_and_log(
                grpc.StatusCode.PERMISSION_DENIED,
                'Request must match the user id of the caller.',
                {'user_id': caller_id},
            )
        )
      try:
        access_role_filter = _get_access_role_value(request.filter)
      except ValueError as exc:
        raise rpc_status.RpcFailureError(
            rpc_status.build_rpc_method_status_and_log(
                grpc.StatusCode.INVALID_ARGUMENT,
                f'Invalid access role filter {request.filter} specified.',
                exp=exc,
            )
        )
      if access_role_filter is None:
        cloud_logging_client.info(
            f'Retrieving all PathologyCohorts for user {caller_id}.',
            {'user_id': caller_id},
        )
      else:
        cloud_logging_client.info(
            f'Retrieving PathologyCohorts for user {caller_id} with access role'
            f' {users_pb2.PathologyUserAccessRole.Name(access_role_filter)}.',
            {'user_id': caller_id},
        )
      return _list_cohorts_for_user(
          snapshot,
          caller_id,
          request.view != cohorts_pb2.PATHOLOGY_COHORT_VIEW_METADATA_ONLY,
          access_role_filter,
      )

  def update_pathology_cohort(
      self,
      request: cohorts_pb2.UpdatePathologyCohortRequest,
      alias: str,
  ) -> cohorts_pb2.PathologyCohort:
    """Updates a PathologyCohort.

    Args:
      request: An UpdatePathologyCohortRequest with cohort to update and fields
        to update contained in a field mask.
      alias: Alias of caller to authenticate.

    Returns:
      PathologyCohort.

    Raises:
      RpcFailureError on failure.
    """
    resource_name = request.pathology_cohort.name
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.UPDATE_PATHOLOGY_COHORT, resource_name
        )
    )
    cohort_id = pathology_resources_util.get_id_from_name(resource_name)
    with self._spanner_client.get_database_snapshot() as snapshot:
      try:
        caller_id = spanner_util.search_user_by_alias(alias, snapshot)
      except exceptions.NotFound as exp:
        cloud_logging_client.error(
            f'Could not find user associated with alias {alias}.', exp
        )
        raise rpc_status.RpcFailureError(
            rpc_status.build_rpc_method_status_and_log(
                grpc.StatusCode.PERMISSION_DENIED,
                f'User does not have permission to update cohort {cohort_id}.',
                exp=exp,
            )
        ) from exp
      if not spanner_util.check_caller_access(
          snapshot,
          alias,
          cohort_id,
          spanner_util.ResourceAccessPermissions.UPDATE,
          caller_id,
      ):
        raise rpc_status.RpcFailureError(
            rpc_status.build_rpc_method_status_and_log(
                grpc.StatusCode.PERMISSION_DENIED,
                f'User does not have permission to update cohort {cohort_id}.',
            )
        )
      has_owner_privilege = spanner_util.check_if_cohort_owner_or_admin(
          snapshot, caller_id, cohort_id
      )

    cohort_to_update = self.get_pathology_cohort(
        cohorts_pb2.GetPathologyCohortRequest(name=resource_name), alias
    )
    # Only active cohorts can be updated.
    if not _get_is_active(cohort_to_update):
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.NOT_FOUND,
              f'Cohort {cohort_id} is not in an active state and cannot be'
              ' updated.',
              {'cohort_state': cohort_to_update.cohort_metadata.cohort_stage},
          )
      )

    # Verify cohort data is fresh.
    if (
        cohort_to_update.cohort_metadata.update_time.seconds
        != request.pathology_cohort.cohort_metadata.update_time.seconds
    ):
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.FAILED_PRECONDITION,
              f'Request contains stale data for cohort {cohort_id}.'
              ' Refresh the data and try again.',
              {
                  'cohort_name': cohort_to_update.name,
                  'last_updated': cohort_to_update.cohort_metadata.update_time,
              },
          )
      )

    cloud_logging_client.info(
        f'Update requested on PathologyCohort {cohort_id}.'
    )
    # Check for invalid arguments.
    if request.update_mask:
      for field in request.update_mask.paths:
        if field in _IMMUTABLE_STATE_FIELDS_FOR_UPDATE:
          raise rpc_status.RpcFailureError(
              rpc_status.build_rpc_method_status_and_log(
                  grpc.StatusCode.INVALID_ARGUMENT,
                  f'Field {field} is an immutable state field and cannot be'
                  ' manually updated.',
              )
          )

        if field in _METADATA_FIELDS:
          if has_owner_privilege:
            try:
              setattr(
                  cohort_to_update.cohort_metadata,
                  field,
                  getattr(request.pathology_cohort.cohort_metadata, field),
              )
            except AttributeError as exc:
              raise rpc_status.RpcFailureError(
                  rpc_status.build_rpc_method_status_and_log(
                      grpc.StatusCode.INTERNAL,
                      f'Failed to update cohort {resource_name}.',
                      exp=exc,
                  )
              ) from exc
          else:
            raise rpc_status.RpcFailureError(
                rpc_status.build_rpc_method_status_and_log(
                    grpc.StatusCode.PERMISSION_DENIED,
                    'User does not have permission to update the metadata'
                    f' for cohort {cohort_id}.',
                )
            )
        elif field == 'slides':
          _remove_duplicate_slides(request.pathology_cohort)
          cohort_to_update.ClearField('slides')
          cohort_to_update.slides.extend(request.pathology_cohort.slides)
        else:
          raise rpc_status.RpcFailureError(
              rpc_status.build_rpc_method_status_and_log(
                  grpc.StatusCode.INVALID_ARGUMENT,
                  f'Invalid mask provided, field in {request.update_mask.paths}'
                  ' does not exist for cohorts.',
              )
          )

    # Only update slide mappings if the field is present in the update mask.
    update_slides = True if 'slides' in request.update_mask.paths else False
    try:
      self._spanner_client.run_in_transaction(
          _update_cohort_in_table, cohort_to_update, update_slides
      )
      cloud_logging_client.info(f'Updated cohort {resource_name}.')
    except Exception as exc:
      if exc.__class__ is rpc_status.RpcFailureError:
        raise exc
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INTERNAL,
              f'Failed to update cohort {resource_name}.',
              exp=exc,
          )
      ) from exc

    # Read UpdateTime only.
    cohort_id = pathology_resources_util.get_id_from_name(cohort_to_update.name)
    try:
      updated_row = self._read_cohort_row(
          cohort_id, [schema_resources.COHORT_UPDATE_TIME]
      ).one()
      cohort_to_update.cohort_metadata.update_time.CopyFrom(
          updated_row[0].timestamp_pb()
      )
    except exceptions.NotFound as exc:
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.NOT_FOUND,
              f'Could not find cohort {resource_name} in database.',
              exp=exc,
          )
      ) from exc
    if request.view == cohorts_pb2.PATHOLOGY_COHORT_VIEW_METADATA_ONLY:
      cohort_to_update.ClearField('slides')
    return cohort_to_update

  def undelete_pathology_cohort(
      self, request: cohorts_pb2.UndeletePathologyCohortRequest, alias: str
  ) -> cohorts_pb2.PathologyCohort:
    """Undelete (unhide) a PathologyCohort.

    Cohorts are soft deleted and marked for permananent deletion at the
    expiration date. They can be undeleted and returned to an active state
    before the expiration date.

    Args:
      request: An UndeletePathologyCohortRequest with cohort to undelete.
      alias: Alias of rpc caller.

    Returns:
      PathologyCohort.

    Raises:
      RpcFailureError on failure.
    """
    resource_name = request.name
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.UNDELETE_PATHOLOGY_COHORT, resource_name
        )
    )
    cohort_id = pathology_resources_util.get_id_from_name(resource_name)
    with self._spanner_client.get_database_snapshot() as snapshot:
      if not spanner_util.check_caller_access(
          snapshot,
          alias,
          cohort_id,
          spanner_util.ResourceAccessPermissions.UNDELETE,
      ):
        raise rpc_status.RpcFailureError(
            rpc_status.build_rpc_method_status_and_log(
                grpc.StatusCode.PERMISSION_DENIED,
                'User does not have permission to undelete cohort'
                f' {cohort_id}.',
            )
        )
    cohort_to_undelete = self.get_pathology_cohort(
        cohorts_pb2.GetPathologyCohortRequest(name=resource_name), alias
    )

    # If cohort is not suspended, return failure.
    if not _get_is_suspended(cohort_to_undelete):
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.ALREADY_EXISTS,
              f'Cohort {cohort_id} is not in a suspended state.'
              f' Failed to undelete cohort {cohort_id}.',
              {'cohort_state': cohort_to_undelete.cohort_metadata.cohort_stage},
          )
      )

    # Mark cohort as active, reset expire time.
    cohort_to_undelete.cohort_metadata.cohort_stage = (
        cohorts_pb2.PATHOLOGY_COHORT_LIFECYCLE_STAGE_ACTIVE
    )
    cohort_to_undelete.cohort_metadata.expire_time.Clear()

    try:
      self._spanner_client.run_in_transaction(
          _undelete_cohort_in_table, cohort_to_undelete
      )
      cloud_logging_client.info(f'Undeleted PathologyCohort {resource_name}.')
    except Exception as exc:
      if exc.__class__ is rpc_status.RpcFailureError:
        raise exc
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INTERNAL,
              f'Failed to undelete cohort {resource_name}.',
          )
      ) from exc

    # Read UpdateTime only.
    try:
      result = self._read_cohort_row(
          cohort_id, [schema_resources.COHORT_UPDATE_TIME]
      ).one()
      cohort_to_undelete.cohort_metadata.update_time.CopyFrom(
          result[0].timestamp_pb()
      )
    except exceptions.NotFound as exc:
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.NOT_FOUND,
              f'Could not find cohort {resource_name} in database.',
              exp=exc,
          )
      ) from exc
    return cohort_to_undelete

  def export_pathology_cohort(
      self, request: cohorts_pb2.ExportPathologyCohortRequest, alias: str
  ) -> Optional[operations_pb2.Operation]:
    """Exports a PathologyCohort to a Cloud Storage bucket.

    Args:
      request: An ExportPathologyCohortRequest with cohort to export.
      alias: Alias of the rpc caller.

    Returns:
      ExportPathologyCohortResponse
    """
    resource_name = request.name
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.EXPORT_PATHOLOGY_COHORT, resource_name
        )
    )
    cohort_id = pathology_resources_util.get_id_from_name(resource_name)
    with self._spanner_client.get_database_snapshot() as snapshot:
      if not spanner_util.check_caller_access(
          snapshot,
          alias,
          cohort_id,
          spanner_util.ResourceAccessPermissions.EXPORT,
      ):
        raise rpc_status.RpcFailureError(
            rpc_status.build_rpc_method_status_and_log(
                grpc.StatusCode.PERMISSION_DENIED,
                f'User does not have permission to export cohort {cohort_id}.',
            )
        )

    # Validate gcs bucket path.
    gcs_dest_path = request.gcs_dest_path
    if not _verify_gcs_bucket_path(gcs_dest_path):
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INVALID_ARGUMENT,
              f'The GCS bucket path provided is invalid: {gcs_dest_path}.',
          )
      )
    cohort_to_export = self.get_pathology_cohort(
        cohorts_pb2.GetPathologyCohortRequest(name=resource_name), alias
    )

    if _get_is_suspended(cohort_to_export):
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INVALID_ARGUMENT,
              f'The cohort {cohort_id} is in a suspended state and cannot be'
              ' exported.',
          )
      )

    source_dicom_store = _get_dicom_store_url(cohort_to_export)

    cloud_logging_client.info(
        f'User {alias} initiating export request on cohort {cohort_id}.',
        {'user_alias': alias, 'resource_id': cohort_id},
    )
    json_response = self._execute_export_request(
        cohort_to_export, source_dicom_store, gcs_dest_path
    )

    # Build PathologyOperationMetadata.
    op_metadata = digital_pathology_pb2.PathologyOperationMetadata(
        export_metadata=digital_pathology_pb2.ExportOperationMetadata(
            gcs_dest_path=request.gcs_dest_path
        ),
        filter_file_path=f'{gcs_util.EXPORT_FILES_PREFIX}{cohort_id}',
    )
    # Generate DpasOperationId and store in Spanner.
    try:
      dpas_op_id = self._spanner_client.run_in_transaction(
          _record_operation_in_table,
          json_response['name'],
          op_metadata,
          request.notification_emails,
      )
    except Exception as exc:
      if exc.__class__ is rpc_status.RpcFailureError:
        raise exc
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INTERNAL,
              'Failed to record operation in table.',
              exp=exc,
          )
      ) from exc

    cloud_logging_client.info(
        f'Initiated export. Writing dicom files to gcs path: {gcs_dest_path}.'
    )

    # Create operation and return.
    any_metadata = any_pb2.Any()
    any_metadata.Pack(op_metadata)

    return operations_pb2.Operation(
        name=pathology_resources_util.convert_operation_id_to_name(dpas_op_id),
        metadata=any_metadata,
    )

  def transfer_deid_pathology_cohort(
      self, request: cohorts_pb2.TransferDeIdPathologyCohortRequest, alias: str
  ) -> Optional[operations_pb2.Operation]:
    """De-Identifies and transfers a cohort to a De-Id DICOM store.

    Args:
      request: A TransferDeidPathologyCohortRequest with cohort to transfer.
      alias: Alias of rpc caller.

    Returns:
      Operation instance.

    Raises:
      RpcFailureError on failure.
    """
    resource_name = request.name
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.TRANSFER_DEID_PATHOLOGY_COHORT,
            resource_name,
        )
    )
    cohort_id = pathology_resources_util.get_id_from_name(resource_name)
    current_user = self._users_handler.identify_current_user(alias)
    with self._spanner_client.get_database_snapshot() as snapshot:
      if not spanner_util.check_caller_access(
          snapshot,
          alias,
          cohort_id,
          spanner_util.ResourceAccessPermissions.TRANSFER_DEID,
      ):
        raise rpc_status.RpcFailureError(
            rpc_status.build_rpc_method_status_and_log(
                grpc.StatusCode.PERMISSION_DENIED,
                'User does not have permission to transfer cohort'
                f' {cohort_id}.',
            )
        )

    # Validate destination dicom store url.
    dest_dicom_store = request.dest_dicom_images
    if not _verify_dicom_store_full_format(dest_dicom_store):
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INVALID_ARGUMENT,
              (
                  'Destination DICOM Store is not in full format'
                  f' {dest_dicom_store}.'
              ),
          )
      )
    cohort_to_transfer = self.get_pathology_cohort(
        cohorts_pb2.GetPathologyCohortRequest(name=resource_name), alias
    )

    if cohort_to_transfer.cohort_metadata.is_deid:
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.ALREADY_EXISTS,
              f'The cohort {cohort_id} already contains de-identified data.',
          )
      )
    if _get_is_suspended(cohort_to_transfer):
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INVALID_ARGUMENT,
              (
                  f'The cohort {cohort_id} is in a suspended state and cannot'
                  ' be transferred.'
              ),
          )
      )

    source_dicom_store = _get_dicom_store_url(cohort_to_transfer)

    # Generate filter file and skip instances with PHI.
    # Format: studies/.../series/.../instances/...
    filtered_instances = filter_file_generator.get_instances_for_deid(
        cohort_to_transfer
    )
    # Convert instances to series.
    # Format: studies/.../series/...
    filtered_series = _get_series_paths_from_instances(filtered_instances)

    if not filtered_series:
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INVALID_ARGUMENT,
              (
                  'All images in the cohort are not supported for '
                  'de-identification. An admin can add additional IOD formats '
                  'to the list of supported formats by adding them to the '
                  'DEID_IOD_LIST configuration setting on the server.'
              ),
              {
                  'resource_name': request.name,
                  'deid_iod_list': (
                      filter_file_generator.DEID_IOD_LIST_FLG.value
                  ),
              },
          )
      )
    cloud_logging_client.info(
        f'User {alias} initiating DeId request on cohort {cohort_id}.',
        {'user_alias': alias, 'resource_id': cohort_id},
    )
    json_response = self._execute_transfer_deid_request(
        cohort_to_transfer,
        source_dicom_store,
        dest_dicom_store,
        filtered_instances,
    )
    if json_response is not None:
      cloud_logging_client.info(
          'Sent dicomStores.Deidentify Request to Healthcare API.',
          {'cohort_resource_name': request.name},
      )

    # Build an unavailable cohort.
    cohort_to_transfer.cohort_metadata.display_name = (
        request.display_name_transferred
    )
    cohort_to_transfer.cohort_metadata.description = (
        request.description_transferred
    )
    try:
      transferred_cohort_id = self._spanner_client.run_in_transaction(
          _copy_cohort,
          cohort_to_transfer,
          dest_dicom_store,
          current_user.user_id,
          True,
          cohorts_pb2.PATHOLOGY_COHORT_LIFECYCLE_STAGE_UNAVAILABLE,
          filtered_series,
      )
    except Exception as exc:
      if exc.__class__ is rpc_status.RpcFailureError:
        raise exc
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INTERNAL,
              'Failed to create DeId Pathology Cohort.',
              exp=exc,
          )
      ) from exc

    cohort_resource_name = pathology_resources_util.convert_cohort_id_to_name(
        current_user.user_id, transferred_cohort_id
    )

    # Build PathologyOperationMetadata.
    op_metadata = digital_pathology_pb2.PathologyOperationMetadata(
        transfer_deid_metadata=digital_pathology_pb2.TransferDeidOperationMetadata(
            resource_name_transferred=cohort_resource_name
        ),
        filter_file_path=f'{gcs_util.DEID_FILES_PREFIX}{cohort_id}',
    )
    # Generate DpasOperationId and store in Spanner.
    try:
      dpas_op_id = self._spanner_client.run_in_transaction(
          _record_operation_in_table,
          json_response['name'],
          op_metadata,
          request.notification_emails,
      )
    except Exception as exc:
      if exc.__class__ is rpc_status.RpcFailureError:
        raise exc
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INTERNAL,
              'Failed to record operation in table.',
              exp=exc,
          )
      ) from exc

    cloud_logging_client.info(
        f'Created DeID PathologyCohort {cohort_resource_name}.',
        {'transferred_cohort_resource_name': cohort_resource_name},
    )

    # Create operation and return.
    any_metadata = any_pb2.Any()
    any_metadata.Pack(op_metadata)

    return operations_pb2.Operation(
        name=pathology_resources_util.convert_operation_id_to_name(dpas_op_id),
        metadata=any_metadata,
    )

  def copy_pathology_cohort(
      self, request: cohorts_pb2.CopyPathologyCohortRequest, alias: str
  ) -> cohorts_pb2.PathologyCohort:
    """Creates a duplicate of an existing pathology cohort.

    Args:
      request: A CopyPathologyCohortRequest with cohort to copy.
      alias: Alias of rpc caller.

    Returns:
      PathologyCohort.

    Raises:
      RpcFailureError on failure.
    """
    resource_name = request.name
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.COPY_PATHOLOGY_COHORT, resource_name
        )
    )
    cohort_id = pathology_resources_util.get_id_from_name(resource_name)
    current_user = self._users_handler.identify_current_user(alias)
    with self._spanner_client.get_database_snapshot() as snapshot:
      if not spanner_util.check_caller_access(
          snapshot,
          alias,
          cohort_id,
          spanner_util.ResourceAccessPermissions.COPY,
      ):
        raise rpc_status.RpcFailureError(
            rpc_status.build_rpc_method_status_and_log(
                grpc.StatusCode.PERMISSION_DENIED,
                f'User does not have permission to copy cohort {cohort_id}.',
            )
        )
    cohort_to_copy = self.get_pathology_cohort(
        cohorts_pb2.GetPathologyCohortRequest(name=resource_name), alias
    )
    if not _get_is_active(cohort_to_copy):
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INVALID_ARGUMENT,
              f'The cohort {cohort_id} is not in an active state and cannot be'
              ' copied.',
          )
      )

    cloud_logging_client.info(
        f'Copying PathologyCohort with resource name: {resource_name}.'
    )

    # Set display_name and description from request.
    cohort_to_copy.cohort_metadata.display_name = request.display_name_copied
    cohort_to_copy.cohort_metadata.description = request.description_copied

    if cohort_to_copy.slides:
      dest_dicom_store = _get_dicom_store_url(cohort_to_copy)
    else:
      dest_dicom_store = None

    copied_cohort_id = self._spanner_client.run_in_transaction(
        _copy_cohort,
        cohort_to_copy,
        dest_dicom_store,
        current_user.user_id,
        cohort_to_copy.cohort_metadata.is_deid,
        cohorts_pb2.PATHOLOGY_COHORT_LIFECYCLE_STAGE_ACTIVE,
    )

    cohort_copy = self._spanner_client.run_in_transaction(
        _get_cohort_by_id,
        alias,
        int(copied_cohort_id),
        request.view != cohorts_pb2.PATHOLOGY_COHORT_VIEW_METADATA_ONLY,
    )

    cloud_logging_client.info(
        f'Created new PathologyCohort with resource name: {cohort_copy.name}.',
        {'copied_cohort_resource_name': cohort_copy.name},
    )

    return cohort_copy

  def share_pathology_cohort(
      self, request: cohorts_pb2.SharePathologyCohortRequest, alias: str
  ) -> cohorts_pb2.PathologyCohort:
    """Updates user access permissions for a cohort.

       Caller must have admin or owner role of cohort to modify access
       permissions.

    Args:
      request: A SharePathologyCohortRequest with the name of the cohort.
      alias: Alias of rpc caller.

    Returns:
      PathologyCohort with updated access permissions.

    Raises:
      RpcFailureError on failure.
    """
    resource_name = request.name
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.SHARE_PATHOLOGY_COHORT, resource_name
        )
    )
    cohort_id = pathology_resources_util.get_id_from_name(resource_name)
    with self._spanner_client.get_database_snapshot() as snapshot:
      if not spanner_util.check_caller_access(
          snapshot,
          alias,
          cohort_id,
          spanner_util.ResourceAccessPermissions.SHARE,
      ):
        raise rpc_status.RpcFailureError(
            rpc_status.build_rpc_method_status_and_log(
                grpc.StatusCode.PERMISSION_DENIED,
                f'User does not have permission to share cohort {cohort_id}.',
            )
        )
    cohort_to_share = self.get_pathology_cohort(
        cohorts_pb2.GetPathologyCohortRequest(
            name=resource_name, view=request.view
        ),
        alias,
    )
    # Only active cohorts can be shared.
    if not _get_is_active(cohort_to_share):
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INVALID_ARGUMENT,
              f'The cohort {cohort_id} is not in an active state and cannot be'
              ' shared.',
              {'cohort_state': cohort_to_share.cohort_metadata.cohort_stage},
          )
      )

    # Check for any invalid emails and grant valid permissions only.
    updated_cohort, invalid_emails = self._spanner_client.run_in_transaction(
        _share_cohort,
        cohort_to_share,
        request.user_access,
        request.cohort_access,
    )
    if invalid_emails:
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INVALID_ARGUMENT,
              (
                  f'Could not process share cohort {cohort_id} for'
                  f' some users. The following emails {invalid_emails} do'
                  ' not match the valid email format.'
              ),
              {'valid_email_regex': _EMAIL_REGEX_FLG.value},
          )
      )

    return updated_cohort

  def save_pathology_cohort(
      self, request: cohorts_pb2.SavePathologyCohortRequest, alias: str
  ) -> cohorts_pb2.SavePathologyCohortResponse:
    """Saves a cohort to a user's dashboard.

    Args:
      request: A SavePathologyCohortRequest with the name of the cohort.
      alias: Alias of rpc caller.

    Returns:
      SavePathologyCohortResponse

    Raises:
      RpcFailureError on failure.
    """
    resource_name = request.name
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.SAVE_PATHOLOGY_COHORT, resource_name
        )
    )
    cohort_id = pathology_resources_util.get_id_from_name(resource_name)
    caller_id = self._users_handler.identify_current_user(alias).user_id

    owner_id = _get_cohort_owner_id(resource_name)
    # Check if caller is cohort owner.
    if int(caller_id) == int(owner_id):
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INVALID_ARGUMENT,
              f'Cohort {cohort_id} is owned by user {caller_id}.'
              ' Only shared cohorts can be saved.',
          )
      )
    with self._spanner_client.get_database_snapshot() as snapshot:
      if not spanner_util.check_caller_access(
          snapshot,
          alias,
          cohort_id,
          spanner_util.ResourceAccessPermissions.SAVE,
      ):
        raise rpc_status.RpcFailureError(
            rpc_status.build_rpc_method_status_and_log(
                grpc.StatusCode.PERMISSION_DENIED,
                f'User does not have permission to save cohort {cohort_id}.',
            )
        )
    cohort_to_save = self.get_pathology_cohort(
        cohorts_pb2.GetPathologyCohortRequest(
            name=resource_name,
            view=cohorts_pb2.PATHOLOGY_COHORT_VIEW_METADATA_ONLY,
        ),
        alias,
    )
    # Only active cohorts can be saved.
    if not _get_is_active(cohort_to_save):
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INVALID_ARGUMENT,
              f'The cohort {cohort_id} is not in an active state and cannot be'
              ' saved.',
              {'cohort_state': cohort_to_save.cohort_metadata.cohort_stage},
          )
      )

    self._spanner_client.insert_or_update_data(
        ORCHESTRATOR_TABLE_NAMES[
            schema_resources.OrchestratorTables.SAVED_COHORTS
        ],
        schema_resources.SAVED_COHORTS_COLS,
        [[cohort_id, caller_id]],
    )
    cloud_logging_client.info(f'Saved cohort {cohort_id} for user {caller_id}.')

    return cohorts_pb2.SavePathologyCohortResponse()

  def unsave_pathology_cohort(
      self, request: cohorts_pb2.UnsavePathologyCohortRequest, alias: str
  ) -> cohorts_pb2.UnsavePathologyCohortResponse:
    """Unsaves a cohort and removes it from a user's dashboard.

    Args:
      request: An UnsavePathologyCohortRequest with the name of the cohort.
      alias: Alias of rpc caller.

    Returns:
      UnsavePathologyCohortResponse

    Raises:
      RpcFailureError on failure.
    """
    resource_name = request.name
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.UNSAVE_PATHOLOGY_COHORT, resource_name
        )
    )
    cohort_id = pathology_resources_util.get_id_from_name(resource_name)
    caller_id = self._users_handler.identify_current_user(alias).user_id
    self._spanner_client.delete_data(
        ORCHESTRATOR_TABLE_NAMES[
            schema_resources.OrchestratorTables.SAVED_COHORTS
        ],
        [[cohort_id, caller_id]],
    )

    cloud_logging_client.info(
        f'Unsaved cohort {cohort_id} for user {caller_id}.'
    )

    return cohorts_pb2.UnsavePathologyCohortResponse()
