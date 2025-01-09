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
"""Util methods for Orchestrator spanner."""

import enum
from typing import List, Mapping, Optional, Set, Union

from google.api_core import exceptions
from google.cloud import spanner
from google.cloud.spanner_v1 import snapshot as s
from google.cloud.spanner_v1 import streamed
from google.cloud.spanner_v1 import transaction as tr
import grpc

from pathology.orchestrator import rpc_status
from pathology.orchestrator.spanner import schema_resources
from pathology.orchestrator.v1alpha import cohorts_pb2
from pathology.orchestrator.v1alpha import users_pb2
from pathology.shared_libs.logging_lib import cloud_logging_client
from pathology.shared_libs.spanner_lib import cloud_spanner_client


class ResourceAccessRole(enum.Enum):
  OWNER = 1
  VIEWER = 2
  EDITOR = 3
  ADMIN = 4
  OPEN_VIEW = 5
  OPEN_EDIT = 6


class ResourceAccessPermissions(enum.Enum):
  GET = 1
  UPDATE = 2
  DELETE = 3
  UNDELETE = 4
  TRANSFER_DEID = 5
  EXPORT = 6
  COPY = 7
  SHARE = 8
  SAVE = 9


# Permissions mapped to roles that have the privilege.
# Roles are sorted in decreasing privilege order.
_RESOURCE_ACCESS_MAP = {
    ResourceAccessPermissions.GET: [
        ResourceAccessRole.OPEN_EDIT,
        ResourceAccessRole.OPEN_VIEW,
        ResourceAccessRole.ADMIN,
        ResourceAccessRole.OWNER,
        ResourceAccessRole.EDITOR,
        ResourceAccessRole.VIEWER,
    ],
    ResourceAccessPermissions.UPDATE: [
        ResourceAccessRole.OPEN_EDIT,
        ResourceAccessRole.ADMIN,
        ResourceAccessRole.OWNER,
        ResourceAccessRole.EDITOR,
    ],
    ResourceAccessPermissions.DELETE: [
        ResourceAccessRole.ADMIN,
        ResourceAccessRole.OWNER,
    ],
    ResourceAccessPermissions.UNDELETE: [
        ResourceAccessRole.ADMIN,
        ResourceAccessRole.OWNER,
    ],
    ResourceAccessPermissions.TRANSFER_DEID: [
        ResourceAccessRole.OPEN_EDIT,
        ResourceAccessRole.ADMIN,
        ResourceAccessRole.OWNER,
        ResourceAccessRole.EDITOR,
    ],
    ResourceAccessPermissions.EXPORT: [
        ResourceAccessRole.OPEN_EDIT,
        ResourceAccessRole.ADMIN,
        ResourceAccessRole.OWNER,
        ResourceAccessRole.EDITOR,
    ],
    ResourceAccessPermissions.COPY: [
        ResourceAccessRole.OPEN_EDIT,
        ResourceAccessRole.OPEN_VIEW,
        ResourceAccessRole.ADMIN,
        ResourceAccessRole.OWNER,
        ResourceAccessRole.EDITOR,
        ResourceAccessRole.VIEWER,
    ],
    ResourceAccessPermissions.SHARE: [
        ResourceAccessRole.ADMIN,
        ResourceAccessRole.OWNER,
    ],
    ResourceAccessPermissions.SAVE: [
        ResourceAccessRole.OPEN_EDIT,
        ResourceAccessRole.OPEN_VIEW,
        ResourceAccessRole.ADMIN,
        ResourceAccessRole.OWNER,
        ResourceAccessRole.EDITOR,
        ResourceAccessRole.VIEWER,
    ],
}

_USER_ROLE_MAP = {
    ResourceAccessRole.ADMIN: (
        users_pb2.PathologyUserAccessRole.PATHOLOGY_USER_ACCESS_ROLE_ADMIN
    ),
    ResourceAccessRole.OWNER: (
        users_pb2.PathologyUserAccessRole.PATHOLOGY_USER_ACCESS_ROLE_OWNER
    ),
    ResourceAccessRole.EDITOR: (
        users_pb2.PathologyUserAccessRole.PATHOLOGY_USER_ACCESS_ROLE_EDITOR
    ),
    ResourceAccessRole.VIEWER: (
        users_pb2.PathologyUserAccessRole.PATHOLOGY_USER_ACCESS_ROLE_VIEWER
    ),
}

_OPEN_ACCESS_MAP = {
    ResourceAccessRole.OPEN_EDIT: cohorts_pb2.PATHOLOGY_COHORT_ACCESS_OPEN_EDIT,
    ResourceAccessRole.OPEN_VIEW: (
        cohorts_pb2.PATHOLOGY_COHORT_ACCESS_OPEN_VIEW_ONLY
    ),
}

ORCHESTRATOR_TABLE_NAMES = schema_resources.ORCHESTRATOR_TABLE_NAMES

_COHORTS_TABLE = ORCHESTRATOR_TABLE_NAMES[
    schema_resources.OrchestratorTables.COHORTS
]
_COHORT_USER_ACCESS_TABLE = ORCHESTRATOR_TABLE_NAMES[
    schema_resources.OrchestratorTables.COHORT_USER_ACCESS
]
_USER_ALIASES_TABLE = ORCHESTRATOR_TABLE_NAMES[
    schema_resources.OrchestratorTables.USER_ALIASES
]


def _check_cohort_owner(
    snapshot: s.Snapshot, caller_id: int, cohort_id: int
) -> bool:
  """Checks if the rpc caller is the cohort owner for specified cohort.

  Args:
    snapshot: Read-only snapshot of database.
    caller_id: User Id of rpc caller
    cohort_id: Cohort Id for cohort to check owner of.

  Returns:
    True if alias matches cohort owner.
  Raises:
    RpcFailureError if cohort is not found.
  """
  cohort_row = snapshot.read(
      _COHORTS_TABLE,
      schema_resources.COHORTS_COLS,
      spanner.KeySet(keys=[[cohort_id]]),
  )
  try:
    cohort_row_dict = dict(zip(schema_resources.COHORTS_COLS, cohort_row.one()))
  except exceptions.NotFound as exc:
    raise rpc_status.RpcFailureError(
        rpc_status.build_rpc_method_status_and_log(
            grpc.StatusCode.NOT_FOUND, f'Could not retrieve cohort {cohort_id}.'
        )
    ) from exc
  return caller_id == cohort_row_dict[schema_resources.CREATOR_USER_ID]


def _check_cohort_open_access(
    snapshot: s.Snapshot,
    cohort_id: int,
    access_permissions: cohorts_pb2.PathologyCohortAccess,
) -> bool:
  """Checks if the cohort has the specified open access permissions.

  Args:
    snapshot: Read-only snapshot of database.
    cohort_id: Cohort Id for cohort to check access to.
    access_permissions: Cohort access permission enum to check for.

  Returns:
    True if the cohort has the specified access permission.
  """
  sql_params = {
      'cohort_id': cohort_id,
      'access_permissions': access_permissions,
  }
  sql_param_types = {
      'cohort_id': spanner.param_types.INT64,
      'access_permissions': spanner.param_types.INT64,
  }
  rows = snapshot.execute_sql(
      f'SELECT {schema_resources.COHORT_ID} FROM {_COHORTS_TABLE} WHERE '
      f'{schema_resources.COHORT_ID}=@cohort_id AND'
      f' {schema_resources.COHORT_ACCESS}=@access_permissions',
      sql_params,
      sql_param_types,
  )
  try:
    rows.one()
    return True
  except exceptions.NotFound:
    return False


def _check_user_access(
    snapshot: s.Snapshot,
    caller_id: int,
    cohort_id: int,
    user_access_role: users_pb2.PathologyUserAccessRole,
) -> bool:
  """Checks if the rpc caller has the privileges for the role in Spanner.

  Args:
    snapshot: Read-only snapshot of database.
    caller_id: User Id of rpc caller.
    cohort_id: Cohort Id for cohort to check access to.
    user_access_role: PathologyUserAccessRole enum to check privileges for.

  Returns:
    True if user has the specified role.
  """
  sql_params = {
      'cohort_id': cohort_id,
      'caller_id': caller_id,
      'access_role': user_access_role,
  }
  sql_param_types = {
      'cohort_id': spanner.param_types.INT64,
      'caller_id': spanner.param_types.INT64,
      'access_role': spanner.param_types.INT64,
  }
  rows = snapshot.execute_sql(
      f'SELECT * FROM {_COHORT_USER_ACCESS_TABLE} WHERE '
      f'{schema_resources.COHORT_ID}=@cohort_id AND'
      f' {schema_resources.USER_ID}=@caller_id AND '
      f'{schema_resources.ACCESS_ROLE}=@access_role',
      sql_params,
      sql_param_types,
  )
  try:
    rows.one()
    return True
  except exceptions.NotFound:
    return False


def _check_user_role(
    snapshot: s.Snapshot,
    caller_id: int,
    cohort_id: int,
    role: ResourceAccessRole,
) -> bool:
  """Checks if the rpc caller has the specified role for the cohort.

  Args:
    snapshot: Read-only snapshot of database.
    caller_id: User Id of rpc caller.
    cohort_id: Cohort Id for cohort to check owner of.
    role: ResourceAccessRole enum to check for.

  Returns:
    True if user has the specified role.
  """
  if _check_user_access(snapshot, caller_id, cohort_id, _USER_ROLE_MAP[role]):
    return True
  if role == ResourceAccessRole.OWNER:
    return _check_cohort_owner(snapshot, caller_id, cohort_id)

  # No access granted to caller.
  return False


def search_user_by_alias(
    alias: str, transaction: Optional[Union[tr.Transaction, s.Snapshot]] = None
) -> int:
  """Searches the spanner database for a user given an alias.

  Args:
    alias: Alias to look up.
    transaction: Optional spanner transaction to use.

  Returns:
    int user_id

  Raises:
    exceptions.NotFound if alias row is not found.
  """
  if transaction:
    row = transaction.read(
        _USER_ALIASES_TABLE,
        [schema_resources.USER_ID],
        spanner.KeySet(keys=[[alias]]),
    )
  else:
    row = cloud_spanner_client.CloudSpannerClient().read_data(
        _USER_ALIASES_TABLE,
        [schema_resources.USER_ID],
        spanner.KeySet(keys=[[alias]]),
    )

  return row.one()[0]


def search_users_by_aliases(
    aliases: List[str], transaction: Union[tr.Transaction, s.Snapshot]
) -> List[int]:
  """Returns the list of user ids corresponding to a list of aliases.

  Args:
    aliases: List of aliases to search.
    transaction: Spanner transaction to use.

  Returns:
    List[int] - List[user_id]
  """
  result = []
  alias_keys = [[alias] for alias in aliases]
  rows = transaction.read(
      _USER_ALIASES_TABLE,
      [schema_resources.USER_ID],
      spanner.KeySet(keys=alias_keys),
  )

  for row in rows:
    result.append(row[0])

  return result


def get_emails_by_user_ids(
    snapshot: s.Snapshot, user_ids: List[int]
) -> Mapping[int, str]:
  """Gets the first user email from Spanner for each user id.

  Args:
    snapshot: Read-only snapshot of database.
    user_ids: UserIds of users to look up.

  Returns:
    Map of user id to email. Only contains user ids for which a corresponding
      email is found.
  """
  if not user_ids:
    return {}

  rows = snapshot.execute_sql(
      f'SELECT {schema_resources.USER_ID}, {schema_resources.USER_ALIAS} '
      f'FROM {_USER_ALIASES_TABLE} WHERE '
      f'  {schema_resources.USER_ID} IN UNNEST(@user_ids)'
      f'  AND {schema_resources.ALIAS_TYPE}=@alias_type',
      params={
          'user_ids': user_ids,
          'alias_type': users_pb2.PATHOLOGY_USER_ALIAS_TYPE_EMAIL,
      },
      param_types={
          'user_ids': spanner.param_types.Array(spanner.param_types.INT64),
          'alias_type': spanner.param_types.INT64,
      },
  )

  ids_to_emails = {}
  for user_id, user_email in rows:
    if user_id not in ids_to_emails:
      ids_to_emails[user_id] = user_email
    else:
      cloud_logging_client.warning(
          f'There are multiple emails associated with the UserId: {user_id}.'
      )
  return ids_to_emails


def get_email_by_user_id(snapshot: s.Snapshot, user_id: int) -> Optional[str]:
  """Gets the first user email from Spanner given a user id.

  Args:
    snapshot: Read-only snapshot of database.
    user_id: UserId of user to look up.

  Returns:
    str email or None if id does not exist.
  """
  id_to_email = get_emails_by_user_ids(snapshot, [user_id])
  return id_to_email.get(user_id, None)


def get_cohort_owner(
    transaction: Union[tr.Transaction, s.Snapshot], cohort_id: int
) -> int:
  """Returns the UserId of the cohort owner.

  Args:
   transaction: Transaction to run Spanner read/writes on.
   cohort_id: Id of cohort to look up.

  Returns:
    int user_id

  Raises:
    exceptions.NotFound if cohort row does not exist.
  """
  row = transaction.read(
      _COHORTS_TABLE,
      [schema_resources.CREATOR_USER_ID],
      spanner.KeySet(keys=[[cohort_id]]),
  ).one()
  return row[0]


def check_caller_access(
    snapshot: s.Snapshot,
    alias: str,
    cohort_id: int,
    access: ResourceAccessPermissions,
    caller_id: Optional[int] = None,
) -> bool:
  """Checks if the caller has access to the cohort through the specified role.

  Args:
    snapshot: Read-only snapshot of Spanner database.
    alias: Alias of rpc caller.
    cohort_id: Cohort Id of cohort to check if access is permitted for.
    access: ResourceAccessPermissions enum representing permissions to check.
    caller_id: Optional caller id to skip retrieving the caller id.

  Returns:
    True if sharing link grants access.

  Raises:
    RpcFailureError if cohort is not found.
  """
  roles_granting_permission = _RESOURCE_ACCESS_MAP[access]
  if caller_id is None:
    try:
      caller_id = search_user_by_alias(alias, snapshot)
    except exceptions.NotFound as exp:
      cloud_logging_client.error(
          f'Could not find user associated with alias {alias}.', exp
      )
      return False

  # Check each role starting with highest privileges.
  for role in roles_granting_permission:
    if (
        role == ResourceAccessRole.OPEN_EDIT
        or role == ResourceAccessRole.OPEN_VIEW
    ):
      if _check_cohort_open_access(snapshot, cohort_id, _OPEN_ACCESS_MAP[role]):
        return True
    else:
      if _check_user_role(snapshot, caller_id, cohort_id, role):
        return True
  # Caller has no access to cohort.
  return False


def check_if_cohorts_owner_or_admin(
    snapshot: s.Snapshot, user_id: int, cohort_ids: List[int]
) -> Set[int]:
  """Checks if the user is the cohort owner or admin for specified cohorts.

  Args:
    snapshot: Read-only snapshot of database.
    user_id: UserId of caller.
    cohort_ids: List of cohort ids to check owner/admin of.

  Returns:
    Set of cohort ids for which user is cohort owner or admin.
  """
  owner_or_admin_cohort_ids = set()
  try:
    rows = snapshot.execute_sql(
        f'SELECT {schema_resources.COHORT_ID} '
        f'FROM {_COHORT_USER_ACCESS_TABLE} '
        'WHERE '
        f'  {schema_resources.COHORT_ID} IN UNNEST(@cohort_list) '
        f'  AND {schema_resources.USER_ID}=@user_id '
        f'  AND {schema_resources.ACCESS_ROLE} IN UNNEST(@access_list)',
        params={
            'cohort_list': cohort_ids,
            'user_id': user_id,
            'access_list': [
                users_pb2.PATHOLOGY_USER_ACCESS_ROLE_OWNER,
                users_pb2.PATHOLOGY_USER_ACCESS_ROLE_ADMIN,
            ],
        },
        param_types={
            'cohort_list': spanner.param_types.Array(spanner.param_types.INT64),
            'user_id': spanner.param_types.INT64,
            'access_list': spanner.param_types.Array(spanner.param_types.INT64),
        },
    )
    for r in rows:
      owner_or_admin_cohort_ids.add(r[0])
  except exceptions.NotFound:
    pass

  if not owner_or_admin_cohort_ids:
    # TODO: Remove once data is backfilled.
    # Temporary fix for old data, check owner by creator userid.
    for cohort_id in cohort_ids:
      try:
        cohort_row = snapshot.read(
            _COHORTS_TABLE,
            schema_resources.COHORTS_COLS,
            spanner.KeySet(keys=[[cohort_id]]),
        ).one()
        if (
            cohort_row[
                schema_resources.COHORTS_COLS.index(
                    schema_resources.CREATOR_USER_ID
                )
            ]
            == user_id
        ):
          owner_or_admin_cohort_ids.add(cohort_id)
      except exceptions.NotFound:
        pass
  return owner_or_admin_cohort_ids


def check_if_cohort_owner_or_admin(
    snapshot: s.Snapshot, user_id: int, cohort_id: int
) -> bool:
  """Checks if the user is the cohort owner or admin for specified cohort.

  Args:
    snapshot: Read-only snapshot of database.
    user_id: UserId of caller.
    cohort_id: Cohort Id for cohort to check owner/admin of.

  Returns:
    True if user matches cohort owner or admin.
  """
  return cohort_id in check_if_cohorts_owner_or_admin(
      snapshot, user_id, [cohort_id]
  )


def _get_saved_cohort_ids_for_user(
    snapshot: s.Snapshot,
    user_id: int,
) -> List[int]:
  """Returns list of saved cohort ids for the user.

  Args:
    snapshot: Read-only snapshot of database.
    user_id: User id to search for.

  Returns:
    List of cohort ids.
  """
  saved_cohorts = ORCHESTRATOR_TABLE_NAMES[
      schema_resources.OrchestratorTables.SAVED_COHORTS
  ]
  cohort_ids = []

  rows = snapshot.execute_sql(
      f'SELECT {saved_cohorts}.{schema_resources.COHORT_ID} FROM '
      f'{saved_cohorts} WHERE {saved_cohorts}.{schema_resources.USER_ID} '
      '= @user_id;',
      params={
          'user_id': user_id,
      },
      param_types={
          'user_id': spanner.param_types.INT64,
      },
  )
  for r in rows:
    cohort_ids.append(
        r[schema_resources.SAVED_COHORTS_COLS.index(schema_resources.COHORT_ID)]
    )

  return cohort_ids


def _get_all_direct_access_cohort_ids(
    snapshot: s.Snapshot, user_id: int
) -> List[int]:
  """Returns all shared cohort ids for the user.

  Args:
    snapshot: Read-only snapshot of database.
    user_id: User id to search for.

  Returns:
    List of cohort ids.
  """
  cohort_user_access = ORCHESTRATOR_TABLE_NAMES[
      schema_resources.OrchestratorTables.COHORT_USER_ACCESS
  ]
  cohort_ids = []

  rows = snapshot.execute_sql(
      f'SELECT {schema_resources.COHORT_ID} FROM {cohort_user_access}'
      f' WHERE {cohort_user_access}.{schema_resources.USER_ID}=@user_id '
      f'AND {schema_resources.ACCESS_ROLE} != @owner;',
      params={
          'user_id': user_id,
          'owner': users_pb2.PATHOLOGY_USER_ACCESS_ROLE_OWNER,
      },
      param_types={
          'user_id': spanner.param_types.INT64,
          'owner': spanner.param_types.INT64,
      },
  )
  for r in rows:
    cohort_ids.append(r[0])

  return cohort_ids


def _get_cohort_ids_by_access_role(
    snapshot: s.Snapshot,
    user_id: int,
    access_role: users_pb2.PathologyUserAccessRole,
) -> List[int]:
  """Returns cohort ids for the user by access role.

  Args:
    snapshot: Read-only snapshot of database.
    user_id: User id to search for.
    access_role: Access role to search on.

  Returns:
    List of cohort ids.
  """
  cohort_ids = []

  rows = snapshot.read(
      _COHORT_USER_ACCESS_TABLE,
      [schema_resources.COHORT_ID],
      spanner.KeySet(keys=[[user_id, access_role]]),
      schema_resources.COHORTS_BY_USER_ID_ACCESS,
  )
  for r in rows:
    cohort_ids.append(r[0])

  return cohort_ids


# TODO: Remove once data is backfilled.
def _get_owned_cohorts_ids_for_user_by_creator(
    snapshot: s.Snapshot, user_id: int
) -> List[int]:
  """Returns owned cohorts for the user by CreatorUserId.

  Temporary fix for old data that doesn't have CohortUserAccess populated.

  Args:
    snapshot: Read-only snapshot of database.
    user_id: User id to search for.

  Returns:
    List of cohort ids.
  """
  cohort_ids = []

  rows = snapshot.read(
      _COHORTS_TABLE,
      [schema_resources.COHORT_ID],
      spanner.KeySet(keys=[[user_id]]),
      schema_resources.COHORTS_BY_CREATOR_USER_ID,
  )
  for r in rows:
    cohort_ids.append(r[0])

  return cohort_ids


# TODO: Reuse once data is backfilled or delete.
def _get_owned_cohorts_for_user(
    transaction: tr.Transaction, user_id: int
) -> streamed.StreamedResultSet:
  """Returns owned cohorts for the user.

  Args:
    transaction: Transaction to run Spanner read on.
    user_id: User id to search for.

  Returns:
    Streamed result set of rows.
  """
  cohort_user_access = ORCHESTRATOR_TABLE_NAMES[
      schema_resources.OrchestratorTables.COHORT_USER_ACCESS
  ]

  return transaction.execute_sql(
      f'SELECT {cohort_user_access}.{schema_resources.COHORT_ID} '
      f'FROM {cohort_user_access} WHERE'
      f' {cohort_user_access}.{schema_resources.USER_ID} = @user_id AND'
      f' {cohort_user_access}.{schema_resources.ACCESS_ROLE} = @access_role;',
      params={
          'user_id': user_id,
          'access_role': users_pb2.PATHOLOGY_USER_ACCESS_ROLE_OWNER,
      },
      param_types={
          'user_id': spanner.param_types.INT64,
          'access_role': spanner.param_types.INT64,
      },
  )


def get_cohorts_for_user(
    snapshot: s.Snapshot,
    user_id: int,
    access_role: users_pb2.PathologyUserAccessRole,
) -> Set[int]:
  """Returns list of cohort ids for the user with the specified access role.

  Args:
    snapshot: Read-only snapshot of database.
    user_id: User id to search for.
    access_role: Access role to search on.

  Returns:
    Set of cohort ids.
  """
  result = set()
  if access_role == users_pb2.PATHOLOGY_USER_ACCESS_ROLE_OWNER:
    result.update(_get_owned_cohorts_ids_for_user_by_creator(snapshot, user_id))
  else:
    saved_cohort_ids = set(_get_saved_cohort_ids_for_user(snapshot, user_id))
    direct_access_ids = set(
        _get_cohort_ids_by_access_role(snapshot, user_id, access_role)
    )

    result.update(saved_cohort_ids.intersection(direct_access_ids))

    # If not direct access, check open access.
    for cohort_id in saved_cohort_ids.difference(direct_access_ids):
      if (
          access_role == users_pb2.PATHOLOGY_USER_ACCESS_ROLE_EDITOR
          and _check_cohort_open_access(
              snapshot, cohort_id, cohorts_pb2.PATHOLOGY_COHORT_ACCESS_OPEN_EDIT
          )
      ):
        result.add(cohort_id)
      elif (
          access_role == users_pb2.PATHOLOGY_USER_ACCESS_ROLE_VIEWER
          and _check_cohort_open_access(
              snapshot,
              cohort_id,
              cohorts_pb2.PATHOLOGY_COHORT_ACCESS_OPEN_VIEW_ONLY,
          )
      ):
        result.add(cohort_id)

  return result


def get_all_cohorts_for_user(snapshot: s.Snapshot, user_id: int) -> Set[int]:
  """Returns list of all accessible cohort ids for the user.

  Args:
    snapshot: Read-only snapshot of database.
    user_id: User id to search for.

  Returns:
    Set of cohort ids.
  """
  result = set()

  result.update(_get_owned_cohorts_ids_for_user_by_creator(snapshot, user_id))
  saved_cohort_ids = set(_get_saved_cohort_ids_for_user(snapshot, user_id))
  direct_access_ids = set(_get_all_direct_access_cohort_ids(snapshot, user_id))

  result.update(saved_cohort_ids.intersection(direct_access_ids))

  # If not direct access, check open access.
  cohort_id_keys = [[x] for x in saved_cohort_ids.difference(direct_access_ids)]
  rows = snapshot.read(
      ORCHESTRATOR_TABLE_NAMES[schema_resources.OrchestratorTables.COHORTS],
      [schema_resources.COHORT_ID, schema_resources.COHORT_ACCESS],
      keyset=spanner.KeySet(keys=cohort_id_keys),
  )

  for r in rows:
    if (
        r[1] == cohorts_pb2.PATHOLOGY_COHORT_ACCESS_OPEN_EDIT
        or r[1] == cohorts_pb2.PATHOLOGY_COHORT_ACCESS_OPEN_VIEW_ONLY
    ):
      result.add(r[0])

  return result
