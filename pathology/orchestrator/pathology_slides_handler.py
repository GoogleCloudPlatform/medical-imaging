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
"""Handler for PathologySlides rpc methods of DigitalPathology service.

Implementation of PathologySlides rpc methods for DPAS API.
"""

from google.api_core import exceptions
from google.cloud import spanner
import grpc

from pathology.orchestrator import id_generator
from pathology.orchestrator import logging_util
from pathology.orchestrator import pathology_resources_util
from pathology.orchestrator import rpc_status
from pathology.orchestrator.spanner import schema_resources
from pathology.orchestrator.v1alpha import slides_pb2
from pathology.shared_libs.logging_lib import cloud_logging_client
from pathology.shared_libs.spanner_lib import cloud_spanner_client


def build_slide_resource(
    scan_uid: int, dicom_uri: str
) -> slides_pb2.PathologySlide:
  """Builds a PathologySlide resource.

  Args:
    scan_uid: Scan unique Id.
    dicom_uri: Dicom Uri of slide.

  Returns:
    PathologySlide
  """
  resource_name = pathology_resources_util.convert_scan_uid_to_name(scan_uid)
  return slides_pb2.PathologySlide(
      name=resource_name, scan_unique_id=str(scan_uid), dicom_uri=dicom_uri
  )


def _get_slide_by_id(transaction, scan_uid: int) -> slides_pb2.PathologySlide:
  """Gets a slide from Spanner database by ScanUniqueId.

  Args:
    transaction: Transaction to run Spanner read/writes on.
    scan_uid: Unique id of slide to retrieve.

  Returns:
    PathologySlide proto instance

  Raises:
    RpcFailureError if slide is not found.
  """
  row = transaction.read(
      'Slides', schema_resources.SLIDES_COLS, spanner.KeySet([[scan_uid]])
  )
  try:
    row_dict = dict(zip(schema_resources.SLIDES_COLS, row.one()))
  except exceptions.NotFound as exc:
    raise rpc_status.RpcFailureError(
        rpc_status.build_rpc_method_status_and_log(
            grpc.StatusCode.NOT_FOUND,
            'Could not retrieve PathologySlide.'
            f'Slide with scan uid {scan_uid} not found.',
            exp=exc,
        )
    ) from exc

  return build_slide_resource(scan_uid, row_dict['DicomUri'])


def _search_slides_by_filter(
    transaction, dicom_filter: str
) -> slides_pb2.ListPathologySlidesResponse:
  """Performs a search on the Slides table for slides matching the dicom filter.

  Args:
    transaction: Transaction to run Spanner reads/writes on.
    dicom_filter: Filter string on dicom uri.

  Returns:
    ListPathologySlidesResponse

  Raises:
    RpcFailureError if a slide is not found.
  """
  response = slides_pb2.ListPathologySlidesResponse()
  rows = transaction.execute_sql(
      'SELECT * FROM Slides WHERE DicomUri LIKE @dicom_filter;',
      params={'dicom_filter': f'%{dicom_filter}%'},
      param_types={'dicom_filter': spanner.param_types.STRING},
  )
  for r in rows:
    response.pathology_slides.append(
        _get_slide_by_id(
            transaction, r[schema_resources.SLIDES_COLS.index('ScanUniqueId')]
        )
    )

  return response


def _verify_dicom_uri_matches_full_format(dicom_uri: str) -> bool:
  """Verifies that a dicom uri matches the expected full path format.

  Ex. projects/.../locations/.../datasets/.../dicomStores/.../dicomWeb
      /studies/.../series/.../instances/...

  Args:
    dicom_uri: DICOM uri to verify.

  Returns:
    True if uri matches format.
  """
  try:
    dicom_uri_parts = dicom_uri.split('/')
    assert len(dicom_uri_parts) == 13
    assert dicom_uri_parts[0] == 'projects'
    assert dicom_uri_parts[2] == 'locations'
    assert dicom_uri_parts[4] == 'datasets'
    assert dicom_uri_parts[6] == 'dicomStores'
    assert dicom_uri_parts[8] == 'dicomWeb'
    assert dicom_uri_parts[9] == 'studies'
    assert dicom_uri_parts[11] == 'series'
    return True
  except AssertionError:
    return False


# Method is public since it is used by pathology_cohorts_handler within a
# transaction.
def insert_slide_in_table(
    transaction: ..., slide: slides_pb2.PathologySlide
) -> int:
  """Inserts slide into Slides table.

  Args:
    transaction: Transaction to run Spanner read/writes in.
    slide: Slide to insert.

  Returns:
    int - scan unique id

  Raises:
    RpcFailureError if DicomUri is not full path or slide with same DicomUri
    already exists.
  """
  # Verify dicom_uri is full path format.
  if not _verify_dicom_uri_matches_full_format(slide.dicom_uri):
    raise rpc_status.RpcFailureError(
        rpc_status.build_rpc_method_status_and_log(
            grpc.StatusCode.INVALID_ARGUMENT,
            f'DicomUri: {slide.dicom_uri} is not in full path format.',
        )
    )
  dicom_hash = id_generator.generate_hash_id(slide.dicom_uri)
  # Try the dicom hash as primary key for Slides table, or rehash.
  scan_uid = dicom_hash
  list_of_tried_hashes = []
  max_retries = 3
  for i in range(0, max_retries):  # pylint: disable=unused-variable
    row = transaction.read(
        'Slides',
        schema_resources.SLIDES_COLS,
        spanner.KeySet(keys=[[scan_uid]]),
    )
    try:
      row_dict = dict(zip(schema_resources.SLIDES_COLS, row.one()))

      # Check if existing entry is a match.
      if slide.dicom_uri == row_dict['DicomUri']:
        raise rpc_status.RpcFailureError(
            rpc_status.build_rpc_method_status_and_log(
                grpc.StatusCode.ALREADY_EXISTS,
                f'Slide with Dicom Uri {slide.dicom_uri} already exists.',
            )
        )

      # Re-hash scan_uid to generate a new id.
      list_of_tried_hashes.append(scan_uid)
      scan_uid = id_generator.generate_hash_id(str(scan_uid))
    except exceptions.NotFound:
      transaction.insert(
          'Slides',
          schema_resources.SLIDES_COLS,
          [[scan_uid, slide.dicom_uri, dicom_hash]],
      )
      cloud_logging_client.info(
          f'Inserted slide {scan_uid}.',
          {'resource_name': f'pathologySlides/{scan_uid}'},
      )
      return scan_uid
  # Retries exceeded.
  raise rpc_status.RpcFailureError(
      rpc_status.build_rpc_method_status_and_log(
          grpc.StatusCode.INTERNAL,
          f'Could not insert Slide with DicomUri {slide.dicom_uri}.'
          'Unable to generate scan_unique_id due to hash collision.',
          {'hash_attempts': str(list_of_tried_hashes)},
      )
  )


class PathologySlidesHandler:
  """Handler for PathologySlides rpc methods of DigitalPathology service."""

  def __init__(self):
    super().__init__()
    self._spanner_client = cloud_spanner_client.CloudSpannerClient()

  def create_pathology_slide(
      self, request: slides_pb2.CreatePathologySlideRequest
  ) -> slides_pb2.PathologySlide:
    """Creates a PathologySlide.

    Args:
        request: A CreatePathologySlideRequest with slide to create.

    Returns:
        PathologySlide

    Raises:
      RpcFailureError on failure.
    """
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.CREATE_PATHOLOGY_SLIDE
        )
    )
    if not request.pathology_slide.dicom_uri:
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INVALID_ARGUMENT,
              'Slide must contain a DICOM Uri.',
          )
      )

    try:
      scan_uid = self._spanner_client.run_in_transaction(
          insert_slide_in_table, request.pathology_slide
      )
    except exceptions.Aborted as exc:
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INTERNAL, 'Insert for slide failed.', exp=exc
          )
      ) from exc

    resource_name = pathology_resources_util.convert_scan_uid_to_name(
        str(scan_uid)
    )
    slide = slides_pb2.PathologySlide(
        name=resource_name, scan_unique_id=str(scan_uid)
    )
    slide.MergeFrom(request.pathology_slide)
    cloud_logging_client.info(
        f'Created PathologySlide {resource_name}.',
        {'resource_name': resource_name},
    )

    return slide

  def get_pathology_slide(
      self, request: slides_pb2.GetPathologySlideRequest
  ) -> slides_pb2.PathologySlide:
    """Gets a PathologySlide.

    Args:
      request: A GetPathologySlideRequest with name of slide to retrieve.

    Returns:
      PathologySlide

    Raises:
      RpcFailureError on failure.
    """
    resource_name = request.name
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.GET_PATHOLOGY_SLIDE, resource_name
        )
    )
    scan_uid = pathology_resources_util.get_id_from_name(resource_name)
    cloud_logging_client.info(
        f'Retrieving PathologySlide {resource_name}.',
        {'resource_name': resource_name},
    )
    result = self._spanner_client.run_in_transaction(_get_slide_by_id, scan_uid)

    return result

  def list_pathology_slides(
      self, request: slides_pb2.ListPathologySlidesRequest
  ) -> slides_pb2.ListPathologySlidesResponse:
    """Lists all PathologySlides with a filter field on dicom uri.

    Args:
      request: A ListPathologySlidesRequest.

    Returns:
      ListPathologySlidesResponse

    Raises:
      RpcFailureError if a slide is not found.
    """
    dicom_filter = request.filter
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.LIST_PATHOLOGY_SLIDES
        )
    )
    cloud_logging_client.info(
        f'Retrieving PathologySlides by filter {dicom_filter}.',
        {'dicom_filter': dicom_filter},
    )
    return self._spanner_client.run_in_transaction(
        _search_slides_by_filter, dicom_filter
    )
