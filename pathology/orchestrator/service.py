# Copyright 2024 Google LLC
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

"""Digital Pathology Service.

Implementation of DigitalPathologyServicer for DPAS API.
Implementations of the business logic of rpc calls are located in
resource-specific modules.
"""

from typing import Optional

import grpc

from google.longrunning import operations_pb2
from google.longrunning import operations_pb2_grpc
from pathology.orchestrator import grpc_util
from pathology.orchestrator import pathology_cohorts_handler
from pathology.orchestrator import pathology_operations_handler
from pathology.orchestrator import pathology_slides_handler
from pathology.orchestrator import pathology_users_handler
from pathology.orchestrator import rpc_status
from pathology.orchestrator.v1alpha import cohorts_pb2
from pathology.orchestrator.v1alpha import digital_pathology_pb2_grpc
from pathology.orchestrator.v1alpha import slides_pb2
from pathology.orchestrator.v1alpha import users_pb2


def parse_status_response(rpc_status_result: rpc_status.RpcMethodStatus,
                          context: grpc.ServicerContext) -> None:
  """Parses response from rpcs and sets context.

  Args:
    rpc_status_result: RpcStatusMethod status from an RpcFailureError.
    context: Grpc context for rpc call.

  Returns:
    None
  """
  context.set_code(rpc_status_result.code)
  context.set_details(rpc_status_result.error_msg)


class DigitalPathologyServicer(
    digital_pathology_pb2_grpc.DigitalPathologyServicer,
    operations_pb2_grpc.OperationsServicer):
  """DigitalPathology gRPC servicer."""

  def __init__(self):
    """Initializes the gRPC servicer with the handlers for methods."""

    super().__init__()
    self._cohorts_handler = pathology_cohorts_handler.PathologyCohortsHandler()
    self._operations_handler = pathology_operations_handler.PathologyOperationsHandler(
    )
    self._slides_handler = pathology_slides_handler.PathologySlidesHandler()
    self._users_handler = pathology_users_handler.PathologyUsersHandler()

  def CreatePathologyCohort(
      self, request: cohorts_pb2.CreatePathologyCohortRequest,
      context: grpc.ServicerContext) -> Optional[cohorts_pb2.PathologyCohort]:
    """Creates a PathologyCohort.

    Args:
        request: A CreatePathologyCohortRequest with cohort to create.
        context: A grpc.ServicerContext for use in the rpc.

    Returns:
        PathologyCohort
    """
    try:
      return self._cohorts_handler.create_pathology_cohort(
          request, grpc_util.get_email(context))
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)

  def GetPathologyCohort(
      self, request: cohorts_pb2.GetPathologyCohortRequest,
      context: grpc.ServicerContext) -> Optional[cohorts_pb2.PathologyCohort]:
    """Gets a PathologyCohort.

    Args:
      request: A GetPathologyCohortRequest with cohort to retrieve.
      context: A grpc.ServicerContext for use in the rpc.

    Returns:
      PathologyCohort
    """
    try:
      return self._cohorts_handler.get_pathology_cohort(
          request, grpc_util.get_email(context))
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)

  def DeletePathologyCohort(
      self, request: cohorts_pb2.DeletePathologyCohortRequest,
      context: grpc.ServicerContext) -> Optional[cohorts_pb2.PathologyCohort]:
    """Deletes (hides) a PathologyCohort.

    Args:
      request: A DeletePathologyCohortRequest with cohort to delete.
      context: A grpc.ServicerContext for use in the rpc.

    Returns:
      PathologyCohort or None if delete fails.
    """
    try:
      return self._cohorts_handler.delete_pathology_cohort(
          request, grpc_util.get_email(context))
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)

  def ListPathologyCohorts(
      self, request: cohorts_pb2.ListPathologyCohortsRequest,
      context: grpc.ServicerContext
  ) -> Optional[cohorts_pb2.ListPathologyCohortsResponse]:
    """Lists PathologyCohorts owned and shared.

    Args:
      request: A ListPathologyCohortRequest.
      context: A grpc.ServicerContext for use in the rpc.

    Returns:
      ListPathologyCohortsResponse
    """
    try:
      return self._cohorts_handler.list_pathology_cohorts(
          request, grpc_util.get_email(context))
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)

  def UpdatePathologyCohort(
      self, request: cohorts_pb2.UpdatePathologyCohortRequest,
      context: grpc.ServicerContext) -> Optional[cohorts_pb2.PathologyCohort]:
    """Updates a PathologyCohort.

    Args:
      request: An UpdatePathologyCohortRequest with cohort to update and fields
        to update contained in a field mask.
      context: A grpc.ServicerContext for use in the rpc.

    Returns:
      PathologyCohort or None on failure.
    """
    try:
      return self._cohorts_handler.update_pathology_cohort(
          request, grpc_util.get_email(context))
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)

  def UndeletePathologyCohort(
      self, request: cohorts_pb2.UndeletePathologyCohortRequest,
      context: grpc.ServicerContext) -> Optional[cohorts_pb2.PathologyCohort]:
    """Undelete (unhide) a PathologyCohort.

    Args:
      request: An UndeletePathologyCohortRequest with cohort to undelete.
      context: A grpc.ServicerContext for use in the rpc.

    Returns:
      PathologyCohort or None if undelete fails.
    """
    try:
      return self._cohorts_handler.undelete_pathology_cohort(
          request, grpc_util.get_email(context))
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)

  def ExportPathologyCohort(
      self, request: cohorts_pb2.ExportPathologyCohortRequest,
      context: grpc.ServicerContext) -> Optional[operations_pb2.Operation]:
    """Exports a PathologyCohort to a Cloud Storage bucket.

    Args:
      request: An ExportPathologyCohortRequest with cohort to export.
      context: A grpc.ServicerContext for use in the rpc.

    Returns:
      ExportPathologyCohortResponse
    """
    try:
      return self._cohorts_handler.export_pathology_cohort(
          request, grpc_util.get_email(context))
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)

  def TransferDeIdPathologyCohort(
      self, request: cohorts_pb2.TransferDeIdPathologyCohortRequest,
      context: grpc.ServicerContext) -> Optional[operations_pb2.Operation]:
    """De-Identifies and transfers a cohort to a De-Id DICOM store.

    Args:
      request: An TransferDeidPathologyCohortRequest with cohort to transfer.
      context: A grpc.ServicerContext for use in the rpc.

    Returns:
      Longrunning Operation instance or None if operation is not started.
    """
    try:
      return self._cohorts_handler.transfer_deid_pathology_cohort(
          request, grpc_util.get_email(context))
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)

  def CopyPathologyCohort(
      self, request: cohorts_pb2.CopyPathologyCohortRequest,
      context: grpc.ServicerContext) -> Optional[cohorts_pb2.PathologyCohort]:
    """Creates a duplicate of an existing pathology cohort.

    Args:
      request: A CopyPathologyCohortRequest with cohort to copy.
      context: A grpc.ServicerContext for use in the rpc.

    Returns:
      PathologyCohort
    """
    try:
      return self._cohorts_handler.copy_pathology_cohort(
          request, grpc_util.get_email(context))
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)

  def SharePathologyCohort(
      self, request: cohorts_pb2.SharePathologyCohortRequest,
      context: grpc.ServicerContext) -> Optional[cohorts_pb2.PathologyCohort]:
    """Updates user access permissions for a cohort.

       Caller must have admin or owner role of cohort to modify access
       permissions.

    Args:
      request: A SharePathologyCohortRequest with the name of the cohort.
      context: A grpc.ServicerContext for use in the rpc.

    Returns:
      PathologyCohort
    """
    try:
      return self._cohorts_handler.share_pathology_cohort(
          request, grpc_util.get_email(context))
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)

  def SavePathologyCohort(
      self, request: cohorts_pb2.SavePathologyCohortRequest,
      context: grpc.ServicerContext
  ) -> Optional[cohorts_pb2.SavePathologyCohortResponse]:
    """Saves a cohort to a user's dashboard.

    Args:
      request: A SavePathologyCohortRequest with the name of the cohort.
      context: A grpc.ServicerContext for use in the rpc.

    Returns:
      SavePathologyCohortResponse
    """
    try:
      return self._cohorts_handler.save_pathology_cohort(
          request, grpc_util.get_email(context))
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)

  def UnsavePathologyCohort(
      self, request: cohorts_pb2.UnsavePathologyCohortRequest,
      context: grpc.ServicerContext
  ) -> Optional[cohorts_pb2.UnsavePathologyCohortResponse]:
    """Unsaves a cohort and removes it from a user's dashboard.

    Args:
      request: An UnsavePathologyCohortRequest with the name of the cohort.
      context: A grpc.ServicerContext for use in the rpc.

    Returns:
      UnsavePathologyCohortResponse
    """
    try:
      return self._cohorts_handler.unsave_pathology_cohort(
          request, grpc_util.get_email(context))
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)

  def CreatePathologySlide(
      self, request: slides_pb2.CreatePathologySlideRequest,
      context: grpc.ServicerContext) -> Optional[slides_pb2.PathologySlide]:
    """Creates a PathologySlide.

    Args:
        request: A CreatePathologySlideRequest with slide to create.
        context: A grpc.ServicerContext for use in the rpc.

    Returns:
        PathologySlide
    """
    try:
      return self._slides_handler.create_pathology_slide(request)
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)

  def GetPathologySlide(
      self, request: slides_pb2.GetPathologySlideRequest,
      context: grpc.ServicerContext) -> Optional[slides_pb2.PathologySlide]:
    """Gets a PathologySlide.

    Args:
      request: A GetPathologySlideRequest with name of slide to retrieve.
      context: A grpc.ServicerContext for use in the rpc.

    Returns:
      PathologySlide
    """
    try:
      return self._slides_handler.get_pathology_slide(request)
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)

  def ListPathologySlides(
      self, request: slides_pb2.ListPathologySlidesRequest,
      context: grpc.ServicerContext
  ) -> Optional[slides_pb2.ListPathologySlidesResponse]:
    """Lists all PathologySlides with a filter field on dicom uri.

    Args:
      request: A ListPathologySlidesRequest.
      context: A grpc.ServicerContext for use in the rpc.

    Returns:
      ListPathologySlidesResponse
    """
    try:
      return self._slides_handler.list_pathology_slides(request)
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)

  def IdentifyCurrentUser(
      self, request: users_pb2.IdentifyCurrentUserRequest,
      context: grpc.ServicerContext) -> Optional[users_pb2.PathologyUser]:
    """Identifies the current PathologyUser from grpc context.

    Creates a new user if there is no current user.

    Args:
      request: IdentifyCurrentUserRequest.
      context: A grpc.ServicerContext for use in the rpc.

    Returns:
      PathologyUser instance of current user.
    """
    try:
      return self._users_handler.identify_current_user(
          grpc_util.get_email(context))
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)

  def GetOperation(
      self, request: operations_pb2.GetOperationRequest,
      context: grpc.ServicerContext) -> Optional[operations_pb2.Operation]:
    """Returns the long running operation instance of an initiated operation.

    Args:
      request: A GetOperationRequest with the resource name of the operation.
      context: A grpc.ServicerContext for use in the rpc.

    Returns:
      Operation instance or None on failure.
    """
    try:
      return self._operations_handler.get_operation(request)
    except rpc_status.RpcFailureError as e:
      return parse_status_response(e.status, context)
