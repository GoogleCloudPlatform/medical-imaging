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
"""Flask REST server for orchestrator."""

import http
import multiprocessing
import os

from absl import app as absl_app
from absl import flags
import flask
import flask_cors
from google.api_core import exceptions
from gunicorn.app.base import BaseApplication
import requests

from google.longrunning import operations_pb2
from google.protobuf import json_format
from pathology.orchestrator import grpc_util
from pathology.orchestrator import pathology_cohorts_handler
from pathology.orchestrator import pathology_operations_handler
from pathology.orchestrator import pathology_slides_handler
from pathology.orchestrator import pathology_users_handler
from pathology.orchestrator import rpc_status
from pathology.orchestrator.v1alpha import cohorts_pb2
from pathology.orchestrator.v1alpha import slides_pb2
from pathology.shared_libs.build_version import build_version
from pathology.shared_libs.flags import secret_flag_utils
from pathology.shared_libs.iap_auth_lib import auth
from pathology.shared_libs.logging_lib import cloud_logging_client


GUNICORN_WORKER_CLASS_FLG = flags.DEFINE_string(
    'gunicorn_worker_class',
    secret_flag_utils.get_secret_or_env('GUNICORN_WORKER_CLASS', 'gthread'),
    'GUnicorn worker class type',
)
GUNICORN_WORKERS_FLG = flags.DEFINE_integer(
    'gunicorn_workers',
    int(
        secret_flag_utils.get_secret_or_env(
            'GUNICORN_WORKERS', (2 * multiprocessing.cpu_count()) + 1
        )
    ),
    'Number of processes GUnicorn should launch',
)
GUNICORN_THREADS_FLG = flags.DEFINE_integer(
    'gunicorn_threads',
    int(secret_flag_utils.get_secret_or_env('GUNICORN_THREADS', 5)),
    'Number of threads each GUnicorn processes should launch',
)

API_PORT_FLG = flags.DEFINE_integer(
    'port',
    int(secret_flag_utils.get_secret_or_env('PORT', 10000)),
    'port to listen on',
)

# Sites to allow requests from for CORS.
# Using env variable directly to allow it to be used in decorator.
ORIGINS_FLG = secret_flag_utils.get_secret_or_env('ORIGINS', '').split(',')

# Required to initialize build version in logs of forked child processes.
os.register_at_fork(
    after_in_child=build_version.init_cloud_logging_build_version
)

flask_app = flask.Flask(__name__)
_USER_INFO_REQUEST_URL = 'https://www.googleapis.com/userinfo/v2/me'
_AUTH_HEADER_KEY = 'Authorization'
_DEFAULT_COHORT_VIEW = 'PATHOLOGY_COHORT_VIEW_UNSPECIFIED'


def _convert_user_id_to_name(user_id: str) -> str:
  """Returns the resource name of the user given the user id."""

  return f'pathologyUsers/{user_id}'


def _get_user_email():
  """Uses the caller's id_token to fetch their e-mail address.

  Returns:
    str - email of caller.
  """
  user_token_header = flask.request.headers.get(_AUTH_HEADER_KEY)
  session = requests.Session()
  response = session.get(
      _USER_INFO_REQUEST_URL, headers={_AUTH_HEADER_KEY: user_token_header}
  ).json()
  return response['email']


def _log_request(request: flask.Request, user_email: str) -> None:
  """Logs incoming method."""
  try:
    cloud_logging_client.info(
        f'Calling DPAS API function {request.method}.',
        {
            'user_email': user_email,
            'httpRequest': {
                'requestMethod': request.method,
                'requestUrl': request.path,
            },
            'json_body': request.get_json(silent=True),
        },
    )
  except exceptions.Unknown:
    # Error in logging client, do nothing
    pass


def _parse_status_response(
    rpc_status_response: rpc_status.RpcMethodStatus,
) -> flask.Response:
  """Parses RpcMethodStatus and packages error as a flask Response.

  Args:
    rpc_status_response: RpcMethodStatus containing code and message to parse.

  Returns:
    flask.Response
  """
  return flask.Response(
      rpc_status_response.error_msg,
      status=grpc_util.convert_grpc_code_to_http(rpc_status_response.code),
      mimetype='text/html',
  )


@flask_app.before_request
def handle_preflight_methods() -> flask.Response:
  if flask.request.method.lower() == 'options':
    return flask.Response(
        status=http.HTTPStatus.OK,
        headers={
            'Access-Control-Allow-Origin': ORIGINS_FLG,
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Methods': '*',
        },
    )


@flask_app.route('/')
def healthcheck():
  return ''


# TODO: Refactor this into a Flask blueprint.
@flask_app.route('/api/v1alpha:identifyCurrentUser', methods=['POST'])
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def identify_current_user():
  """Process identify_current_user."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  users_handler = pathology_users_handler.PathologyUsersHandler()
  try:
    response = users_handler.identify_current_user(user_email)
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


# PathologyCohorts Methods.
@flask_app.route(
    '/api/v1alpha/pathologyUsers/<string:user_id>/pathologyCohorts',
    methods=['POST'],
)
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def create_pathology_cohort(user_id: str) -> flask.Response:
  """Process create_pathology_cohort."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  cohorts_handler = pathology_cohorts_handler.PathologyCohortsHandler()
  cohort_request = json_format.ParseDict(
      flask.request.get_json(), cohorts_pb2.CreatePathologyCohortRequest()
  )
  try:
    cohort_request.parent = _convert_user_id_to_name(user_id)
    response = cohorts_handler.create_pathology_cohort(
        cohort_request, user_email
    )
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


@flask_app.route(
    '/api/v1alpha/pathologyUsers/<string:user_id>/pathologyCohorts/<string:cohort_id>',
    methods=['GET'],
)
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def get_pathology_cohort(user_id: str, cohort_id: str) -> flask.Response:
  """Process get_pathology_cohort."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  view = flask.request.args.get('view', default=_DEFAULT_COHORT_VIEW, type=str)
  link_token = flask.request.args.get('linkToken', default=None, type=str)
  cohorts_handler = pathology_cohorts_handler.PathologyCohortsHandler()
  request = cohorts_pb2.GetPathologyCohortRequest(
      name=f'{_convert_user_id_to_name(user_id)}/pathologyCohorts/{cohort_id}',
      view=cohorts_pb2.PathologyCohortView.Value(view),
      link_token=link_token,
  )
  try:
    response = cohorts_handler.get_pathology_cohort(request, user_email)
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


@flask_app.route(
    '/api/v1alpha/pathologyUsers/<string:user_id>/pathologyCohorts',
    methods=['GET'],
)
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def list_pathology_cohorts(user_id: str) -> flask.Response:
  """Process list_pathology_cohorts."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  view = flask.request.args.get('view', default=_DEFAULT_COHORT_VIEW, type=str)
  access_filter = flask.request.args.get('filter', default='', type=str)
  cohorts_handler = pathology_cohorts_handler.PathologyCohortsHandler()
  request = cohorts_pb2.ListPathologyCohortsRequest(
      parent=_convert_user_id_to_name(user_id),
      view=cohorts_pb2.PathologyCohortView.Value(view),
      filter=access_filter,
  )
  try:
    response = cohorts_handler.list_pathology_cohorts(request, user_email)
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


@flask_app.route(
    '/api/v1alpha/pathologyUsers/<string:user_id>/pathologyCohorts/<string:cohort_id>',
    methods=['PATCH'],
)
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def update_pathology_cohort(user_id: str, cohort_id: str) -> flask.Response:
  """Process update_pathology_cohort."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  cohorts_handler = pathology_cohorts_handler.PathologyCohortsHandler()
  update_request = json_format.ParseDict(
      flask.request.get_json(), cohorts_pb2.UpdatePathologyCohortRequest()
  )
  update_request.pathology_cohort.name = (
      f'{_convert_user_id_to_name(user_id)}/pathologyCohorts/{cohort_id}'
  )

  # Use link token from request body, if not present, retrieve query parameter.
  if not update_request.link_token:
    update_request.link_token = flask.request.args.get(
        'linkToken', default='', type=str
    )

  # Use view from request body, if not present, retrieve query parameter.
  if not update_request.view:
    view = flask.request.args.get(
        'view', default=_DEFAULT_COHORT_VIEW, type=str
    )
    update_request.view = cohorts_pb2.PathologyCohortView.Value(view)

  try:
    response = cohorts_handler.update_pathology_cohort(
        update_request, _get_user_email()
    )
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


@flask_app.route(
    '/api/v1alpha/pathologyUsers/<string:user_id>/pathologyCohorts/<string:cohort_id>',
    methods=['DELETE'],
)
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def delete_pathology_cohort(user_id: str, cohort_id: str) -> flask.Response:
  """Process delete_pathology_cohort."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  cohorts_handler = pathology_cohorts_handler.PathologyCohortsHandler()
  delete_request = cohorts_pb2.DeletePathologyCohortRequest(
      name=f'{_convert_user_id_to_name(user_id)}/pathologyCohorts/{cohort_id}',
      link_token=flask.request.args.get('linkToken', default='', type=str),
  )

  try:
    response = cohorts_handler.delete_pathology_cohort(
        delete_request, user_email
    )
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


@flask_app.route(
    '/api/v1alpha/pathologyUsers/<string:user_id>/pathologyCohorts/<string:cohort_id>:undelete',
    methods=['POST'],
)
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def undelete_pathology_cohort(user_id: str, cohort_id: str) -> flask.Response:
  """Process undelete_pathology_cohort."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  cohorts_handler = pathology_cohorts_handler.PathologyCohortsHandler()
  undelete_request = cohorts_pb2.UndeletePathologyCohortRequest(
      name=f'{_convert_user_id_to_name(user_id)}/pathologyCohorts/{cohort_id}',
      link_token=flask.request.args.get('linkToken', default='', type=str),
  )

  try:
    response = cohorts_handler.undelete_pathology_cohort(
        undelete_request, user_email
    )
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


@flask_app.route(
    '/api/v1alpha/pathologyUsers/<string:user_id>/pathologyCohorts/<string:cohort_id>:export',
    methods=['POST'],
)
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def export_pathology_cohort(user_id: str, cohort_id: str) -> flask.Response:
  """Process export_pathology_cohort."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  cohorts_handler = pathology_cohorts_handler.PathologyCohortsHandler()
  export_request = json_format.ParseDict(
      flask.request.get_json(), cohorts_pb2.ExportPathologyCohortRequest()
  )
  export_request.name = (
      f'{_convert_user_id_to_name(user_id)}/pathologyCohorts/{cohort_id}'
  )

  # Use link token from request body, if not present, retrieve query parameter.
  if not export_request.link_token:
    export_request.link_token = flask.request.args.get(
        'linkToken', default='', type=str
    )

  try:
    response = cohorts_handler.export_pathology_cohort(
        export_request, user_email
    )
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


@flask_app.route(
    '/api/v1alpha/pathologyUsers/<string:user_id>/pathologyCohorts/<string:cohort_id>:transfer',
    methods=['POST'],
)
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def transfer_deid_pathology_cohort(
    user_id: str, cohort_id: str
) -> flask.Response:
  """Process transfer_deid_pathology_cohort."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  cohorts_handler = pathology_cohorts_handler.PathologyCohortsHandler()
  deid_request = json_format.ParseDict(
      flask.request.get_json(), cohorts_pb2.TransferDeIdPathologyCohortRequest()
  )
  deid_request.name = (
      f'{_convert_user_id_to_name(user_id)}/pathologyCohorts/{cohort_id}'
  )

  # Use link token from request body, if not present, retrieve query parameter.
  if not deid_request.link_token:
    deid_request.link_token = flask.request.args.get(
        'linkToken', default='', type=str
    )

  try:
    response = cohorts_handler.transfer_deid_pathology_cohort(
        deid_request, user_email
    )
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


@flask_app.route(
    '/api/v1alpha/pathologyUsers/<string:user_id>/pathologyCohorts/<string:cohort_id>:copy',
    methods=['POST'],
)
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def copy_pathology_cohort(user_id: str, cohort_id: str) -> flask.Response:
  """Process copy_pathology_cohort."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  cohorts_handler = pathology_cohorts_handler.PathologyCohortsHandler()
  copy_request = json_format.ParseDict(
      flask.request.get_json(), cohorts_pb2.CopyPathologyCohortRequest()
  )
  copy_request.name = (
      f'{_convert_user_id_to_name(user_id)}/pathologyCohorts/{cohort_id}'
  )

  # Use link token from request body, if not present, retrieve query parameter.
  if not copy_request.link_token:
    copy_request.link_token = flask.request.args.get(
        'linkToken', default='', type=str
    )

  # Use view from request body, if not present, retrieve query parameter.
  # View can optionally be supplied as a query parameter to maintain the
  # standard with GET.
  if not copy_request.view:
    view = flask.request.args.get(
        'view', default=_DEFAULT_COHORT_VIEW, type=str
    )
    copy_request.view = cohorts_pb2.PathologyCohortView.Value(view)

  try:
    response = cohorts_handler.copy_pathology_cohort(copy_request, user_email)
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


@flask_app.route(
    '/api/v1alpha/pathologyUsers/<string:user_id>/pathologyCohorts/<string:cohort_id>:share',
    methods=['POST'],
)
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def share_pathology_cohort(user_id: str, cohort_id: str) -> flask.Response:
  """Process share_pathology_cohort."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  cohorts_handler = pathology_cohorts_handler.PathologyCohortsHandler()
  share_request = json_format.ParseDict(
      flask.request.get_json(), cohorts_pb2.SharePathologyCohortRequest()
  )
  share_request.name = (
      f'{_convert_user_id_to_name(user_id)}/pathologyCohorts/{cohort_id}'
  )

  # Use view from request body, if not present, retrieve query parameter.
  if not share_request.view:
    share_request.view = cohorts_pb2.PathologyCohortView.Value(
        flask.request.args.get('view', default=_DEFAULT_COHORT_VIEW, type=str)
    )
  try:
    response = cohorts_handler.share_pathology_cohort(share_request, user_email)
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


@flask_app.route(
    '/api/v1alpha/pathologyUsers/<string:user_id>/pathologyCohorts/<string:cohort_id>:save',
    methods=['POST'],
)
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def save_pathology_cohort(user_id: str, cohort_id: str) -> flask.Response:
  """Process save_pathology_cohort."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  cohorts_handler = pathology_cohorts_handler.PathologyCohortsHandler()
  save_request = json_format.ParseDict(
      flask.request.get_json(), cohorts_pb2.SavePathologyCohortRequest()
  )
  save_request.name = (
      f'{_convert_user_id_to_name(user_id)}/pathologyCohorts/{cohort_id}'
  )

  try:
    response = cohorts_handler.save_pathology_cohort(save_request, user_email)
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


@flask_app.route(
    '/api/v1alpha/pathologyUsers/<string:user_id>/pathologyCohorts/<string:cohort_id>:unsave',
    methods=['POST'],
)
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def unsave_pathology_cohort(user_id: str, cohort_id: str) -> flask.Response:
  """Process unsave_pathology_cohort."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  cohorts_handler = pathology_cohorts_handler.PathologyCohortsHandler()
  unsave_request = json_format.ParseDict(
      flask.request.get_json(), cohorts_pb2.UnsavePathologyCohortRequest()
  )
  unsave_request.name = (
      f'{_convert_user_id_to_name(user_id)}/pathologyCohorts/{cohort_id}'
  )

  try:
    response = cohorts_handler.unsave_pathology_cohort(
        unsave_request, user_email
    )
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


# PathologySlides Methods.
@flask_app.route('/api/v1alpha/pathologySlides', methods=['POST'])
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def create_pathology_slide() -> flask.Response:
  """Process create_pathology_slide."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  slides_handler = pathology_slides_handler.PathologySlidesHandler()
  slide_request = json_format.ParseDict(
      flask.request.get_json(), slides_pb2.CreatePathologySlideRequest()
  )
  try:
    response = slides_handler.create_pathology_slide(slide_request)
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


@flask_app.route(
    '/api/v1alpha/pathologySlides/<string:scan_uid>', methods=['GET']
)
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def get_pathology_slide(scan_uid: str) -> flask.Response:
  """Process get_pathology_slide."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  slides_handler = pathology_slides_handler.PathologySlidesHandler()
  request = slides_pb2.GetPathologySlideRequest(
      name=f'pathologySlides/{scan_uid}'
  )
  try:
    response = slides_handler.get_pathology_slide(request)
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


@flask_app.route('/api/v1alpha/pathologySlides', methods=['GET'])
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def list_pathology_slides() -> flask.Response:
  """Process list_pathology_slides."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  dicom_filter = flask.request.args.get('filter', default='', type=str)
  slides_handler = pathology_slides_handler.PathologySlidesHandler()
  request = slides_pb2.ListPathologySlidesRequest(filter=dicom_filter)
  try:
    response = slides_handler.list_pathology_slides(request)
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


@flask_app.route('/api/v1alpha/operations/<string:op_id>', methods=['GET'])
@auth.validate_iap
@flask_cors.cross_origin(origins=ORIGINS_FLG, supports_credentials=True)
def get_operation(op_id: str) -> flask.Response:
  """Process get_operation."""
  user_email = _get_user_email()
  _log_request(flask.request, user_email)
  op_handler = pathology_operations_handler.PathologyOperationsHandler()
  request = operations_pb2.GetOperationRequest(name=f'operations/{op_id}')
  try:
    response = op_handler.get_operation(request)
    json_response = json_format.MessageToJson(response)
    return flask.Response(
        json_response, status=http.HTTPStatus.OK, mimetype='application/json'
    )
  except rpc_status.RpcFailureError as ex:
    return _parse_status_response(ex.status)


class GunicornApplication(BaseApplication):
  """gunicorn WSGI wrapper for the Flask server.

  Explicitly adding this wrapper in our Python code allows us to get Abseil flag
  and logging support by wrapping GunicornApplication in an absl.app.
  This would not happen if we invoked flask_app directly from the gunicorn
  command line.

  More info: https://docs.gunicorn.org/en/stable/custom.html#custom-application
  """

  def __init__(self, app: flask.Flask):
    self.application = app
    super().__init__()

  def load_config(self):
    self.cfg.set('worker_class', GUNICORN_WORKER_CLASS_FLG.value)
    self.cfg.set('workers', str(GUNICORN_WORKERS_FLG.value))
    self.cfg.set('threads', str(GUNICORN_THREADS_FLG.value))
    self.cfg.set('print_config', 'true')  # Print the configuration
    self.cfg.set('timeout', '180')  # Extend timeout
    self.cfg.set('bind', '0.0.0.0:%s' % API_PORT_FLG.value)
    self.cfg.set('accesslog', '-')
    self.cfg.set(
        'access_log_format', '%(u)s "%(r)s" %(s)s "%(f)s" "%({body}i)s"'
    )
    cloud_logging_client.info('Gunicorn configuration', self.cfg.settings)

  def load(self) -> flask.Flask:
    return self.application


def main(unused_argv):
  build_version.init_cloud_logging_build_version()
  flask_cors.CORS(
      flask_app,
      origins=ORIGINS_FLG,
      supports_credentials=True,
      resources={r'/dpas/*': {'origins': ORIGINS_FLG}},
  )
  GunicornApplication(flask_app).run()


if __name__ == '__main__':
  absl_app.run(main)
