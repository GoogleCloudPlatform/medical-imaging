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
"""Flask REST server for Tile-server."""

import http
import time

from absl import app as absl_app
import flask
import flask_cors
from gunicorn.app.base import BaseApplication
from werkzeug import routing
from werkzeug.middleware import proxy_fix

from pathology.dicom_proxy import dicom_proxy_blueprint
from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import logging_util
from pathology.dicom_proxy import redis_cache
from pathology.shared_libs.build_version import build_version
from pathology.shared_libs.logging_lib import cloud_logging_client


DEFAULT_TEXT_MIMETYPE = 'text/html; charset=utf-8'
HEALTH_CHECK_HTML = 'Digital_Pathology_Proxy_Server-Blueprint-Health-Check'
LOCAL_REDIS_SERVER_DISABLED = 'Error connecting to local Redis instance.'
REMOTE_REDIS_SERVER_DISABLED = 'Error connecting to remote Redis instance.'

_last_health_check_log_time = 0.0
_redis_server_is_alive_healthcheck = True
flask_app = flask.Flask(__name__)
# Set flask maximum content length to 21 GB. Value slightly greater than
# DICOM stores max instance file size to block malicious uploads.
flask_app.config['MAX_CONTENT_LENGTH'] = 21 * (2**30)
flask_app.config['COMPRESS_REGISTER'] = (
    False  # disable default compression of all eligible requests
)
flask_app.config['COMPRESS_MIMETYPES'] = [
    'application/javascript',
    'application/json',
    'application/dicom+json',
    'text/css',
    'text/html',
    'text/javascript',
    'text/xml',
]
dicom_proxy_blueprint.compress.init_app(flask_app)

# Enable flask to correctly redirect requests based on NGINX set proxy header
# settings (see nginx.conf); e.g. proxy_set_header X-Forwarded-Proto. Enables
# proxy to re-create url from the perspective of the caller and not the proxy
# for re-direction purposes.
flask_app.wsgi_app = proxy_fix.ProxyFix(
    flask_app.wsgi_app,
    x_for=1,
    x_proto=1,
    x_host=1,
    x_port=0,
    x_prefix=1,
)

flask_app.register_blueprint(dicom_proxy_blueprint.dicom_proxy)


@flask_app.before_request
def handle_preflight_methods() -> flask.Response:
  if flask.request.method.lower() == 'options':
    return flask.Response(
        status=http.HTTPStatus.OK,
        headers={
            'Access-Control-Allow-Origin': dicom_proxy_flags.ORIGINS_FLG.value,
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Methods': '*',
            'Access-Control-Allow-Credentials': 'true',
        },
    )


@flask_app.route('/', methods=['GET', 'POST'], endpoint='healthcheck')
@logging_util.log_exceptions_in_health_check
def healthcheck() -> flask.Response:
  """Returns checks redis cache is running and returns response.

  Requested DICOM instance frame images (flask.Response)
  """
  current_time = time.time()
  global _last_health_check_log_time
  global _redis_server_is_alive_healthcheck
  delta_time = current_time - _last_health_check_log_time
  if delta_time >= dicom_proxy_flags.HEALTH_CHECK_LOG_INTERVAL_FLG.value:
    _last_health_check_log_time = current_time
    cache = redis_cache.RedisCache()
    # Only check if redis server is running if tile-server using  local.
    # If using non-local host store the redis cache should health checked
    # directly.
    if cache.is_localhost:
      _redis_server_is_alive_healthcheck = cache.ping()
    elif not cache.ping():
      cloud_logging_client.critical(REMOTE_REDIS_SERVER_DISABLED)

  if not _redis_server_is_alive_healthcheck:
    cloud_logging_client.critical(LOCAL_REDIS_SERVER_DISABLED)
    response = flask.make_response(
        LOCAL_REDIS_SERVER_DISABLED, http.HTTPStatus.BAD_REQUEST
    )
    response.mimetype = DEFAULT_TEXT_MIMETYPE
    return response

  response = flask.make_response(HEALTH_CHECK_HTML, http.HTTPStatus.OK)
  response.mimetype = DEFAULT_TEXT_MIMETYPE
  return response


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
    self.cfg.set('worker_class', 'gthread')
    self.cfg.set('workers', str(dicom_proxy_flags.GUNICORN_WORKERS_FLG.value))
    self.cfg.set('threads', str(dicom_proxy_flags.GUNICORN_THREADS_FLG.value))
    self.cfg.set('bind', 'unix:/tmp/gunicorn.sock')
    self.cfg.set('accesslog', '-')
    self.cfg.set(
        'access_log_format', '%(u)s "%(r)s" %(s)s "%(f)s" "%({body}i)s"'
    )
    cloud_logging_client.info('Gunicorn configuration', self.cfg.settings)

  def load(self) -> flask.Flask:
    return self.application


def _url_map_to_dict(url_map: routing.Map) -> dict[str, str]:
  return {str(rule.endpoint): str(rule.rule) for rule in url_map.iter_rules()}


def main(unused_argv):
  build_version.init_cloud_logging_build_version()
  redis_cache.setup()
  cloud_logging_client.debug('Tile-server process started.')
  flask_cors.CORS(
      flask_app,
      origins=dicom_proxy_flags.ORIGINS_FLG.value,
      supports_credentials=True,
  )
  cloud_logging_client.info(
      'Flask Route Url Map', _url_map_to_dict(flask_app.url_map)
  )
  GunicornApplication(flask_app).run()


if __name__ == '__main__':
  try:
    absl_app.run(main)
  except Exception as exp:
    cloud_logging_client.critical('Exception raised in tile-server', exp)
    raise
