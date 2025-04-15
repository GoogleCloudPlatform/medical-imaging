"""Flask REST server for Tile-server using Hypercorn."""
#from multiprocessing import current_process
#print(f"[DEBUG] Parent process is daemon: {current_process().daemon}")

import http
import time
import flask
import flask_cors
from werkzeug import routing
from werkzeug.middleware import proxy_fix

from pathology.dicom_proxy import dicom_proxy_blueprint
from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import logging_util
from pathology.dicom_proxy import redis_cache
from pathology.shared_libs.build_version import build_version
from pathology.shared_libs.logging_lib import cloud_logging_client

from asgiref.wsgi import WsgiToAsgi

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
flask_app.config['COMPRESS_REGISTER'] = False # disable default compression of all eligible requests
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

#flask_app.wsgi_app = proxy_fix.ProxyFix(
#    flask_app.wsgi_app,
#    x_for=1,
#    x_proto=1,
#    x_host=1,
#    x_port=0,
#    x_prefix=1,
#)

flask_app.register_blueprint(dicom_proxy_blueprint.dicom_proxy)


@flask_app.before_request
def handle_preflight_methods() -> flask.Response:
    cloud_logging_client.debug(f"[{flask.request.method}] {flask.request.path}")
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

    http_version = flask.request.environ.get("SERVER_PROTOCOL")  # e.g. "HTTP/1.1" or "HTTP/2"
    response = flask.make_response(f"{HEALTH_CHECK_HTML}: {http_version}", http.HTTPStatus.OK)
    response.mimetype = DEFAULT_TEXT_MIMETYPE
    return response


def _url_map_to_dict(url_map: routing.Map) -> dict[str, str]:
    return {str(rule.endpoint): str(rule.rule) for rule in url_map.iter_rules()}

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


# Wrap it for ASGI
asgi_app = WsgiToAsgi(flask_app)