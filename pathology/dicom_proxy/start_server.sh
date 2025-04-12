#!/bin/bash
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

<<<<<<< Updated upstream
/usr/sbin/nginx -c /nginx.conf -e stderr
python3 -OO /pathology/dicom_proxy/server.pyc
=======
# /usr/sbin/nginx -c /nginx.conf -e stderr
#python3 -OO /pathology/dicom_proxy/server.pyc

python3 -OO hypercorn /pathology/dicom_proxy/server_hypercorn:flask_app --bind ${GUNICORN_BIND:"0.0.0:8080"} --workers ${GUNICORN_WORKERS:4} --keep-alive ${KEEP_ALIVE:-3600}
>>>>>>> Stashed changes
