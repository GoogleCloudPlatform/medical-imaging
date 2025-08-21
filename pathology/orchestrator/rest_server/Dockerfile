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

# The context must be set to the parent directory of the Dockerfile so files in
# other orchestrator/ subdirectories can be used.
# E.g. docker build ../ --file=./Dockerfile
ARG BASE_CONTAINER
FROM ${BASE_CONTAINER}

USER root
ENV PYTHONPATH "${PYTHONPATH}:/"
# Spanner needs this. Set to actual project.
# ENV GOOGLE_CLOUD_PROJECT="dpas-digipat"

COPY pathology/orchestrator/rest_server/requirements.txt pathology/shared_libs/docker_setup/setuptools_requirements.txt  /
# Run create_requirements_txt.sh to build requirements.txt from requirements.in
RUN python3 -m pip install -r setuptools_requirements.txt --require-hashes && \
  python3 -m pip install --upgrade -r requirements.txt --require-hashes && \
  python3 -m pip uninstall -y pip && \
  apt-get remove -y libsqlite3-0 && \
  apt-get -y autoremove --purge zlib1g-dev && \
  apt autoremove -y && \
  rm requirements.txt setuptools_requirements.txt

COPY ./pathology/__init__.py /pathology/
COPY pathology/orchestrator/*.py pathology/orchestrator/*.txt /pathology/orchestrator/
COPY pathology/orchestrator/test_data/ /pathology/orchestrator/test_data/
COPY pathology/orchestrator/rest_server/ /pathology/orchestrator/rest_server/
COPY pathology/orchestrator/refresher/ /pathology/orchestrator/refresher/
COPY pathology/orchestrator/spanner/ /pathology/orchestrator/spanner/
COPY pathology/orchestrator/v1alpha/ /pathology/orchestrator/v1alpha/
COPY pathology/shared_libs/ /pathology/shared_libs/
COPY pathology/transformation_pipeline/ingestion_lib/dicom_util /pathology/transformation_pipeline/ingestion_lib/dicom_util

# Remove files that should not be in the container
RUN find /pathology/orchestrator \( -name "*.sh" \
                                    -o -name "Dockerfile" \
                                 \) -type f -delete && \
    # Add compile python modules
    python3 -m compileall -b /pathology && \
    find /pathology -type d -exec chmod 755 {} \; && \
    find /pathology -type f -exec chmod 644 {} \;

RUN USER_SITE=$(python3 -m site --user-site) && \
  SITE_PKG_DIR=$(echo $USER_SITE | sed 's:/root/.:/usr/:g') && \
  python3 -m grpc_tools.protoc \
    --proto_path=. \
    --proto_path=./pathology/orchestrator/v1alpha \
    --proto_path=./pathology/orchestrator/spanner \
    --proto_path=${SITE_PKG_DIR} \
    --python_out=. \
    --grpc_python_out=. \
    ./pathology/orchestrator/v1alpha/cohorts.proto \
    ./pathology/orchestrator/v1alpha/digital_pathology.proto \
    ./pathology/orchestrator/v1alpha/sharing_links.proto \
    ./pathology/orchestrator/v1alpha/slides.proto \
    ./pathology/orchestrator/v1alpha/users.proto \
    ./pathology/orchestrator/spanner/spanner_resources.proto

# Run unit tests on container.
RUN set -e && \
    python3 -m unittest discover -p "*_test.py" -s "/pathology/orchestrator" -t / && \
    set +e

# Set container to run as non-root user.
# NonRootUser configured in dpas-debian-python-base image.
USER NonRootUser

ENTRYPOINT ["python3", "/pathology/orchestrator/rest_server/server.py"]
