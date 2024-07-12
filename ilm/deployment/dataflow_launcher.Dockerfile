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
#
# ==============================================================================
# ILM Dataflow Launcher Base Image
#
# We need to pass Beam SDK as a local file to workers due to private VPC
# requirements, so we copy from a SDK image (see go/beam-sdk-containers).
FROM gcr.io/cloud-dataflow/v1beta3/beam_python3.11_sdk:2.57.0 AS beam_sdk

# See go/beam-sdk-containers#python-sdk-images for more info about base images.
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:20240506-rc00

ARG WORKDIR=/dataflow/template

RUN apt-get update && \
    apt-get upgrade -y && \
    mkdir -p ${WORKDIR} "${WORKDIR}/ilm"

WORKDIR ${WORKDIR}
COPY ilm/deployment/requirements.txt /requirements.txt

RUN python3 -m pip install -r /requirements.txt --require-hashes && \
    rm /requirements.txt

# Security fixes
RUN rm -rf \
    # Remove go binaries from gcloud SDK that are built with older go versions.
    /usr/local/gcloud/google-cloud-sdk/bin/anthoscli \
    /usr/local/gcloud/google-cloud-sdk/bin/gcloud-crc32c \
    # Remove unused go file built with go1.18 which triggers vulnerabilities.
    /opt/google/dataflow/boot \
    /opt/apache/beam/boot \
    /usr/local/gcloud/google-cloud-sdk/platform/bundledpythonunix/lib/python3.9/site-packages/cryptography*

# Remove packages with potential vulnerabilities
RUN apt-get purge -y \
    mariadb-common \
    git \
    curl \
    libsqlite3-0 \
    libtiff-dev && \
    apt-get autoremove -y

RUN pip uninstall -y pip

COPY --from=beam_sdk /opt/apache/beam/tars/apache-beam.tar.gz apache-beam.tar.gz

COPY ilm/ .

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/batch_pipeline_main.py"
