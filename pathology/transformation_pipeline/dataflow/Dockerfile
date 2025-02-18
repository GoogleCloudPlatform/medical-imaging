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
# Custom Docker container for wsi image ingestion Dataflow launcher, see:
# https://cloud.google.com/dataflow/docs/guides/using-custom-containers and
# https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates#use_custom_container_images
ARG BASE_CONTAINER
FROM ${BASE_CONTAINER}

WORKDIR /
USER root

# Copy SDK entrypoint binary from Apache Beam image.
COPY --from=apache/beam_python3.12_sdk:2.63.0 /opt/apache/beam /opt/apache/beam

# Configure the Flex Template.
COPY --from=gcr.io/dataflow-templates-base/python311-template-launcher-base:latest /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher
COPY ./dataflow_main.py /transformation_pipeline/dataflow_main.py
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/transformation_pipeline/dataflow_main.py"

# Install Apache Beam SDK.
COPY ./requirements.txt /requirements.txt
RUN python -m ensurepip --upgrade && \
    python -m pip install -r /requirements.txt --require-hashes && \
    python -m pip uninstall -y pip

# Set the entrypoint to the Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]