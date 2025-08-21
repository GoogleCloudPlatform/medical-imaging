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
# The context must be set to the parent directory of the Dockerfile so files in
# other orchestrator/ subdirectories can be used.
# E.g. docker build ../ --file=./Dockerfile
ARG BASE_CONTAINER
FROM ${BASE_CONTAINER}

USER root
WORKDIR /
# Add additional ICC Profiles to the container here.
ENV THIRD_PARTY_ICC_PROFILE_DIRECTORY='/icc_profiles'
# copy adobe rgb icc profile into 3rd party icc profile directory
RUN mkdir /icc_profiles
# To add additional ICC Profiles to the container, add place them in the
# /icc_profiles directory
# Note: During build the following warning maybe raised:
# WARNING: Ignoring invalid distribution -wh-pip
# (/usr/local/lib/python3.9/site-packages)
# this is a benign warning. Caused by pip detecting docker whiteout files
# exposed within /usr/local/lib/python3.9/site-packages that were produced
# as a result of updating pip in prior docker layers. Whiteout files
# mark the deletion of files or directories. To remove this warning the base
# python containers need to be rebuilt with the most up-to date versions of pip.
COPY ./pathology/dicom_proxy/requirements.txt \
     ./pathology/shared_libs/docker_setup/setuptools_requirements.txt /
# Copy licenses
COPY ./LICENSE /pathology/license
COPY ./LICENSE /license
# Copy Bazel build requirements
COPY ./WORKSPACE ./MODULE.bazel /
COPY ./pathology/bazel_requirements.txt ./pathology/__init__.py /pathology/
# Copy redis config and python source
COPY ./pathology/dicom_proxy/redis.conf /
COPY ./pathology/dicom_proxy /pathology/dicom_proxy/
COPY ./pathology/shared_libs /pathology/shared_libs/
# required to run echo -e
SHELL ["/bin/bash", "-c"]
RUN apt-get update && \
    apt-get install -y --no-install-recommends redis stunnel4 && \
# Install nginx mainline server
    apt-get install -y --no-install-recommends curl gnupg2 ca-certificates lsb-release debian-archive-keyring libreadline8 libterm-readline-gnu-perl && \
    curl https://nginx.org/keys/nginx_signing.key | gpg --dearmor | tee /usr/share/keyrings/nginx-archive-keyring.gpg >/dev/null && \
    echo "deb [signed-by=/usr/share/keyrings/nginx-archive-keyring.gpg] http://nginx.org/packages/mainline/debian `lsb_release -cs` nginx" | tee /etc/apt/sources.list.d/nginx.list && \
    echo -e "Package: *\nPin: origin nginx.org\nPin: release o=nginx\nPin-Priority: 900\n" | tee /etc/apt/preferences.d/99nginx && \
    apt update && \
    apt-get install -y --no-install-recommends nginx && \
    rm /etc/nginx/nginx.conf && \
    apt-get purge -y curl && \
# /requirements.txt installed without dependencies to avoid installing
# EZ-WSI opencv headless which conflicts with the build environment built
# opencv.
# Install python
    python3 -m pip install -r setuptools_requirements.txt --require-hashes && \
    python3 -m pip install --upgrade -r requirements.txt --require-hashes --no-deps && \
    python3 -m pip uninstall -y pip && \
    rm /requirements.txt \
       /setuptools_requirements.txt && \
    update-ca-certificates && \
    apt-get purge -y \
      libldap-2.4-2 \
      libldap-common \
      make \
      build-essential \
      nasm \
      libsqlite3-0 && \
    apt autoremove -y && \
    rm /usr/bin/apt /usr/bin/apt-get && \
    mv /pathology/dicom_proxy/start_server.sh /start_server.sh && \
# Compile python in root folder and server.py
   python3 -m compileall -b /pathology && \
   find /pathology -type d -exec chmod 755 {} \; && \
   find /pathology -type f -exec chmod 644 {} \; && \
   chmod 644 /redis.conf && \
# Make icc profiles copied in to icc profiles dir folder readable.
   find /icc_profiles -type d -exec chmod 755 {} \; && \
   find /icc_profiles -type f -exec chmod 644 {} \; && \
# Setup nginx
   mv /pathology/dicom_proxy/nginx.conf /nginx.conf && \
# Setup server start script
   chmod 755 /start_server.sh && \
   chmod +x /start_server.sh
ENV PYTHONPATH="${PYTHONPATH}:/"
# Run unit tests on container.
RUN set -e && \
    python3 -m unittest discover -p "*_test.py" -s "/pathology/dicom_proxy" -t / && \
    python3 -m unittest discover -p "*_test.py" -s "/pathology/shared_libs" -t / && \
    set +e && \
    rm -r /tmp/*
# Set container to run as non-root user.
# NonRootUser configured in dpas-debian-python-base image.
USER NonRootUser
ENTRYPOINT ["/start_server.sh"]