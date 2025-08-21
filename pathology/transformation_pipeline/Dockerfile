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
# Docker container for wsi image ingestion
#
# Build from CLI:
#
# from pathology_cloud working dir run: ./transformation_pipeline/deploy_ingest.sh
#
ARG BASE_CONTAINER
FROM ${BASE_CONTAINER}

USER root
WORKDIR /

# Add additional ICC Profiles to the container here.
ENV THIRD_PARTY_ICC_PROFILE_DIRECTORY='/icc_profiles'
RUN apt-get -qq update && apt-get install -y --no-install-recommends wget && \
    mkdir /icc_profiles
# To add additional ICC Profiles to the container, add place them in the
# /icc_profiles directory

# Download ndpi test file if it appears to a git lfs pointer.
COPY ./pathology/transformation_pipeline/testdata/ndpi_test.ndpi /ndpi_test.ndpi
RUN file_size=$(wc -c <"/ndpi_test.ndpi") && \
    if [ $file_size -le 100000 ]; then wget https://openslide.cs.cmu.edu/download/openslide-testdata/Hamamatsu/CMU-1.ndpi -O /ndpi_test.ndpi ; fi && \
    apt-get -qq purge -y wget

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    make \
    cmake \
    build-essential \
    libglib2.0-dev \
    libgtest-dev \
    libjpeg-dev \
    libxml2-dev \
    libcairo2-dev \
    libsqlite3-dev \
    liblzma-dev \
    meson # For building gdk-pixbuf

# Build ZXing Barcode reader
COPY ./pathology/transformation_pipeline/testdata/barcode_wikipedia_pdf417.png /wikipedia_pdf417.png
RUN set -e && \
    git clone https://github.com/zxing/zxing.git && \
    cd zxing && \
    # Sync to the checkpoint has cpp version
    git checkout 00f634024ceeee591f54e6984ea7dd666fab22ae && \
    # Remove unused source
    rm -rf /zxing/android \
          /zxing/android-integration \
          /zxing/androidtest \
          /zxing/core \
          /zxing/csharp \
          /zxing/iphone \
          /zxing/javase \
          /zxing/jruby \
          /zxing/objc \
          /zxing/qt \
          /zxing/zxingorg && \
    # Build
    mkdir cpp/build && \
    cd cpp/build && \
    cmake -G "Unix Makefiles" ..  && \
    make && \
    find /zxing -type f -name "*.o" -exec rm -f {} \;  && \
    # Test build
    ./zxing /wikipedia_pdf417.png  && \
    # Remove test barcode image
    rm /wikipedia_pdf417.png

# Build gdk-pixbuf from source. gdk-pixbuf depends on libtiff5 which reports
# https://nvd.nist.gov/vuln/detail/CVE-2022-3970 and is built from source in
# base_py_opencv_docker.
RUN ldconfig && git clone https://github.com/GNOME/gdk-pixbuf.git && \
    cd gdk-pixbuf && \
    # Pin to version 2.42.12 (last known version as of 5/14/2024)
    # https://github.com/GNOME/gdk-pixbuf/commit/e4315fb8553776e13d39e3f2e0ea8792db61720c
    git checkout e4315fb8553776e13d39e3f2e0ea8792db61720c && \
    meson setup _build . -Dman=false && \
    meson compile -C _build && \
    # Avoid "cannot open shared object file" errors, from
    # https://stackoverflow.com/questions/480764
    ldconfig && \
    meson install -C _build && \
    ldconfig && \
    cd .. && \
    rm -rf gdk-pixbuf

# Build wsi-to-dicom-converter
RUN set -e && \
    # Clone wsi-to-dicom-converter
    git clone https://github.com/GoogleCloudPlatform/wsi-to-dicom-converter && \
    cd wsi-to-dicom-converter && \
    # Sync to change submitted to head on 1/19/2025: Updates deprecated dcmtk function and updates linter
    git reset --hard 0076ded3f0b6bd6d6f34199394ebd92533883359 && \
    cp /usr/lib/x86_64-linux-gnu/glib-2.0/include/glibconfig.h /usr/include/glib-2.0/glibconfig.h && \
    mkdir build && \
    cd build && \
    # Remove un-needed files.
    set +e && \
    find /wsi-to-dicom-converter -path "*/doc/html" -type d -exec rm -rf {} \; && \
    find /wsi-to-dicom-converter -path "*/doc/*" -type f -name "*.js" -exec rm -f {} \; && \
    set -e && \
    # Build
    cmake -DSTATIC_BUILD=ON -DTESTS_BUILD=ON \
      -DJPEG_LIBRARY=/opt/libjpeg-turbo/lib64/libjpeg.a \
      -DJPEG_INCLUDE_DIR=/opt/libjpeg-turbo/include ..  && \
    make -j12 && \
    # Run Unit Tests
    ./gTests && \
    cd .. && \
    # Run EndToEnd Tests
    # ./endToEnd/endToEndTest.sh SKIP_BUILD_ENVIRONMENT && \
    rm ./cloud_build/debianBuild.sh && \
    touch ./cloud_build/debianBuild.sh && \
    chmod +x ./cloud_build/debianBuild.sh && \
    rm -rf /wsi-to-dicom-converter/src \
       /wsi-to-dicom-converter/tests \
       /wsi-to-dicom-converter/endToEnd \
       /wsi-to-dicom-converter/CMakeLists.txt \
       /wsi-to-dicom-converter/build/*.dcm \
       /wsi-to-dicom-converter/build/CMakeCache.txt \
       /wsi-to-dicom-converter/build/cmake_install.cmake \
       /wsi-to-dicom-converter/CMake \
       /wsi-to-dicom-converter/cloud_build \
       /wsi-to-dicom-converter/patches \
       /wsi-to-dicom-converter/build/CMakeTmp \
       /wsi-to-dicom-converter/build/CMakeFiles && \
    find /wsi-to-dicom-converter -type f -name "*.o" -exec rm -f {} \; && \
    # Remove files that should not be in the container
    find /wsi-to-dicom-converter \
      \( -name "*.md" -o -name "*.sh" -o -name "BUILD" -o \
         -name "OWNERS" -o -name "Dockerfile" -o -name "METADATA" \
      \) -type f -delete

RUN apt-get update && \
    apt-get install -y libglu1-mesa-dev \
                       libpng16-16 && \
    apt-mark manual libcairo2 \
                    libcairo2-dev \
                    libcairo-gobject2 \
                    libcairo-script-interpreter2 \
                    libjpeg62-turbo \
                    libjpeg62-turbo-dev \
                    libjsoncpp25 && \
# Patch security issues in dbus (10-7-2022)
# https://security-tracker.debian.org/tracker/CVE-2022-42010
# https://security-tracker.debian.org/tracker/CVE-2022-42011
# https://security-tracker.debian.org/tracker/CVE-2022-42012
    apt-get upgrade -y dbus && \
    rm -rf /usr/lib/google-cloud-sdk/bin/anthoscli \
           /usr/lib/google-cloud-sdk/bin/gcloud-crc32c

# Setup python enviroment
COPY ./pathology/transformation_pipeline/requirements.txt \
    ./pathology/shared_libs/docker_setup/setuptools_requirements.txt /
# /requirements.txt installed without dependencies to avoid installing
# EZ-WSI opencv headless which conflicts with the build environment built
# opencv.
RUN python3 -m pip install -r setuptools_requirements.txt --require-hashes && \
    python3 -m pip install -r /requirements.txt --require-hashes --no-deps && \
    python3 -m pip uninstall -y pip && \
    apt-get purge -y \
      libldap-common \
      libglib2.0-dev \
      libjpeg-dev \
      git \
      wget \
      make \
      cmake \
      build-essential \
      gcc && \
    apt-get autoremove -y && \
    rm /requirements.txt \
       /setuptools_requirements.txt \
       /usr/bin/apt \
       /usr/bin/apt-get

# Copy licenses
COPY ./LICENSE /pathology/license
COPY ./LICENSE /license
COPY ./pathology/transformation_pipeline/ThirdParty_LICENSE.docker /thirdparty_licenses.txt

# Copy Bazel build requirements
COPY ./WORKSPACE ./MODULE.bazel /
COPY ./pathology/bazel_requirements.txt ./pathology/__init__.py /pathology/

# Copy Python source
COPY ./pathology/transformation_pipeline /pathology/transformation_pipeline/
COPY ./pathology/shared_libs /pathology/shared_libs/

# Make python directory __init__ files for copied directories.
RUN mv /pathology/transformation_pipeline/default_config.* / && \
    mv /ndpi_test.ndpi /pathology/transformation_pipeline/testdata/ndpi_test.ndpi && \
    python3 -m compileall -b /pathology && \
    find /pathology -type d -exec chmod 755 {} \; && \
    find /pathology -type f -exec chmod 644 {} \; && \
    # Make icc profiles copied in to icc profiles dir folder readable.
    find /icc_profiles -type d -exec chmod 755 {} \; && \
    find /icc_profiles -type f -exec chmod 644 {} \; && \
    chmod 644 /thirdparty_licenses.txt \
              /default_config.yaml \
              /default_config.json \
              /license
ENV PYTHONPATH="${PYTHONPATH}:/"

# Run unit tests on container.
RUN set -e && \
    python3 -m unittest discover -p "*_test.py" -s "/pathology/transformation_pipeline" -t / && \
    python3 -m unittest discover -p "*_test.py" -s "/pathology/shared_libs" -t / && \
    set +e && \
    rm -r /tmp/*

# Set container to run as non-root user.
# NonRootUser configured in dpas-debian-python-base image.
USER NonRootUser

ENTRYPOINT ["python3", "-OO", "/pathology/transformation_pipeline/gke_main.py"]
