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
# Docker container for WSI image ingestion
#
# Build from CLI:
#
# from pathology_cloud working dir run: ./transformation_pipeline/deploy_ingest.sh
#
FROM ${BASE_CONTAINER}

RUN apt-get update && \
    apt-get install -y \
      apt-utils \
      wget \
      build-essential \
      cmake \
      unzip \
      libgtest-dev \
      libxml2-dev \
      libcairo2-dev \
      libgtk-3-dev \
      sqlite3 \
      libsqlite3-dev \
      valgrind \
      libjsoncpp-dev \
      libgdk-pixbuf2.0-dev \
      libglib2.0-dev

WORKDIR /

#Install Abseil TS 20240116.2
ARG ABSEIL_VERSION="20240116.2"
RUN wget -O abseil.zip "https://github.com/abseil/abseil-cpp/archive/refs/tags/${ABSEIL_VERSION}.zip" && \
    unzip abseil.zip && \
    rm abseil.zip && \
    mkdir -p "/abseil-cpp-${ABSEIL_VERSION}/build" && \
    mkdir -p /abseil && \
    cd "/abseil-cpp-${ABSEIL_VERSION}/build" && \
    cmake ..  -DCMAKE_INSTALL_PREFIX=/abseil/install && \
    cmake  --build . --target install &&  \
    rm -rf "/abseil-cpp-${ABSEIL_VERSION}"

#Install DCMTK 3.6.8
ARG DCMTK_VERSION="3.6.8"
RUN wget -O "dcmtk-${DCMTK_VERSION}.zip" "https://github.com/DCMTK/dcmtk/archive/refs/tags/DCMTK-${DCMTK_VERSION}.zip" > /dev/null && \
    unzip "dcmtk-${DCMTK_VERSION}.zip" > /dev/null  && \
    rm "dcmtk-${DCMTK_VERSION}.zip" && \
    mkdir -p "/dcmtk-DCMTK-${DCMTK_VERSION}/build" && \
    cd "/dcmtk-DCMTK-${DCMTK_VERSION}/build"  && \
    cmake -DJPEG_LIBRARY=/opt/libjpeg-turbo/lib64/libjpeg.a -DJPEG_INCLUDE_DIR=/opt/libjpeg-turbo/include -DDCMTK_FORCE_FPIC_ON_UNIX:BOOL=TRUE -DDCMTK_ENABLE_CXX11:BOOL=TRUE -DDCMTK_ENABLE_CHARSET_CONVERSION:BOOL=FALSE -DBUILD_SHARED_LIBS:BOOL=ON -DDCMTK_MODULES:STRING="oficonv;ofstd;oflog;dcmdata;dcmimgle;dcmimage;dcmjpeg" ..  && \
    make -j12 && \
    make DESTDIR=/ install && \
    rm -rf "/dcmtk-DCMTK-${DCMTK_VERSION}"
ENV DCMDICTPATH=/usr/local/share/dcmtk-3.6.8/dicom.dic
ENV PATH=/usr/local/bin:$PATH

#Install Boost 1.85.0
ARG BOOST_VERSION="1.85.0"
ARG BOOST_UNDERSCORE="1_85_0"
RUN wget -O "boost_${BOOST_UNDERSCORE}.tar.gz" "https://boostorg.jfrog.io/artifactory/main/release/${BOOST_VERSION}/source/boost_${BOOST_UNDERSCORE}.tar.gz" > /dev/null && \
    tar xvzf "boost_${BOOST_UNDERSCORE}.tar.gz"  > /dev/null && \
    rm "boost_${BOOST_UNDERSCORE}.tar.gz" && \
    cd "boost_${BOOST_UNDERSCORE}" && \
    ./bootstrap.sh --prefix=/usr/ \
    # From https://github.com/GoogleCloudPlatform/wsi-to-dicom-converter/blob/master/CMake/Boost.cmake
      --with-libraries=system,atomic,thread,chrono,program_options,log,filesystem && \
    ./b2 && \
    ./b2 install && \
    rm -rf "/boost_${BOOST_UNDERSCORE}"

#Install Openslide 4.0.0
# OpenSlide does not use the libjpeg-turbo installed in the package, so we
# install libjpeg-dev here.
ARG OPENSLIDE_VERSION="4.0.0"
COPY /requirements.txt /requirements.txt
RUN apt-get install -y libjpeg-dev ninja-build && \
   wget -O "openslide-${OPENSLIDE_VERSION}.tar.gz" "https://github.com/openslide/openslide/archive/refs/tags/v${OPENSLIDE_VERSION}.tar.gz" > /dev/null && \
   tar xvzf "openslide-${OPENSLIDE_VERSION}.tar.gz"  > /dev/null && \
   rm "openslide-${OPENSLIDE_VERSION}.tar.gz" && \
   cd "openslide-${OPENSLIDE_VERSION}" && \
   pip3 install -r /requirements.txt --require-hashes && \
   meson setup /openslide-build && \
   meson compile -C /openslide-build && \
   meson install -C /openslide-build && \
   pip3 uninstall -y meson && \
   rm -rf "/openslide-${OPENSLIDE_VERSION}" /openslide-build && \
   rm /requirements.txt && \
   apt-get purge --autoremove -y libjpeg-dev pkg-config ninja-build
# Enable WSI2DCM and Python to find Openslide library
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:/usr/local/lib/x86_64-linux-gnu:/usr/local/lib/aarch64-linux-gnu

#Install JsonCPP 1.9.5
ARG JSONCPP_VERSION="1.9.5"
RUN wget -O "${JSONCPP_VERSION}.zip" "https://github.com/open-source-parsers/jsoncpp/archive/refs/tags/${JSONCPP_VERSION}.zip" > /dev/null && \
    unzip "${JSONCPP_VERSION}.zip"  > /dev/null && \
    rm "${JSONCPP_VERSION}.zip" && \
    mkdir -p "/jsoncpp-${JSONCPP_VERSION}/build" > /dev/null && \
    cd "/jsoncpp-${JSONCPP_VERSION}/build" && \
    cmake -DCMAKE_BUILD_TYPE=release -DBUILD_STATIC_LIBS=OFF -DBUILD_SHARED_LIBS=ON -G "Unix Makefiles" .. && \
    make -j12 && \
    make install && \
    cd / && \
    rm -rf "/jsoncpp-${JSONCPP_VERSION}"

RUN apt-get purge --autoremove -y \
      apt-utils \
      wget \
      build-essential \
      cmake \
      unzip \
      libgtest-dev \
      libxml2-dev \
      libcairo2-dev \
      libgtk-3-dev \
      sqlite3 \
      libsqlite3-dev \
      valgrind \
      libjsoncpp-dev \
      libgdk-pixbuf2.0-dev \
      libglib2.0-dev && \
    apt-get autoremove -y
