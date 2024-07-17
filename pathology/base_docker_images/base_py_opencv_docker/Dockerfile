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
FROM ${BASE_CONTAINER}

RUN apt-get update && \
    apt-get install -y \
      git \
      apt-utils \
      wget \
      build-essential \
      cmake \
      zlib1g-dev \
      unzip && \
    apt-get upgrade -y gnutls-bin

RUN rm -rf /usr/lib/google-cloud-sdk/bin/anthoscli \
           /usr/lib/google-cloud-sdk/bin/gcloud-crc32c

WORKDIR /

#Install LibJpegTurbo 3.0.3
RUN apt-get install -y nasm && \
    wget -O libjpeg_turbo.zip https://github.com/libjpeg-turbo/libjpeg-turbo/archive/refs/tags/3.0.3.zip > /dev/null && \
    unzip libjpeg_turbo.zip > /dev/null && \
    rm libjpeg_turbo.zip && \
    mv libjpeg-turbo-3.0.3 libjpeg-turbo && \
    mkdir -p /libjpeg-turbo/build && \
    cd /libjpeg-turbo/build && \
    cmake -G"Unix Makefiles" -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DBUILD_SHARED_LIBS:bool=on .. && \
    make -j12 && \
    make install && \
    rm -rf /libjpeg-turbo

#Install OpenJPEG 2.5.2
RUN apt-get install -y liblcms2-dev libzstd-dev libwebp-dev && \
    wget -O v2.5.2.zip  https://github.com/uclouvain/openjpeg/archive/v2.5.2.zip > /dev/null && \
    unzip v2.5.2.zip > /dev/null && \
    rm v2.5.2.zip && \
    mkdir -p /openjpeg-2.5.2/build && \
    cd /openjpeg-2.5.2/build && \
    cmake  -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS:bool=on -DCMAKE_INSTALL_PREFIX="/usr" .. && \
    make -j12 && \
    make install && \
    make clean && \
    cd / && \
    rm -rf /openjpeg-2.5.2

# Install libtiff from source with JPEG support
# http://www.simplesystems.org/libtiff/build.html
# Last release of libtiff has multiple reported CVEs and libtiff does not do
# patch releases
# (see https://gitlab.com/libtiff/libtiff/-/issues/483#note_1189949359).
# Specifically, to address https://nvd.nist.gov/vuln/detail/CVE-2022-3970
# fixed in https://gitlab.com/libtiff/libtiff/-/commit/227500897dfb07fb7d27f7aa570050e62617e3be
RUN git clone https://gitlab.com/libtiff/libtiff.git && \
  cd libtiff && \
  # Last known commit with green pipeline as of 6/2023.  Version 4.6.0
  # https://gitlab.com/libtiff/libtiff/-/commit/8b20804fc0ddeaa93667b799b5e1a2a7dc9e3fb2
  git checkout 8b20804fc0ddeaa93667b799b5e1a2a7dc9e3fb2 && \
  cmake -Djpeg=ON -DJPEG_INCLUDE_DIR=/opt/libjpeg-turbo/include \
    -DJPEG_LIBRARY=/opt/libjpeg-turbo/lib64/libjpeg.a \
    # Blank DZSTD_LIBRARY= avoids linker errors later
    # (undefined reference to `ZSTD_*). Config here:
    # https://gitlab.com/libtiff/libtiff/-/blob/master/cmake/FindZSTD.cmake
    -Dzstd=ON -DZSTD_LIBRARY= \
    -Dzlib=ON -DBUILD_SHARED_LIBS:bool=on \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON . && \
  make install

#Install OpenCV 4.10.0 (numpy required to build opencv python interface)
# OpenCV build is using above built: LibJpegTurbo & OpenJPEG for
# jpeg and JPEG2000 decoding.
COPY /opencv_build_test_requirements.txt /opencv_build_test_requirements.txt
COPY /opencv_build_requirements.txt /opencv_build_requirements.txt
COPY /opencv_build_test.py /opencv_build_test.py
RUN python3 -m pip install -r opencv_build_requirements.txt --require-hashes && \
    apt-get update && \
    apt-get install -y libglib2.0-0 && \
    wget -O opencv.zip https://github.com/opencv/opencv/archive/refs/tags/4.10.0.zip > /dev/null && \
    unzip opencv.zip > /dev/null && \
    rm opencv.zip  && \
    mv opencv-4.10.0 opencv && \
    mkdir -p /opencv/build && \
    cd /opencv/build && \
    cmake -DWITH_JPEG=ON -DBUILD_JPEG=OFF -DWITH_TIFF=ON -DBUILD_TIFF=OFF -DJPEG_LIBRARY=/opt/libjpeg-turbo/lib64/libjpeg.a -DJPEG_INCLUDE_DIR=/opt/libjpeg-turbo/include .. -DBUILD_LIST=core,imgproc,imgcodecs,python3  && \
    make -j12 && \
    make install && \
    cd / && \
    rm -rf /opencv && \
    python3 -m pip install -r opencv_build_test_requirements.txt --require-hashes && \
    python3 /opencv_build_test.py && \
    rm /opencv_build_test.py /opencv_build_requirements.txt /opencv_build_test_requirements.txt

# Removes libraries that were maintained in base image from Python build to
# support building opencv
RUN rm -rf /usr/src/python; \
	  find /usr/local -depth \
		\( \
			\( -type d -a \( -name test -o -name tests -o -name idle_test \) \) \
			-o \( -type f -a \( -name '*.pyc' -o -name '*.pyo' -o -name 'libpython*.a' \) \) \
		\) -exec rm -rf '{}' +

# Remove packages installed above
RUN apt-get purge --autoremove -y \
  git \
  apt-utils \
  wget \
  build-essential \
  cmake \
  unzip \
  zlib1g-dev
