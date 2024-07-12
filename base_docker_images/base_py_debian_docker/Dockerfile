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
# Base Debian Docker container for wsi image ingestion
#
# Debian 12.6-slim + Python 3.12.4
#
FROM debian:12.6-slim

# Start Python 3.12.4 install.
# docker install code from offical python 3.12.4 DOCKER image.
# https://github.com/docker-library/python/blob/b968d488efc09fd34672fc6238182e1222ae005e/3.12/slim-bookworm/Dockerfile

# ensure local python is preferred over distribution python
ENV PATH /usr/local/bin:$PATH

# cannot remove LANG even though https://bugs.python.org/issue19846 is fixed
# last attempted removal of LANG broke many users:
# https://github.com/docker-library/python/pull/570
ENV LANG C.UTF-8

# runtime dependencies
RUN set -eux; \
  apt-get update; \
  apt-get install -y --no-install-recommends \
    ca-certificates \
    netbase \
    tzdata \
  ; \
  rm -rf /var/lib/apt/lists/*

ENV GPG_KEY 7169605F62C751356D054A26A821E680E5FA6305
ENV PYTHON_VERSION 3.12.4

RUN set -eux; \
  \
  savedAptMark="$(apt-mark showmanual)"; \
  apt-get update; \
  apt-get install -y --no-install-recommends \
    dpkg-dev \
    gcc \
    gnupg \
    libbluetooth-dev \
    libbz2-dev \
    libc6-dev \
    libdb-dev \
    libexpat1-dev \
    libffi-dev \
    libgdbm-dev \
    liblzma-dev \
    libncursesw5-dev \
    libreadline-dev \
    libsqlite3-dev \
    libssl-dev \
    make \
    tk-dev \
    uuid-dev \
    wget \
    xz-utils \
    zlib1g-dev \
  ; \
  \
  wget -O python.tar.xz "https://www.python.org/ftp/python/3.12.4/Python-3.12.4.tar.xz"; \
  wget -O python.tar.xz.asc "https://www.python.org/ftp/python/3.12.4/Python-3.12.4.tar.xz.asc"; \
  GNUPGHOME="$(mktemp -d)"; export GNUPGHOME; \
  gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys "$GPG_KEY"; \
  gpg --batch --verify python.tar.xz.asc python.tar.xz; \
  gpgconf --kill all; \
  rm -rf "$GNUPGHOME" python.tar.xz.asc; \
  mkdir -p /usr/src/python; \
  tar --extract --directory /usr/src/python --strip-components=1 --file python.tar.xz; \
  rm python.tar.xz; \
  \
  cd /usr/src/python; \
  gnuArch="$(dpkg-architecture --query DEB_BUILD_GNU_TYPE)"; \
  ./configure \
    --build="$gnuArch" \
    --enable-loadable-sqlite-extensions \
    --enable-optimizations \
    --enable-option-checking=fatal \
    --enable-shared \
    --with-lto \
    --with-system-expat \
    --without-ensurepip \
  ; \
  nproc="$(nproc)"; \
  EXTRA_CFLAGS="$(dpkg-buildflags --get CFLAGS)"; \
  LDFLAGS="$(dpkg-buildflags --get LDFLAGS)"; \
  LDFLAGS="${LDFLAGS:--Wl},--strip-all"; \
  make -j "$nproc" \
    "EXTRA_CFLAGS=${EXTRA_CFLAGS:-}" \
    "LDFLAGS=${LDFLAGS:-}" \
    "PROFILE_TASK=${PROFILE_TASK:-}" \
  ; \
# https://github.com/docker-library/python/issues/784
# prevent accidental usage of a system installed libpython of the same version
  rm python; \
  make -j "$nproc" \
    "EXTRA_CFLAGS=${EXTRA_CFLAGS:-}" \
    "LDFLAGS=${LDFLAGS:--Wl},-rpath='\$\$ORIGIN/../lib'" \
    "PROFILE_TASK=${PROFILE_TASK:-}" \
    python \
  ; \
  make install; \
  ldconfig; \
  \
  apt-mark auto '.*' > /dev/null; \
  apt-mark manual $savedAptMark; \
  find /usr/local -type f -executable -not \( -name '*tkinter*' \) -exec ldd '{}' ';' \
    | awk '/=>/ { so = $(NF-1); if (index(so, "/usr/local/") == 1) { next }; gsub("^/(usr/)?", "", so); printf "*%s\n", so }' \
    | sort -u \
    | xargs -r dpkg-query --search \
    | cut -d: -f1 \
    | sort -u \
    | xargs -r apt-mark manual \
  ; \
  apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false; \
  rm -rf /var/lib/apt/lists/*; \
  \
  python3 --version

# make some useful symlinks that are expected to exist ("/usr/local/bin/python" and friends)
RUN set -eux; \
  for src in idle3 pydoc3 python3 python3-config; do \
    dst="$(echo "$src" | tr -d 3)"; \
    [ -s "/usr/local/bin/$src" ]; \
    [ ! -e "/usr/local/bin/$dst" ]; \
    ln -svT "$src" "/usr/local/bin/$dst"; \
  done

# if this is called "PIP_VERSION", pip explodes with "ValueError: invalid truth value '<VERSION>'"
ENV PYTHON_PIP_VERSION 24.0
# https://github.com/pypa/get-pip
ENV PYTHON_GET_PIP_URL https://github.com/pypa/get-pip/raw/dbf0c85f76fb6e1ab42aa672ffca6f0a675d9ee4/public/get-pip.py
ENV PYTHON_GET_PIP_SHA256 dfe9fd5c28dc98b5ac17979a953ea550cec37ae1b47a5116007395bfacff2ab9

RUN set -eux; \
  \
  savedAptMark="$(apt-mark showmanual)"; \
  apt-get update; \
  apt-get install -y --no-install-recommends wget; \
  \
  wget -O get-pip.py "$PYTHON_GET_PIP_URL"; \
  echo "$PYTHON_GET_PIP_SHA256 *get-pip.py" | sha256sum -c -; \
  \
  apt-mark auto '.*' > /dev/null; \
  [ -z "$savedAptMark" ] || apt-mark manual $savedAptMark > /dev/null; \
  apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false; \
  rm -rf /var/lib/apt/lists/*; \
  \
  export PYTHONDONTWRITEBYTECODE=1; \
  \
  python get-pip.py \
    --disable-pip-version-check \
    --no-cache-dir \
    --no-compile \
    "pip==$PYTHON_PIP_VERSION" \
  ; \
  rm -f get-pip.py; \
  \
  pip --version

# End Python 3.12.4 install

# Add gcloud to base container
# Add public key for gcloud
# https://randomadult.com/install-google-cloud-cli-on-debian-12/
COPY ./requirements.txt /requirements.txt
RUN apt-get update && \
    apt-get install -y apt-utils && \
    apt-get install -y curl apt-transport-https ca-certificates gnupg && \
    echo "deb https://packages.cloud.google.com/apt cloud-sdk main" | \
      tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
      tee /usr/share/keyrings/cloud.google.gpg && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
      gpg --dearmor | tee /etc/apt/trusted.gpg.d/cloud.google.gpg && \
    apt-get update -y && apt-get install -y google-cloud-cli && \
    # Mark this package from upgrades later - since we remove vulnerable
    # binaries below we do not want them reinstalled in an upgrade.
    apt-mark hold google-cloud-cli && \
    groupadd -r NonRootUserGroup && useradd -r -m -g NonRootUserGroup NonRootUser && \
    runuser -l NonRootUser -c "gcloud config set disable_file_logging true" && \
    # Patch gcloud packaged python 3.11.8 distribution
    cd /usr/lib/google-cloud-sdk/platform/bundledpythonunix && \
    ./bin/pip3 install --upgrade -r /requirements.txt --require-hashes && \
    rm /requirements.txt

# Remove some of the above installed packages to reduce vulnerabilities.
RUN apt-get purge -y curl apt-utils && apt-get -y autoremove && \
# Pip reports (albeit disputed) https://nvd.nist.gov/vuln/detail/CVE-2018-20225
# Remove these from google-cloud-sdk - Pip is installed above for general use.
  rm -rf /usr/lib/google-cloud-sdk/platform/bundledpythonunix/lib/python3.9/ensurepip \
  /usr/lib/google-cloud-sdk/platform/bundledpythonunix/lib/python3.9/site-packages/pip-21.1.3-py3.9.egg \
  /usr/lib/google-cloud-sdk/platform/bundledpythonunix/bin/pip \
  /usr/lib/google-cloud-sdk/bin/gcloud-crc32c \
  /usr/lib/google-cloud-sdk/bin/anthoscli

ENV CLOUDSDK_CONFIG=/home/NonRootUser/.config/gcloud