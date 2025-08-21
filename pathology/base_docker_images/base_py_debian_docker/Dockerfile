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
# Debian 12.11-slim + Python 3.13.5
#
FROM debian:12.11-slim

# Start Python 3.13.5 install.
# docker install code from offical python 3.13.5 DOCKER image.
# https://github.com/docker-library/python/blob/3fae0a14ac171f46e47d7ce41567e40524af5bcc/3.13/slim-bookworm/Dockerfile

# ensure local python is preferred over distribution python
RUN set -eux; \
  apt-get update; \
  apt-get install -y --no-install-recommends \
    ca-certificates \
    netbase \
    tzdata \
  ; \
  rm -rf /var/lib/apt/lists/*

ENV GPG_KEY 7169605F62C751356D054A26A821E680E5FA6305
ENV PYTHON_VERSION 3.13.5
ENV PYTHON_SHA256 93e583f243454e6e9e4588ca2c2662206ad961659863277afcdb96801647d640

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
  wget -O python.tar.xz "https://www.python.org/ftp/python/3.13.5/Python-3.13.5.tar.xz"; \
  echo "$PYTHON_SHA256 *python.tar.xz" | sha256sum -c -; \
  wget -O python.tar.xz.asc "https://www.python.org/ftp/python/3.13.5/Python-3.13.5.tar.xz.asc"; \
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
    $(test "$gnuArch" != 'riscv64-linux-musl' && echo '--with-lto') \
    --with-ensurepip \
  ; \
  nproc="$(nproc)"; \
  EXTRA_CFLAGS="$(dpkg-buildflags --get CFLAGS)"; \
  LDFLAGS="$(dpkg-buildflags --get LDFLAGS)"; \
  LDFLAGS="${LDFLAGS:--Wl},--strip-all"; \
    arch="$(dpkg --print-architecture)"; arch="${arch##*-}"; \
# https://docs.python.org/3.12/howto/perf_profiling.html
# https://github.com/docker-library/python/pull/1000#issuecomment-2597021615
    case "$arch" in \
      amd64|arm64) \
        # only add "-mno-omit-leaf" on arches that support it
        # https://gcc.gnu.org/onlinedocs/gcc-14.2.0/gcc/x86-Options.html#index-momit-leaf-frame-pointer-2
        # https://gcc.gnu.org/onlinedocs/gcc-14.2.0/gcc/AArch64-Options.html#index-momit-leaf-frame-pointer
        EXTRA_CFLAGS="${EXTRA_CFLAGS:-} -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer"; \
        ;; \
      i386) \
        # don't enable frame-pointers on 32bit x86 due to performance drop.
        ;; \
      *) \
        # other arches don't support "-mno-omit-leaf"
        EXTRA_CFLAGS="${EXTRA_CFLAGS:-} -fno-omit-frame-pointer"; \
        ;; \
    esac; \
  make -j "$nproc" \
    "EXTRA_CFLAGS=${EXTRA_CFLAGS:-}" \
    "LDFLAGS=${LDFLAGS:-}" \
  ; \
# https://github.com/docker-library/python/issues/784
# prevent accidental usage of a system installed libpython of the same version
  rm python; \
  make -j "$nproc" \
    "EXTRA_CFLAGS=${EXTRA_CFLAGS:-}" \
    "LDFLAGS=${LDFLAGS:--Wl},-rpath='\$\$ORIGIN/../lib'" \
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
  export PYTHONDONTWRITEBYTECODE=1; \
  python3 --version; \
  pip3 --version

# make some useful symlinks that are expected to exist ("/usr/local/bin/python" and friends)
RUN set -eux; \
  for src in idle3 pip3 pydoc3 python3 python3-config; do \
    dst="$(echo "$src" | tr -d 3)"; \
    [ -s "/usr/local/bin/$src" ]; \
    [ ! -e "/usr/local/bin/$dst" ]; \
    ln -svT "$src" "/usr/local/bin/$dst"; \
  done
# End Python 3.13.5 install

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

# Remove test key material included by 3rd-party libraries
RUN find /usr/lib/google-cloud-sdk/platform/gsutil/third_party \
         /usr/local/lib/python3.13/test/certdata \
         /usr/src/python/Lib/test/certdata \
         -type f \( -name "*.pem" -o -name "*.key" \) -delete

ENV CLOUDSDK_CONFIG=/home/NonRootUser/.config/gcloud

USER NonRootUser