FROM ubuntu:20.04

COPY ./philips_isyntax_sdk.zip /philips_isyntax_sdk.zip

# Unzip the Philips SDK
RUN apt-get update && \
    apt-get install --no-install-recommends -y unzip && \
    unzip  /philips_isyntax_sdk.zip && \
    cd /PathologySDK_2.0-L1_Packages/Ubuntu20.04 && \
    unzip philips-pathologysdk-2.0-L1-ubuntu20_04_py38_commercial && \
    cd /PathologySDK_2.0-L1_Packages/SampleCodes && \
    unzip PythonTools && \
    mv /PathologySDK_2.0-L1_Packages/Ubuntu20.04/philips-pathologysdk-2.0-L1-ubuntu20_04_py38_commercial /philips_sdk && \
    mv /PathologySDK_2.0-L1_Packages/SampleCodes/PythonTools /pythontools && \
    rm -rf /PathologySDK_2.0-L1_Packages

# Patch the Philips SDK installer
COPY ./pathology/transformation_pipeline/isyntax_transformation/sdk_patch/InstallPathologySDK.sh /philips_sdk
# Patch the Philips SDK Python Tools
COPY ./pathology/transformation_pipeline/isyntax_transformation/sdk_patch/isyntax_to_tiff.patch /pythontools/isyntax_to_tiff.py
COPY ./pathology/transformation_pipeline/isyntax_transformation/sdk_patch/libtiff_interface.patch /pythontools/libtiff_interface.py

# RUN philips_sdk/InstallPathologySDK.sh
RUN apt-get update && \
    apt-get install --no-install-recommends -y nano && \
    apt-get install --no-install-recommends -y tzdata && \
    echo 'America/Chicago' > /etc/timezone && \
    dpkg-reconfigure -f noninteractive tzdata && \
    cd /philips_sdk && \
    chmod +x ./InstallPathologySDK.sh && \
    ./InstallPathologySDK.sh

COPY ./pathology/transformation_pipeline/isyntax_transformation/numpy_requirements.txt /numpy_requirements.txt
COPY ./pathology/transformation_pipeline/isyntax_transformation/requirements.txt /requirements.txt

# Install Python dependencies
RUN apt-get install -y libtiff5-dev pip && \
    # libtiff pip installation requires numpy exists
    pip install -r numpy_requirements.txt && \
    pip install -r requirements.txt && \
    rm /requirements.txt /numpy_requirements.txt

COPY ./pathology/transformation_pipeline/isyntax_transformation/gke_main.py /gke_main.py
COPY ./pathology/transformation_pipeline /pathology/transformation_pipeline/
COPY ./pathology/shared_libs /pathology/shared_libs/
RUN find /pathology -type d -exec chmod 755 {} \; && \
    find /pathology -type f -exec chmod 644 {} \; && \
    chmod 755 /gke_main.py &&\
    groupadd -r NonRootUserGroup && useradd -r -m -g NonRootUserGroup NonRootUser

USER NonRootUser
ENTRYPOINT ["python3", "gke_main.py"]