# Cloud Pathology Image Transformation Pipeline

## Overview

The transformation pipeline is a multipurpose container that polls for pub/sub
messages and processes them. The pipeline is configurable via environmental
variables and contains sub-components which transform images into the DICOM store.

The code base is written to be testable via bazel based testing framework and
deployable to GCP hosted GKE containers.

## Getting Started

* For a high level specification overview of the transformation pipeline,
refer to the documentation [here](https://github.com/GoogleCloudPlatform/Cloud-Pathology/blob/main/transformation_pipeline/docs/digital_pathology_transformation_pipeline_to_dicom_spec.md)
* For a how to guide with picture walkthrough of the pipeline refer to documentation
  [here](https://github.com/GoogleCloudPlatform/Cloud-Pathology/blob/main/transformation_pipeline/docs/digital_pathology_transformation_pipeline_to_dicom.md)

## Requirements

  - Recommended OS: Ubuntu 20.04 or higher or Debian 11 or higher
  - [Python 3.8+](https://www.python.org/about/) (`sudo apt-get install python3.8`)
  - [Bazel](https://bazel.build/install)
  - [pip](https://pypi.org/project/pip/) (`sudo apt-get install pip`)
  - [Docker](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository)
    - Ensure you have followed the
  [post installation steps](https://docs.docker.com/engine/install/linux-postinstall/)
  before proceeding.
  - [gcloud command line](https://cloud.google.com/sdk/docs/install)

## Building the Transformation Pipeline

To build a new image (use image built from
`base_docker_images/base_transformation_docker/` as base), run:

```
gcloud builds submit --config=./transformation_pipeline/cloudbuild.yaml \
  --timeout=24h \
  --substitutions=REPO_NAME="<YOUR GCR DESTINATION>",_BASE_CONTAINER="<YOUR BASE CONTAINER GCR>"
```

## Deployment

To deploy the container you built follow the deployment instructions in the IaC
repository [here](https://github.com/GoogleCloudPlatform/cloud-pathology-iac/blob/main/README.md)
.

## Test your deployment

For a how to guide with a walkthrough of the pipeline refer to documentation
  [here](https://github.com/GoogleCloudPlatform/Cloud-Pathology/blob/main/transformation_pipeline/docs/digital_pathology_transformation_pipeline_to_dicom.md)

A tool to test transformation pipeline deployments is provided
  [here](https://github.com/GoogleCloudPlatform/cloud-pathology/tree/main/transformation_pipeline/test_utils/ingest_data_generator). The tool simplifies triggering transformation pipeline execution by generating synthetic metadata for imaging and triggering the pipeline with the provided imaging and metadata. Among its uses the tool can be used to test imaging formats, GKE scaling configurations, and metadata generation.
