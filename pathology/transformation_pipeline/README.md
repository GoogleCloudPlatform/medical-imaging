# Cloud Pathology Image Transformation Pipeline


## Overview

The transformation pipeline is designed to make it easy to convert proprietary digital pathology image  formats to DICOM and ingest the DICOM images into a [Google Cloud DICOM store](https://cloud.google.com/healthcare-api/docs/how-tos/dicom) at scale. The tool enables you to:


* Perform pixel equivalent transformation for preferred formats (SVS, TIFF, and DICOM)
* Control the pyramid levels generated and materialized in cloud.
* Control the Study Instance UID imaging for generated imaging.
* Merge custom metadata with the generated DICOM imaging.
* Runs at scale in [GKE](https://cloud.google.com/kubernetes-engine?hl=en) and scales horizontally to meet scanning demand.
* Also, supports [local (non-cloud) execution](https://github.com/GoogleCloudPlatform/medical-imaging/tree/main/pathology/transformation_pipeline/local) on linux systems or within virtual machines to enable small jobs and pipeline testing.


## Documentation
* [Deploying Digital Transformation Pipeline IAC.](https://github.com/GoogleCloudPlatform/medical-imaging/tree/main/iac)
* For a high level overview of the transformation pipeline, refer to the documentation [here](https://github.com/GoogleCloudPlatform/medical-imaging/blob/main/pathology/transformation_pipeline/docs/digital_pathology_transformation_pipeline_to_dicom_spec.md)
* For a how to guide with picture walkthrough of the pipeline refer to documentation [here](https://github.com/GoogleCloudPlatform/medical-imaging/blob/main/pathology/transformation_pipeline/docs/digital_pathology_transformation_pipeline_to_dicom.md)


## Requirements

* Recommended OS: Ubuntu 20.04 or higher or Debian 12 or higher
* [Python 3.11+](https://www.python.org/about/)
* [Bazel](https://bazel.build/install)
* [Pip](https://pypi.org/project/pip/) (`sudo apt-get install pip`)
* [Docker](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository)
    * Ensure you have followed the [post installation steps](https://docs.docker.com/engine/install/linux-postinstall/) before proceeding.
* [gcloud command line](https://cloud.google.com/sdk/docs/install)


## Retrieving the Transformation Pipeline Code base 

To get started, clone the repository:

 1. Run the following commands to make a installation directory for
    Cloud Pathology:

  ```shell
    export CLOUD_PATH=$HOME/cloud_pathology
    mkdir $CLOUD_PATH
    cd $CLOUD_PATH
  ```

2. Clone Cloud Pathology into the directory you just created:

  ```shell
    git clone https://github.com/GoogleCloudPlatform/medical-imaging.git $CLOUD_PATH
  ```

## Running Transformation Pipeline Unit Tests

The transformation pipeline Docker executes the transformation pipeline unit tests as the final BUILD step. The unit tests can be executed directly against the code base using Blaze or Python.

1) Run unit tests using Bazel 


```
cd $CLOUD_PATH
cd  ./pathology/transformation_pipeline
bazel test …
```

2)  Run unit tests using Python

```
export PYTHONPATH="${PYTHONPATH}:/"
cd $CLOUD_PATH
python3 -m unittest discover -p "*_test.py" -s "$CLOUD_PATH/pathology/transformation_pipeline" -t $CLOUD_PATH
```

## Building the Transformation Pipeline

To build the Transformation Pipeline container:

  1. Build the [base_transformation_docker](../base_docker_images/base_transformation_docker) container. This step only needs to be repeated if elements in the base container are changed.

  2. Build the Transformation Pipeline container. Set _BASE_CONTAINER to the path to the built base_transformation_docker base container, e.g., gcr.io/${PATH_BASE_TRANSFORM_DOCKER}:latest

  To build the container run:

```
cd $CLOUD_PATH
gcloud builds submit --config=./pathology/transformation_pipeline/cloudbuild.yaml \
  --timeout=24h \
  --substitutions=REPO_NAME="<YOUR GCR DESTINATION>",_BASE_CONTAINER="<YOUR BASE CONTAINER GCR>"
```

  Following a successful build the newly built container will be visible in your destination registry. If the build is not successful inspect the gcloud build logs to determine the issue.

## Running the Transformation Pipeline Locally

The transformation pipeline can be run locally (Linux systems) or within Cloud virtual machines. Directions to build and run the pipeline locally are provided [here](https://github.com/GoogleCloudPlatform/medical-imaging/tree/main/pathology/transformation_pipeline/localhost). Building the local runner requires first building the Cloud deployable container. The local runner does not support horizontal scaling and is intended to facilitate the transformation of small jobs and non-cloud end-to-end testing.


## Cloud Deployment

To deploy the container you built, follow the deployment instructions in the IaC directory [here](https://github.com/GoogleCloudPlatform/medical-imaging/blob/main/iac/README.md) .


## Testing your Cloud Deployment

For a how to guide with a walkthrough of the pipeline refer to documentation [here](https://github.com/GoogleCloudPlatform/medical-imaging/blob/main/pathology/transformation_pipeline/docs/digital_pathology_transformation_pipeline_to_dicom.md)

A tool to test transformation pipeline deployments is provided [here](https://github.com/GoogleCloudPlatform/medical-imaging/blob/main/pathology/transformation_pipeline/test_utils/ingest_data_generator). The tool simplifies triggering transformation pipeline execution by generating synthetic metadata for imaging and triggering the pipeline with the provided imaging and metadata. Among its uses, the tool can be used to test imaging formats, GKE scaling configurations, and metadata generation.