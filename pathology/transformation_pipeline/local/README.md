# Local Transformation Pipeline


## Overview

The local transformation pipeline is the containerized version of the cloud pathology transformation pipeline that is designed to be executed locally on a linux compatible system or within a linux virtual machine. The local transformation pipeline can use but does not require Cloud services (e.g., DICOM store). The local transformation pipeline does not support horizontal scaling. The primary uses for the local transformation pipeline are to: 1) transform a relatively small number of image assets to DICOM and 2) test transformation pipeline functionality.

## Background

The local transformation pipeline is a docker container that executes the transformation pipeline container within wrappers which abstract away cloud services GKE and optionally other cloud services such as the DICOM Store. The core functionality of the transformation pipeline executed from within the local container is identical to the cloud deployment.

Configuring and starting the local container is not trivial. A Python script is provided to simplify container deployment. The launching Python script has no third-party Python module dependencies.


## Requirements



* Linux environment with [Docker](https://docs.docker.com/)
* [Python](https://www.python.org/) version 3.10+
* [GCloud](https://cloud.google.com/sdk/docs/install)
* [Built Cloud Pathology Transformation Pipeline container.](https://github.com/GoogleCloudPlatform/medical-imaging/tree/main/pathology/transformation_pipeline)

## Quick Start

###   Build local transformation pipeline container (Do once):

* Build Cloud Pathology Transformation Pipeline container. Container can be built into a Cloud Artifact Registry or other Docker compatible repository.
* Build local docker container.
    $./build_docker.sh &lt;TRANSFORMATION_PIPELINE_CONTAINER>

###   Start Transformation Pipeline:

* python3 start_local.py &lt;CONFIGURATION PARAMETERS>


## Using the Local Transformation Pipeline.

Starting and configuring the local container is not trivial. The python script “start_local.py” is provided to simplify container deployment. Section provides an overview of key configuration parameters. This list is not exhaustive please consult the [source](https://github.com/GoogleCloudPlatform/medical-imaging/blob/main/pathology/transformation_pipeline/localhost/start_local.py) for a full list of the configuration parameters.

It is <u>highly recommended</u> that the following parameters are defined:

* dicom_store
* image_ingestion_dir and/or input_images

*DICOM Store Configuration*



* -dicom_store: Defines the HTTPS address of a GCP DICOM Store that locally transformed imaging will be ingested into or the path to a directory on the local file system that the container will use to mock a virtual DICOM store. If running against a local directory the container will write DICOM imaging into the directory. Imaging will be written using a StudyInstanceUID/SeriesInstanceUID/SOPInstanceUID hierarchy. Imaging may be placed within the directory prior to execution to prime the virtual store with imaging. DICOM instances can be retrieved from the virtual store by copying/moving generated imaging from the virtual store. DICOM instances should not be removed while they are being actively read/written to. If the -dicom_store is not defined the local container will store all generated DICOM imagining within a temporary directory that is automatically removed following termination of the start_local.py script.

    Examples:

    *  Configure container to write DICOM instances to a directory on the file system. \
python3 start_local.py -dicom_store /my/dicom/store \

    *  Configure container to write DICOM instances to a Cloud DICOM Store. Parameter value would should match configuration parameter used for GKE container DICOMWEB_URL parameter. The local environment from which the container is executed is required to have application default credentials that permit reading and writing to the DICOM Store. \
 \
python3 start_local.py -dicom_store `https://healthcare.googleapis.com/v1/projects/${PROJECT}/locations/${LOCATION}/datasets/${DATASET}/dicomStores/${DICOM_STORE}/dicomWeb`

*Imaging Input/Output Configuration*



* -image_ingestion_dir: Defines the local directory that contains/or will contain the images that the pipeline will transform. If undefined the pipeline will be configured to use a temporary directory. If not configured then input_images parameter should be configured to define a list of images to transform. By default the pipeline will cease execution when all imaging contained within the ingestion directory has been processed. The pipeline can be configured to poll the ingestion directory. If enabled the pipeline will execute indefinitely and will transform imaging copied into the ingestion directory.

    Examples:

    *  Configure container to read imaging from a specific location on the file system \
python3 start_local.py -image_ingestion_dir /my/ingestion/dir
* -poll: By default the pipeline will cease execution when all imaging contained within the ingestion directory has been processed. The pipeline can be configured to poll the ingestion directory. If enabled the pipeline will execute indefinitely and will transform imaging copied into the ingestion directory.

    Examples:

    *  Configure container to read imaging from a specific location on the file system \
python3 start_local.py -poll True
* -input_images: List of one or more paths to images to be processed by the transformation pipeline. Images may be located on the local file system or in a Google Cloud Storage (GCS). Paths to images stored in GCS  should be defined using a gs style path.

    Examples:

    * python3 start_local.py -input_images /images/file_1.svs /images/file_2.svs gs://my_bucket/file_3.svs
* -processed_image_dir: The transformation pipeline will move images from the image_ingestion_dir to the processed_image_dir after the pipeline has finished transforming the input imaging.  The processed_image_dir parameter defines this location on the local file system. If undefined imaging is moved to a temporary directory.

    Example:

    *  Configure container to read imaging from a specific location on the file system \
python3 start_local.py -processed_image_dir /my/processed/dir

*Metadata Configuration*

By default the pipeline does not require additional metadata to transform imaging. StudyInstanceUIDs are generated as needed for imaging. The mechanisms through which the pipeline joins imaging is described [here](https://github.com/GoogleCloudPlatform/medical-imaging/blob/main/pathology/transformation_pipeline/docs/joining_wsi_imaging_with_metadata.md) and the schemas that control how CSV or BigQuery Table metadata is merged with the generated DICOM is described [here](https://github.com/GoogleCloudPlatform/medical-imaging/blob/main/pathology/transformation_pipeline/docs/transform_pipeline_metadata_values_and_schema.md).



* -metadata_dir: Defines a local directory that the pipeline will read metadata csv files and mapping schemas.

     \
Examples:


    python3 start_local.py -processed_image_dir /my/metadata/dir \


* -big_query: Defines a Big Query table (project_id.dataset_id.table_name) that will be used as the source for slide metadata. By default slide metadata will be read from CSV files written to the metadata_dir.  \


    Examples: \
 \
python3 start_local.py -big_query MY_GCP_PROJECT.MY_DATASET.MY_TABLE
