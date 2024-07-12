# Heuristics based Image Lifecycle Management

## Overview

The Heuristics based Image Lifecycle Management pipeline is an Apache Beam
pipeline that runs in [Dataflow](https://cloud.google.com/dataflow) to
automatically update storage classes for instances on a
[DICOM Store](https://cloud.google.com/healthcare-api/docs/how-tos/dicom)
based on user defined rules around DICOM metadata and instance access data.

[Storage Tiering](https://cloud.google.com/storage/docs/storage-classes) is a
feature on the Healthcare DICOM Store API. Available storage classes for
individual DICOM instances are: Standard, Nearline, Coldline and Archive.

DICOM metadata is obtained from the BigQuery table streamed directly from the
DICOM Store. To configure streaming, see documentation
[here](https://cloud.google.com/healthcare-api/docs/how-tos/dicom-bigquery-streaming).

Instance access data is obtained from Data Access Audit logs streamed to
BigQuery. To configure streaming, see documentation
[here](https://cloud.google.com/logging/docs/export/configure_export_v2).

The following request types are covered:

* RetrieveInstance
* RetrieveRenderedInstance
* RetrieveFrames
* RetrieveRenderedFrames
* RetrieveStudy
* RetriveSeries

Note: Frame requests are considered for partial instance access, e.g. if 3
frames are accessed out of 10, it will only count as a 0.3 access.

## Requirements

*  Recommended OS: Ubuntu 20.04 or higher or Debian 11 or higher
*  [Docker](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository)
*  [gcloud command line](https://cloud.google.com/sdk/docs/install)

## Pipeline configuration

See `ilm_config.py` for details and full documentation. See
`deployment/sample_config.json` for an example config.

The `ImageLifecycleManagementConfig` class defines all pipeline configuration
options and is passed to the pipeline as a GCS file with the configuration in
JSON format. See section below for details on each option.

Rules to move between storage classes are defined in `StorageClassConfig`,
which can be tailored according to your use case. Note that within a `MoveRule`,
Instances are moved if **ANY** of the conditions in the downgrade
(`ToLowerAvailabilityCondition`) or upgrade (`ToHigherAvailabilityCondition`)
list are met. For a condition to be satisfied, **ALL** the criteria within it
must be satisfied.

The `ImageLifecycleManagementConfig` also includes a **`dry_run` mode**. If
`dry_run=true`, the pipeline will compute planned storage class updates
according to the `StorageClassConfig` and generate a report without executing
updates in the DICOM Store. This can be used for testing the pipeline and
verifying planned storage class updates before actually executing them.

A report with storage class updates in CSV format is generated at the end of the
pipeline (see `ReportConfiguration` for details).

## Build Docker images

To build Docker images for the ILM worker and launcher, run the following:

```shell
GCR_DESTINATION_LAUNCHER=<set me>
GCR_DESTINATION_WORKER=<set me>

gcloud builds submit --config=./deployment/cloudbuild.yaml \
--substitutions=_REPO_NAME=${GCR_DESTINATION_LAUNCHER},_DOCKERFILE=./deployment/dataflow_launcher.Dockerfile

cloud builds submit --config=./deployment/cloudbuild.yaml \
--substitutions=_REPO_NAME=${GCR_DESTINATION_WORKER},_DOCKERFILE=./deployment/dataflow_worker.Dockerfile
```

## Running in Dataflow

To configure a pipeline to run in Dataflow using a Flex template, see
documentation [here](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates).

### Required permissions

Service account used to run Dataflow pipeline (see `service_account_email`
parameter in *Launch pipeline* section below) will require IAM permissions
for reading/writing to GCS buckets configured, DICOM Store, BigQuery logs and
DICOM metadata tables, reading images from artifact registry, running Dataflow
pipeline and logs writing. Permissions to build and read flex template are
listed [here](https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates?permissions_to_build_a_flex_template).

Additionally, the Cloud Healthcare API service account (i.e.
`service-<project number>@gcp-sa-healthcare.iam.gserviceaccount.com`) must have
the Storage Object Viewer role for the temporary GCS bucket used in the ILM
config (`tmp_gcs_uri` parameter). This is required to allow reading the filter
files on the storage class change requests (setBlobStorageSettings) to the
DICOM Store.

### Build Flex template

To run the pipeline in Dataflow, you will
need to build a flex template. You can build the template file in GCS with the
following:

```shell
PATH_TO_LOCAL_TEMPLATE_FILE=deployment/metadata.json
PATH_TO_GCS_TEMPLATE_FILE=<set me>
GCR_DESTINATION_LAUNCHER=<set me>

gcloud dataflow flex-template build ${PATH_TO_GCS_TEMPLATE_FILE} \
--image ${GCR_DESTINATION_LAUNCHER} \
--sdk-language "PYTHON" \
--metadata-file ${PATH_TO_LOCAL_TEMPLATE_FILE}
```

When launching the pipeline, the `--template-file-gcs-location` flag should
correspond to the `PATH_TO_GCS_TEMPLATE_FILE` created above.

### Launch pipeline

Example command (replace with your own values for GCP project, GCS paths,
service account, etc):

```shell
gcloud dataflow flex-template run ilm-pipeline \
--region us-west1 \
--project my-proj \
--template-file-gcs-location gs://my-gcs-bucket/<path to metadata.json> \
--parameters experiments=use_runner_v2 \
--parameters max_num_workers=5 \
--parameters sdk_container_image=gcr.io/<path to ILM worker image> \
--parameters sdk_location=container \
--parameters service_account_email=my-service-account@my-proj.iam.gserviceaccount.com \
--parameters worker_machine_type=n2-highmem-4 \
--parameters project=my-proj \
--parameters temp_location=gs://my-gcs-bucket/temp_location \
--parameters staging_location=gs://my-gcs-bucket/staging_location \
--parameters ilm_config_gcs_uri=gs://my-gcs-bucket/<path to ILM config.json>
```

### Schedule to run automatically

To schedule the ILM pipeline to run automatically following a custom defined
schedule (e.g. every Monday at 10am, every weekday, etc), create a data pipeline
in Dataflow. See full documentation [here](https://cloud.google.com/dataflow/docs/guides/data-pipelines#create_a_data_pipeline)
for details on how to create it.

Make sure to select the custom template option and point to the flex template in
GCS defined above (`gs://my-gcs-bucket/<path-to-metadata.json>`). Add the same
parameters as in the *Launch pipeline* section above (if creating the data
pipeline by importing from a job, these parameters will be automatically
populated). Also note the cloud scheduler service account used will require
`roles/datapipelines.invoker` IAM role.

## Testing locally

To run locally (make sure to have all packages in `deployment/requirements.txt`
installed),

```
python -m batch_pipeline_main \
--ilm_config_gcs_uri=gs://my-gcs-bucket/<path to ILM config.json> \
--project=my-proj \
--temp_location=gs://my-gcs-bucket/temp_location \
--runner=apache_beam.runners.portability.fn_api_runner.FnApiRunner
```
