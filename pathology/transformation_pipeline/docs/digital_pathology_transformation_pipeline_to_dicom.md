# HowTo: Digital Pathology WSI Transformation Pipeline to DICOM

## Purpose

The universal Digital Pathology Whole Slide Imaging (WSI) Transformation
Pipeline serves as a robust and scalable solution for converting a wide array of
digital pathology input formats into DICOM WSI format. This paper thoroughly
outlines the technical specifications and configuration options of the
pipeline's capabilities.

## Background

![alt text](https://github.com/GoogleCloudPlatform/Cloud-Pathology/blob/main/transformation_pipeline/docs/images/graphical_overview_1.png?raw=true)

![alt text](https://github.com/GoogleCloudPlatform/Cloud-Pathology/blob/main/transformation_pipeline/docs/images/graphical_overview_2.png?raw=true)

Figure 1: A graphical overview of the digital pathology pipeline
The process of transforming an input Whole Slide Imaging (WSI) file into DICOM
format in a destination DICOM store consists of the following steps:

1. The arrival of the WSI image activates the Google Cloud Storage (GCS)
    bucket's publish/subscribe mechanism.
2. The Google Kubernetes Engine (GKE) hosted ingest container acknowledges
    the incoming message.
3. The GKE container then downloads both the WSI file and its corresponding
    metadata.
4. The system identifies the side via the barcode in the slide label or
    alternative, found in the filename.
5. The system searches for the slide metadata within a Big Query Table or
    CSV file(s).
6. It generates a DICOM JSON file containing the metadata for the WSI file.
7. The system then transforms the WSI file into one or multiple DICOM
    instrances and amalgamates the metadata with them.
8. If applicable, a secondary capture of DICOM files is then generated for
    backup purposes.
9. The DICOM store is subsequently scanned to prevent the duplication of
    DICOM files.
1. The DICOM files are then uploaded to the destination DICOM store.
1. Finally, all the performed actions are recorded and logged to Google
    Cloud Operations (Ops) for tracking and future reference.

## Transformation Pipeline Steps:

1. **Installation**

    The transformation docker container is to be deployed as a workload
    within a Google Kubernetes Engine (GKE) in your GCP environment. This
    deployment can be executed using an Infrastructure as Code (IaC) tool like
    Terraform or other equivalent methods. The workload's settings are
    controlled by environment variables, which can be configured using the
    Terraform scripts or through a YAML file specifically designed for GKE
    configuration [[1]](https://github.com/GoogleCloudPlatform/cloud-pathology-iac/blob/main/readme.md).

    Pipeline configuration in [GKE](https://cloud.google.com/kubernetes-engine)
    (Auto configuration by IaC):

    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;In brief, the Digital Pathology
    Transformation pipeline is
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;configured by IaC to:

    -  Execute using
        [C2-Standard 30](https://cloud.google.com/compute/docs/compute-optimized-machines)
        (recommended) virtual machines.
        -  Pyramid generation is a memory and compute heavy task
            and requires a compute optimized VM with large memory. These
            requirements are directly affected by the size of imaging (pixel
            area dimensions).

    -  Pipeline pods map to a single virtual machine.
        -  Memory requirement is directly affected by size of imaging ingested.
        -  Container will consume all available compute resources when
        ingesting large imaging. Machines with greater compute capacity,
        C2-Standard-30 > C2-Standard-16, will execute faster for large
        imaging.  As imaging decreases in size the effect of VM compute
        capacity on pipeline performance decreases.

    -  Pipeline horizontally scales based on
    [pub/sub](https://cloud.google.com/pubsub) message queue depth.

    In addition to the GKE workload, 4 GCS buckets are required:

    1. Input imaging bucket
    2. Input imaging metadata bucket
    3. Output success - where successful transformation originals are moved to.
    4. Output fail - where failed transformation originals are moved to.

	Configurable Environmental Variables:

    -  `CLOUD_OPS_LOG_NAME: Name of default log transformation pipeline logs
    are written to; Default value: transformation_log; configured by IaC.`

2. **Slide Metadata**

    Pipeline requires a metadata definition for all slides. Slide metadata
    configuration described in document [[2]](https://github.com/GoogleCloudPlatform/cloud-pathology/blob/main/transformation_pipeline/docs/transform_pipeline_metadata_values_and_schema.md).
    Metadata files should be uploaded to the IaC created metadata ingestion
    bucket.

	Configurable Environmental Variables:

    -  `METADATA_BUCKET`: Value GS formatted path to GCS bucket that stores
        ingestion metadata. Default value:
        `gs://{PROJECT_ID}-transformation-input-metadata `defined by IaC script.

    -  `BIG_QUERY_METADATA_TABLE`: Define to enable metadata value definition
        from BigQuery table or table view. Value should be formatted
        "{gcp_project_id}.{big_query_dataset_id}.{table_name}" to define the GCP
        project, Dataset, and table name the Big Query Table resides in. The
        transformation pipeline service account must be given IAM read/write
        permissions to the table. When enabled the pipeline will not import
        metadata from CSV files. See [[2]](https://github.com/GoogleCloudPlatform/cloud-pathology/blob/main/transformation_pipeline/docs/transform_pipeline_metadata_values_and_schema.md) for more
        information.

    -  `METADATA_PRIMARY_KEY_COLUMN_NAME`: Defines metadata value table column
        that contains the primary key value used to match imaging with metadata;
        default value "BarCode Value". Metadata primary key column is required to
        be defined in the metadata value table. See [[2]](https://github.com/GoogleCloudPlatform/cloud-pathology/blob/main/transformation_pipeline/docs/transform_pipeline_metadata_values_and_schema.md)
        for more information.

    -  `GCS_INGEST_STUDY_INSTANCE_UID_SOURCE`: Applies Only to DICOM instances
        ingested via GCS. Options = METADATA or DICOM. Default value: METADATA. If
        defined as METADATA then Study Instance UID in DICOM instances will be
        replaced with value defined in metadata. If defined as DICOM then Study
        Instance UID in DICOM instances will not be replaced with value in metadata.

3. **Slide Imaging**

    The transformation pipeline is triggered by
    [pub/sub](https://cloud.google.com/pubsub) messages. The pipeline supports
    receiving messages from
    [GCS](https://cloud.google.com/storage/docs/reporting-changes) and the
    [DICOM store](https://cloud.google.com/healthcare-api/docs/dicom-pubsub).
    The effect of the message sources is slightly different.

    -  [GCS pub/sub messages](https://cloud.google.com/storage/docs/reporting-changes)
        trigger full transformation pipeline action when imaging is added to the
        image ingestion bucket. All imaging is required to have metadata defined
        in the metadata value table
        (See [[2]](https://github.com/GoogleCloudPlatform/cloud-pathology/blob/main/transformation_pipeline/docs/transform_pipeline_metadata_values_and_schema.md)
        ). The specific imaging formats and the process through which the images
        are joined with the metadata value table is (described in
        [[3]](https://github.com/GoogleCloudPlatform/cloud-pathology/blob/main/transformation_pipeline/docs/joining_wsi_imaging_with_metadata.md)
        ). In brief, the triggering imaging is transformed as needed to DICOM,
        metadata is merged with the generated DICOM, and the generated DICOM is
        uploaded to the DICOM store.

    -  DICOM store pub/sub messages that identify the addition of new imaging
        to the store can be configured to trigger the Transformation Pipeline to
        perform a metadata merge with the newly added DICOM instance. In brief,
        a metadata merge is performed by determining if metadata is defined in
        the metadata value table
        [[2]](https://github.com/GoogleCloudPlatform/cloud-pathology/blob/main/transformation_pipeline/docs/transform_pipeline_metadata_values_and_schema.md)
        for the DICOM instance by comparing the Barcode Value in the added
        DICOM with the metadata primary key in the metadata value table. If
        metadata exists then the DICOM in the store is updated to contain the
        additional metadata. If no metadata is found, then DICOM is left
        unmodified in the store.

    IaC defined location:

    -  `WSI Image Ingestion Bucket`: {PROJECT_ID}-transformation-input-images

    Configurable Environmental Variables

    -  `PROJECT_ID`: GCP project that contains pub/sub message subscriptions;
        defined using IaC script.
    -  `GCS_SUBSCRIPTION`: Pub/sub subscription for GCS image ingest bucket.
        Default value: `transformation-gcs-subscription `defined by IaC script.
    -  `DICOM_STORE_SUBSCRIPTION`: Pub/sub subscription for DICOM store
        messages. Default value: `transformation-dicom-store-subscription `
        defined by IaC script.
    -  `INGEST_IGNORE_ROOT_DIR`: Files uploaded into listed image ingestion
        bucket root  bucket folder names will be ignored by the Transformation
        Pipeline. Default setting ignores temporary files created by
        [TSOP](https://cloud.google.com/storage-transfer-service). Default value =
        `["cloud-ingest", "storage-transfer"]`
    -  `GCS_UPLOAD_IGNORE_FILE_EXT`: Comma separated values of defining file
        extensions which should be ignored if uploaded GCS. Not defined by default
        IaC. Example, excludes json, text, and files without extensions:  `.json,
        .txt, ""`
    -  `SLIDEID_REGEX`: Described in [[3]](https://github.com/GoogleCloudPlatform/cloud-pathology/blob/main/transformation_pipeline/docs/joining_wsi_imaging_with_metadata.md).
    -  `FILENAME_SLIDEID_SPLIT_STR`: Described in [[3]](https://github.com/GoogleCloudPlatform/cloud-pathology/blob/main/transformation_pipeline/docs/joining_wsi_imaging_with_metadata.md).
    -  `TEST_WHOLE_FILENAME_AS_SLIDEID`: Described in
        [[3]](https://github.com/GoogleCloudPlatform/cloud-pathology/blob/main/transformation_pipeline/docs/joining_wsi_imaging_with_metadata.md).
    -  `DISABLE_BARCODE_DECODER`: Described in [[3]](https://github.com/GoogleCloudPlatform/cloud-pathology/blob/main/transformation_pipeline/docs/joining_wsi_imaging_with_metadata.md).

4. **Transforming Metadata to DICOM**

    Slide imaging metadata defined within the metadata value table
    [[2]](https://github.com/GoogleCloudPlatform/cloud-pathology/blob/main/transformation_pipeline/docs/transform_pipeline_metadata_values_and_schema.md)
    is transformed to JSON formatted DICOM and then merged with the DICOM images
    generated in Step 5.

    Configurable Environmental Variables

    -  `ENABLE_CREATE_MISSING_STUDY_INSTANCE_UID`: Default (False); By default
        the transformation pipeline requires that metadata and/or ingested DICOM
        define a valid DICOM StudyInstanceUID for each slide. Setting
        `ENABLE_CREATE_MISSING_STUDY_INSTANCE_UID `to True will configure the
        pipeline to generate StudyInstanceUID for metadata that does not define the
        StudyInstanceUID. Pipeline generated Study Instance UID requires that the
        metadata define Accession Number. See [[2]](https://github.com/GoogleCloudPlatform/cloud-pathology/blob/main/transformation_pipeline/docs/transform_pipeline_metadata_values_and_schema.md)
        for more information.

    -  `REQUIRE_TYPE1_DICOM_TAG_METADATA_IS_DEFINED`: By default metadata
        values are not required to define values for all DICOM Type 1 tags
        defined in the Metadata mapping schema. This can be enabled by setting
        the Transformation Pipeline environmental variable
        `REQUIRE_TYPE1_DICOM_TAG_METADATA_IS_DEFINED: True`. Refer to Transform
        Pipeline DICOM Metadata Values and Schema document
        [[2]](https://github.com/GoogleCloudPlatform/cloud-pathology/blob/main/transformation_pipeline/docs/transform_pipeline_metadata_values_and_schema.md)
        for more information.

5. **Transformation of Slide Imaging to DICOM**

    The DICOM pyramid generation is performed using the
    [wsi-to-dicom converter](https://github.com/GoogleCloudPlatform/wsi-to-dicom-converter);
    native code that will scale to consume all available GKE pod resources. At
    a high level the CPU and memory resources required to run transformation
    are dependent on the pixel area and the image compression formats used
    within the input WSI imaging and the output DICOM imaging

    Transformation of WSI imaging to DICOM is configured through the
    following environmental variables:

    -  `INGESTION_PYRAMID_LAYER_GENERATION_CONFIG_PATH`: Defines pyramid
        imaging transformation command line parameters. Refer to:
        [WSI Pyramid Downsampling Config](https://github.com/GoogleCloudPlatform/cloud-pathology/blob/main/transformation_pipeline/docs/wsi_pyramid_downsampling_configuration.md).

    -  `DICOM_FRAME_HEIGHT`: Defines height of DICOM frames in pixels. Default
        value: 256, Recommended value range (256 - 512) and recommended that frame
        width and height are equal. This parameter does not apply when pixel
        equivalent transform is performed. When pixel equivalent ingestion is used
        the DICOM frame dimensions are defined by the frame dimensions in the input
        imaging.

    -  `DICOM_FRAME_WIDTH`: Defines width of DICOM frames in pixels. Default
        value: 256, Recommended value range (256 - 512) and recommended that frame
        width and height are equal. This parameter does not apply when pixel
        equivalent transform is performed. When pixel equivalent ingestion is used
        the DICOM frame dimensions are defined by the frame dimensions in the input
        imaging.

    -  `COMPRESSION`: Defines image compression used to encode DICOM frames.
        Default value: JPEG, Options[JPEG, JPEG2000, RAW]. Does apply to pyramid
        layers ingested using pixel equivalent transform.

    -  `FIRST_LEVEL_COMPRESSION`: Optional, if defined superseeds compression
        parameter and defines image compression used to encode highest
        magnification level. Default value: undefined, Options[JPEG, JPEG2000, RAW,
        and Undefined].  Does apply to pyramid layers ingested using pixel
        equivalent transform.

    -  `JPEG_COMPRESSION_QUALITY`: Defines image jpeg compression quality
        setting. Default value: 95, range(1 - 100).

    -  `JPEG_COMPRESSION_SUBSAMPLING_FLG`: Defines image jpeg compression
        subsampling quality setting. Default value: SUBSAMPLE_444,
        Options[SUBSAMPLE_444, SUBSAMPLE_440, SUBSAMPLE_442, SUBSAMPLE_420].

    -  `PIXEL_EQUIVALENT_TRANSFORM`: Defines image pyramid levels transformed
        using pixel equivalent transformation. Default value:
        HIGHEST_MAGNIFICATION, Options[HIGHEST_MAGNIFICATION, ALL_LEVELS, DISABLED].

    -  `FLAT_IMAGES_VL_MICROSCOPIC_IMAGE_IOD`: Optional boolean flag., Default
        value: False. If true configures un-tiled  image ingestion to be encoded
        using the VL MICROSCOPIC IMAGE IOD else VL Slide-Coordinates Microscopic
        Image IOD.

6. **Upload DICOM Imaging to Cloud DICOM Store**

> DICOM images generated by the Transformation pipeline are uploaded to the
DICOM store. For Convenience DICOM instances may also be copied to GCS.

    -  `DICOMWEB_URL`: URL to DICOM store that the DICOM instances created by
        the transformation pipeline will be uploaded to. Default value:
        `[https://healthcare.googleapis.com/v1/projects/{MY_PROJECT}/locations/{MY_LOCATION}/datasets/{MY_DATASET}/dicomStores/{MY_DICOM_STORE}/dicomWeb](https://healthcare.googleapis.com/v1/projects/{MY_PROJECT}/locations/{MY_LOCATION}/datasets/{MY_DATASET}/dicomStores/{MY_DICOM_STORE}/dicomWeb)
        defined by IaC.`

    -  `COPY_DICOM_TO_BUCKET_URI`: Defines, optional, GS style path to GCS
        bucket where DICOM imaging should be mirrored. DICOM instances output using
        gs://{bucket}/{StudyInstanceUID}/{SeriesInstanceUID}/{SOPInstanceUID}.dcm
        organization.

7. **Conclusion of Transformation**

    At the conclusion of the transformation the transformation pipeline can
    be configured to log a link to the imaging added to the DICOM store.
    Imaging is copied or moved from the ingestion bucket to a success or
    failure bucket path. All images placed in the success/failure bucket will
    have associated metadata that identifies the Cloud Operations logs which
    result in the files placement.

    Configurable Environmental Variables:

    -  `VIEWER_DEBUG_URL`: Defines, optional, Base URL to DICOM store that the
        transformation pipeline log to provide a direct link to from the logs to
        the imaging in a third party viewer. Assumes third party viewer is
        parameterized with DICOM web path. Leave undefined to disable. Default
        value:
        `https://{DOMAIN_NAME}/dpas/viewer?series=projects/{MY_PROJECT}/locations/{MY_LOCATION}/datasets/{MY_DATASET}/dicomStores/{MY_DICOM_STORE}/dicomWeb
        defined by IaC.`

    -  `INGEST_SUCCEEDED_URI`: GS style URI defining the GCS path that imaging
        should be moved to when an image successfully ingests into the DICOM Store.
        Success and failure bucket paths can share the same bucket but should not
        be identical. Default value:
        `gs://{PROJECT_ID}-transformation-output/success defined by IaC.`

    -  `INGEST_FAILED_URI `GS style URI defining the GCS the GCS path that
        imaging should be moved to when image transformation fails. Success and
        failure bucket paths can share the same bucket but should not be identical.
        Failed ingestions can be retried by moving the image from the failure
        bucket to the Transformation Pipeline'cs image input bucket. Default value:
        `gs://{PROJECT_ID}-transformation-output/failure defined by IaC.`

    -  `DELETE_FILE_FROM_INGEST_BUCKET_AT_INGEST_SUCCESS_OR_FAILURE`:  Default
        = True. If true, at pipeline completion the image triggering pipeline
        operation will be moved from the image ingestion bucket to the success or
        failure bucket. If False; then the file will be copied to the success or
        failure bucket but removed from the image ingestion bucket. Setting the
        flag to false will result in duplication of the pipeline triggering imaging
        in the ingestion bucket and the success or failure bucket at pipeline
        completion. The purpose of this flag is to enable workflows that require
        that files be retained within the ingestion bucket to avoid duplicate upload.

## References

* [1] [HowTo: Setting up a Cloud Pathology Preview Deployment](https://github.com/GoogleCloudPlatform/cloud-pathology-iac/blob/main/README.md)

* [2] [HowTo: Transform Pipeline DICOM Metadata Values and Schema](https://github.com/GoogleCloudPlatform/cloud-pathology/blob/main/transformation_pipeline/docs/transform_pipeline_metadata_values_and_schema.md)

* [3] [Joining WSI Imaging with Metadata](https://github.com/GoogleCloudPlatform/cloud-pathology/blob/main/transformation_pipeline/docs/joining_wsi_imaging_with_metadata.md)