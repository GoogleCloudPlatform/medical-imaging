# Cloud Pathology

<center><img src="pathology/images/frontend.png"></img></center>

This repository contains open source tools created by Google Research to make it ***easy*** to bring digital pathology imaging to [Google Cloud](https://cloud.google.com/healthcare-api/docs/how-tos/dicom) and to serve pathology images from Google Cloud for a variety of applications including: medical imaging AI, interactive visualization, and more.

This repo contains:

### <li> [Transformation Pipeline](https://github.com/GoogleCloudPlatform/medical-imaging/tree/main/pathology/transformation_pipeline)</li>
Ingest digital pathology images into the [Google Cloud DICOM store](https://cloud.google.com/healthcare-api/docs/how-tos/dicom). The tool supports image format conversions for non-DICOM images, DICOM metadata augmentation, and more. The tool is designed to run at scale in [GKE](https://cloud.google.com/kubernetes-engine?hl=en) to enable enterprise-scale image ingestion. The transformation pipeline can also be run [locally](https://github.com/GoogleCloudPlatform/medical-imaging/tree/main/pathology/transformation_pipeline/local) on a Linux system or within a virtual machine for testing and small jobs.

### <li> [DICOM Proxy](https://github.com/GoogleCloudPlatform/medical-imaging/tree/main/pathology/dicom_proxy)</li>
HTTP proxy for Google Cloud DICOM store that adds additional pathology-specific features, e.g.: Accelerated frame serving, support for DICOM rendered transcoding of JPEG-XL encoded DICOM images, and Server side ICC Color Profile transformation, etc.

### <li>[Front End](https://github.com/GoogleCloudPlatform/medical-imaging/tree/main/pathology/viewer)</li>
Reference zero footprint web viewer for visualization of DICOM digital pathology imaging. Viewer supports interactive visualization and annotation of DICOM pathology imaging.


## Getting Started

To get started, clone the repository:

  1. Run the following commands to make an installation directory for
    Cloud Pathology:

  ```shell
    export CLOUD_PATH=$HOME/cloud_pathology
    mkdir $CLOUD_PATH
    cd $CLOUD_PATH
  ```

2. Clone Cloud pathology into the directory you just created:

  ```shell
    git clone https://github.com/GoogleCloudPlatform/medical-imaging.git $CLOUD_PATH
  ```

## Deploying

For more information on deploying our applications see the IaC directory [here](https://github.com/GoogleCloudPlatform/medical-imaging/tree/main/iac).


## Related Open Source Projects

#### <li> [EZ-WSI DICOMweb](https://github.com/GoogleCloudPlatform/EZ-WSI-DICOMweb)</li>
EZ WSI DicomWeb is a [Pip installable](https://pypi.org/project/ez-wsi-dicomweb/) Python library that greatly simplifies the accessing digital pathology images from a Cloud DICOM Store. The library provides built in support for frame caching to accelerate image access and provides a [client-side interface](https://github.com/GoogleCloudPlatform/EZ-WSI-DICOMweb/blob/main/ez_wsi_dicomweb/documentation/getting_started_with_embeddings.ipynb) to easily convert pathology imaging into ML embeddings using Google Research Pathology Foundations ML embeddings.

#### <li>[Google Research Pathology Foundations](https://research.google/blog/helping-everyone-build-ai-for-healthcare-applications-with-open-foundation-models/)</li>
[Path Foundation](https://developers.google.com/health-ai-developer-foundations/path-foundation) is a machine learning (ML) model that produces digital pathology specific ML embeddings. The embeddings produced by this model can be used to efficiently build AI models for pathology related tasks, with less data and less compute than traditional approaches; see [blog post](https://research.google/blog/health-specific-embedding-tools-for-dermatology-and-pathology/) and example [colabs](https://github.com/Google-Health/path-foundation/blob/master/notebooks/README.md).
