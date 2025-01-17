# Google Research Digital Pathology

This repository contains open source tools created by Google Research to make it **easy** to bring digital pathology imaging to [Google Cloud](https://cloud.google.com/healthcare-api/docs/how-tos/dicom) and to serve pathology images from Google Cloud for a variety of applications including: medical imaging AI, interactive visualization, and more.

<center><img src="pathology/images/frontend.png"></img></center>

## [Cloud Pathology](https://github.com/GoogleCloudPlatform/medical-imaging/tree/main/pathology)
The cloud pathology repository contains the following components:

#### <li> [Transformation Pipeline](https://github.com/GoogleCloudPlatform/medical-imaging/tree/main/pathology/transformation_pipeline)</li>
Ingest digital pathology images into the [Google Cloud DICOM store](https://cloud.google.com/healthcare-api/docs/how-tos/dicom). The tool supports image format conversions for non-DICOM images, DICOM metadata augmentation, and more. The tool is designed to run at scale in [GKE](https://cloud.google.com/kubernetes-engine?hl=en) to enable enterprise-scale image ingestion. The transformation pipeline can also be run [locally](https://github.com/GoogleCloudPlatform/medical-imaging/tree/main/pathology/transformation_pipeline/local) on a Linux system or within a virtual machine for testing and small jobs.

#### <li> [DICOM Proxy](https://github.com/GoogleCloudPlatform/medical-imaging/tree/main/pathology/dicom_proxy)</li>
HTTP proxy for Google Cloud DICOM store that adds additional pathology-specific features, e.g., accelerated frame serving, support for DICOM rendered transcoding of JPEG-XL encoded DICOM images, and Server side ICC Color Profile transformation, etc.

#### <li>[Front End](https://github.com/GoogleCloudPlatform/medical-imaging/tree/main/pathology/viewer)</li>
Reference zero footprint web viewer for visualization of DICOM digital pathology imaging. Viewer supports interactive visualization and annotation of DICOM pathology imaging.


## [Image Lifecycle Management](https://github.com/GoogleCloudPlatform/medical-imaging/tree/main/ilm)

This repository also contains Google Research's Image Life Cycle Management (ILM) tool. ILM is **applicable to all DICOM imaging modalities**. ILM automates DICOM assets movement between [DICOM store storage classes](https://cloud.google.com/healthcare-api/docs/dicom-storage-class) to minimize Cloud storage cost. The ILM tool monitors the Google DICOM store logs and uses heuristics (rules set by the user) to automate the movement of DICOM images to the optimal storage tier (standard - archival).


## [Infrastructure as Code (IaC)](https://github.com/GoogleCloudPlatform/medical-imaging/tree/main/iac)

Use terraform to configure cloud infrastructure to deploy pathology components and/or ILM.

## Related Open Source Projects

#### <li> [EZ-WSI DICOMweb](https://github.com/GoogleCloudPlatform/EZ-WSI-DICOMweb)</li>
EZ WSI DicomWeb is a [Pip installable](https://pypi.org/project/ez-wsi-dicomweb/) Python library that greatly simplifies the accessing digital pathology images from a Cloud DICOM Store. The library provides built in support for frame caching to accelerate image access and provides a [client-side interface](https://github.com/GoogleCloudPlatform/EZ-WSI-DICOMweb/blob/main/ez_wsi_dicomweb/documentation/getting_started_with_embeddings.ipynb) to easily convert pathology imaging into ML embeddings using Google Research Pathology Foundations ML embeddings.

#### <li>[Google Research Pathology Foundations](https://research.google/blog/helping-everyone-build-ai-for-healthcare-applications-with-open-foundation-models/)</li>
[Path Foundation](https://developers.google.com/health-ai-developer-foundations/path-foundation) is a machine learning (ML) model that produces digital pathology specific ML embeddings. The embeddings produced by this model can be used to efficiently build AI models for pathology related tasks, with less data and less compute than traditional approaches; see [blog post](https://research.google/blog/health-specific-embedding-tools-for-dermatology-and-pathology/) and example [colabs](https://github.com/Google-Health/path-foundation/blob/master/notebooks/README.md).

## Contributing

See <code>[CONTRIBUTING.md](http://CONTRIBUTING.md)</code> for details.


## License

See <code>[LICENSE](http://LICENSE)</code> for details.

