# Cloud Pathology

Today there is a distinction between traditional pathology bounded by physical
constraints and the limitless potential of digital pathology. Digitization is a
prerequisite for advancing the state of the art in areas such as health equity,
equitable access, eliminating bias, and improving quality of care and patient
outcomes.

Historically, access to bleeding edge technology has been gated. We want these
technologies to be equitably accessible to as many people in as many places as
quickly as possible in order to broadly deliver on the promise of health equity
for all people wherever they may be. Improving quality of care, patient
outcomes, eliminating bias, and increasing operational efficiency are all
within our collective ability to deliver. Furthermore, this codebase promotes
DICOM as the unifying standard for three key Pathology assets: raw images,
annotations and ML model output and a contribution towards continued support
for interoperability and integration.

In our initial release we are sharing our transformation pipeline. The Digital
Pathology Whole Slide Imaging (WSI) Transformation Pipeline serves as a robust
and scalable solution for converting a wide array of digital pathology input
formats into DICOM WSI format.

This repository contains the source code for a collection of digital pathology
capabilities that will enable users to view and analyze digital pathology
slides. It is designed to improve the efficiency and viability of digitally
enhanced pathology workflows, and to provide clinical and technical
professionals with the tools they need to move their work forward in a
standards oriented approach.


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
    git clone https://github.com/GoogleCloudPlatform/cloud-pathology.git $CLOUD_PATH
  ```

## Applications

### Transformation Pipeline

Get started [here](https://github.com/GoogleCloudPlatform/Cloud-Pathology/blob/main/transformation_pipeline/README.md)


## Deploying

For more information on deploying our applications see our IaC repository
(Coming Soon!).

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for details.

## License

See [`LICENSE`](LICENSE) for details.
