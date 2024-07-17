# OpenCV Base Docker Image

This image is built using the Dockerfile in this directory to provide a base
image with OpenCV and related C++ libraries. OpenCV is built for use in C++ and
Python. OpenCV is built from source and configured to use the built JPEG and
JPEG2000 codecs.

- Libraries built in this container:

  *  LibJpegTurbo (3.0.3) JPEG Codec
  *  OpenJPEG (2.5.2) JPEG 2000 Codec
  *  OpenCV (4.10.0)

To rebuild the base image:

   1. Make the desired changes to Dockerfile
   2. Run (use image built from `../base_py_debian_docker/` as base):

      ```shell
      gcloud builds submit --config=./cloudbuild.yaml \
        --timeout=24h \
        --substitutions REPO_NAME=<YOUR GCR DESTINATION>,_BASE_CONTAINER=<YOUR BASE CONTAINER GCR>
      ```

   3. Rebuild derived images and test for regressions.
