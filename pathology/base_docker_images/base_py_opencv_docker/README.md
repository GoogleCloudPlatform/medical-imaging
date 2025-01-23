# OpenCV Base Docker Image

This image is built using the Dockerfile in this directory to provide a base
image with OpenCV and related C++ libraries. OpenCV is built for use in C++ and
Python. OpenCV is built from source and configured to use the built JPEG and
JPEG2000 codecs.

- Libraries built in this container:

  *  LibJpegTurbo (3.1.0) JPEG Codec
  *  OpenJPEG (2.5.3) JPEG 2000 Codec
  *  OpenCV (4.10.0)

To build the OpenCV base container:

1. Build the [base_debian_docker](../base_py_debian_docker) container. This step only needs to be repeated if elements in the base container are changed.

2. Build the OpenCV Base container. Set _BASE_CONTAINER to the path to the built base_py_debian container, e.g., gcr.io/${PATH_BASE_PY_DEBIAN_DOCKER}:latest.

      To build the OpenCV Base container run:

      ```shell
      gcloud builds submit --config=./cloudbuild.yaml \
        --timeout=24h \
        --substitutions REPO_NAME=<YOUR GCR DESTINATION>,_BASE_CONTAINER=<YOUR BASE CONTAINER GCR>
      ```

      Following a successful build the newly built container will be visible in your destination registry. If the build is not successful inspect the gcloud build logs to determine the issue.

3. Any derived images should now be re-built and tested for regressions.
