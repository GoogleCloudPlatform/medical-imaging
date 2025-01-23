# Transformation Base Image Docker

This image is built using the Dockerfile in this directory to provide a base
image with C++ libraries used by the transformation pipeline to speed the
build. This base image removes the need to rebuild the libraries in this
container when the transformation containers cache expires.

- Libraries built in this container:

  *  Boost (1.87.0)
  *  Openslide (4.0.0)
  *  JsonCPP (1.9.6)
  *  Abseil (20240722.0)
  *  DCMTK (3.6.9)

The docker image will need to be rebuilt when these libraries are updated.

##  Transformation Base Image

To build the Transformation Base container:

1. Build the [base_py_opencv_docker](../base_py_opencv_docker) container. This step only needs to be repeated if elements in the base container are changed.

2. Build the Transformation Base container. Set _BASE_CONTAINER to the path to built base_py_opencv base container, e.g., gcr.io/${PATH_PY_OPENCV_DOCKER_BASE_IMAGE}:latest.

  To build the container image run:

      ```shell
      gcloud builds submit --config=./cloudbuild.yaml \
        --timeout=24h \
        --substitutions REPO_NAME=<YOUR GCR DESTINATION>,_BASE_CONTAINER=<YOUR BASE CONTAINER GCR>
      ```

   Following a successful build the newly built container will be visible in your destination registry. If the build is not successful inspect the gcloud build logs to determine the issue.

3. Rebuild derived images and test for regressions.