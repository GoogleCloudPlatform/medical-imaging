# Transformation Base Image Docker

This image is built using the Dockerfile in this directory to provide a base
image with C++ libraries used by the transformation pipeline to speed the
build. This base image removes the need to rebuild the libraries in this
container when the transformation containers cache expires.

- Libraries built in this container:

  *  Boost (1.85.0)
  *  Openslide (4.0.0)
  *  JsonCPP (1.9.5)
  *  Abseil (20240116.2)
  *  DCMTK (3.6.8)


The docker image will need to be rebuilt when these libraries are updated.

To rebuild the base image:

   1. Make the desired changes to Dockerfile
   2. Run (use image built from `../base_py_opencv_docker/` as base):

      ```shell
      gcloud builds submit --config=./cloudbuild.yaml \
        --timeout=24h \
        --substitutions REPO_NAME=<YOUR GCR DESTINATION>,_BASE_CONTAINER=<YOUR BASE CONTAINER GCR>
      ```

   3. Rebuild derived images and test for regressions.