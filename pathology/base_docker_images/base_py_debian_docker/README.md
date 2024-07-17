# Base Docker Image

This image is built using the Dockerfile in this directory to provide a unified
base image with the most recent Debian-slim docker with security patches and an
install of Python. Externally pre-built Python Docker images are not maintained
against Debian docker security updates.

The docker image will need to be rebuilt when:

   1.  New Debian OS version or security patches are released. Slim Docker
       should be used to minimize the OS footprint and security perimeter.

   2.  It is desirable to move to a newer release of Python. Moving to a new
       release of Python will require altering the Python install component of
       the docker. To update consult the official Python Docker for the desired
       version.

       NOTE: Python 3.10.X is not compatible with DPAS Transformation. The next
       major Python version should be 3.11.X or later.

After rebuilding the base image, any derived containers will need to be rebuilt
to incorporate the base image changes.

To rebuild the base image:

   1. Make the desired changes to Dockerfile
   2. Run

      ```shell
      gcloud builds submit --config=./cloudbuild.yaml \
         --timeout=24h --substitutions REPO_NAME=<YOUR IMAGE DESTINATION>
      ```

   3. Rebuild derived images and test for regressions.