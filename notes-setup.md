# PathCloud Setup Notes for Medical-Imaging-GCP

Build base image dockerfile:
`pathology/base_docker_images/base_py_debian_docker/Dockerfile`
Base Image Name: medicalimaging-gcp-base-py-debian:latest

- can't use hyphens to refer to a base image
  docker tag 8fc53e693cbf base_py_debian_docker

### Build base image py_opencv

```sh
docker buildx build --build-arg BASE_CONTAINER=base_py_debian_docker:latest -t base_py_opencv_docker .
docker tag 0870763d5c0b us-west2-docker.pkg.dev/gcp-pathology-poc1/pathcloud/base_py_opencv_docker
```

or download and tag it:

```sh
docker pull us-west2-docker.pkg.dev/gcp-pathology-poc1/pathcloud/base_py_opencv_docker:latest
docker tag us-west2-docker.pkg.dev/gcp-pathology-poc1/pathcloud/base_py_opencv_docker:latest base_py_opencv_docker
```

### Build dicom-proxy container

After adding or modifying pip packages in the requirements.in, update the requirements.txt file in the `pathology/dicom_proxy` directory with the following command:

```sh
cd pathology/dicom_proxy
pip-compile --generate-hashes requirements.in
```

Then build the dicom-proxy container using the following command:

```sh
docker buildx build --build-arg BASE_CONTAINER=base_py_opencv_docker:latest -t us-west2-docker.pkg.dev/gcp-pathology-poc1/pathcloud/dicom-proxy-gcp:0.0.1 -f ./pathology/dicom_proxy/Dockerfile .
```

```sh
docker run -d --name dicom-proxy-gcp \
  -p 8080:8080 \
  -e ORIGINS=* \
  -e GOOGLE_APPLICATION_CREDENTIALS=/credentials/gcp-pathology-poc1_service_key.json \
  -e VALIDATE_IAP=false \
  -e JWT_AUDIENCE=/projects/1053568465268/global/backendServices/1470682154844812331 \
  -e URL_PATH_PREFIX=/dicom_proxy \
  -e API_PORT_FLG=8080 \
  -e DEFAULT_DICOM_STORE_API_VERSION=v1beta1 \
  -e ENABLE_APPLICATION_DEFAULT_CREDENTIALS=false \
  -e ENABLE_DEBUG_FUNCTION_TIMING=false \
  -e REDIS_CACHE_HOST_IP=localhost \
  -e REDIS_CACHE_HOST_PORT=6379 \
  -e GUNICORN_BIND=0.0.0.0:8080 \
  -e ENABLE_FAKE_EMAIL=true \
  -v $(pwd)/gcp-pathology-poc1_service_key.json:/credentials/gcp-pathology-poc1_service_key.json \
  us-west2-docker.pkg.dev/gcp-pathology-poc1/pathcloud/dicom-proxy-gcp:0.0.1
```

# To see the log output at the console, use the following command:

docker logs -f dicom-proxy-gcp

```sh
gcloud auth login
docker push us-west2-docker.pkg.dev/gcp-pathology-poc1/pathcloud/dicom-proxy-gcp:0.0.1
```

### Deploy Container to Cloud Run

Googles recommendation is int(os.cpu_count() \* 3.4) for the GUNICORN_WORKERS variable.

### Private with authenticated users

```sh
gcloud run deploy dicom-proxy-gcp-private01 \
--image us-west2-docker.pkg.dev/gcp-pathology-poc1/pathcloud/dicom-proxy-gcp:0.0.1 \
--region=us-west2 --project=gcp-pathology-poc1 \
--port=8080 --allow-unauthenticated \
--memory 8G --cpu 8 --execution-environment=gen2 \
--cpu-boost \
--use-http2 \
--session-affinity \
--min-instances=1 --max-instances=100 --timeout=300 --concurrency=40 \
--set-env-vars "ORIGINS=*" \
--set-env-vars "GUNICORN_WORKERS=24" \
--set-env-vars "VALIDATE_IAP=false" \
--set-env-vars "JWT_AUDIENCE=/projects/1053568465268/global/backendServices/1470682154844812331" \
--set-env-vars "URL_PATH_PREFIX=/private01" \
--set-env-vars "API_PORT_FLG=8080" \
--set-env-vars "GUNICORN_BIND=0.0.0.0:8080" \
--set-env-vars "ENABLE_FAKE_EMAIL=true" \
--set-env-vars "DEFAULT_DICOM_STORE_API_VERSION=v1beta1" \
--set-env-vars "ENABLE_APPLICATION_DEFAULT_CREDENTIALS=false" \
--set-env-vars "ENABLE_DEBUG_FUNCTION_TIMING=true" \
--set-env-vars "REDIS_CACHE_HOST_IP=10.158.57.91" \
--set-env-vars "REDIS_CACHE_HOST_PORT=6379" \
--set-env-vars "CLOUD_OPS_LOG_NAME=dicom-proxy-gcp-private" \
--service-account=dicom-web-proxy-public@gcp-pathology-poc1.iam.gserviceaccount.com \
--network=default \
--subnet=default \
--vpc-egress=private-ranges-only \
--execution-environment=gen2 \
--region=us-west2 \
--project=gcp-pathology-poc1 \
 && gcloud run services update-traffic dicom-proxy-gcp-private01 --to-latest
```

#### Public with no authentication

```sh
gcloud run deploy dicom-proxy-public01 \
--image us-west2-docker.pkg.dev/gcp-pathology-poc1/pathcloud/dicom-proxy-gcp:0.0.1 \
--region=us-west2 --project=gcp-pathology-poc1 \
--port=8080 --allow-unauthenticated \
--memory 8G --cpu 8 --execution-environment=gen2 \
--cpu-boost \
--use-http2 \
--session-affinity \
--min-instances=1 --max-instances=100 --timeout=300 --concurrency=40 \
--set-env-vars "ORIGINS=*" \
--set-env-vars "GUNICORN_WORKERS=24" \
--set-env-vars "VALIDATE_IAP=false" \
--set-env-vars "JWT_AUDIENCE=/projects/1053568465268/global/backendServices/1470682154844812331" \
--set-env-vars "URL_PATH_PREFIX=/dicom-public01" \
--set-env-vars "API_PORT_FLG=8080" \
--set-env-vars "GUNICORN_BIND=0.0.0.0:8080" \
--set-env-vars "ENABLE_FAKE_EMAIL=true" \
--set-env-vars "DEFAULT_DICOM_STORE_API_VERSION=v1beta1" \
--set-env-vars "ENABLE_APPLICATION_DEFAULT_CREDENTIALS=true" \
--set-env-vars "ENABLE_DEBUG_FUNCTION_TIMING=true" \
--set-env-vars "REDIS_CACHE_HOST_IP=10.158.57.91" \
--set-env-vars "REDIS_CACHE_HOST_PORT=6379" \
--set-env-vars "CLOUD_OPS_LOG_NAME=dicom-public01-logs" \
--service-account=dicom-web-proxy-public@gcp-pathology-poc1.iam.gserviceaccount.com \
--network=default \
--subnet=default \
--vpc-egress=private-ranges-only \
--execution-environment=gen2 \
--region=us-west2 \
--project=gcp-pathology-poc1 \
 && gcloud run services update-traffic dicom-proxy-public01 --to-latest
```

## Setup IAP for DICOM Proxy

Enable IAP for each service (using `OAUTH_CLIENT_ID` and `OAUTH_CLIENT_SECRET`
from [Step 2.1](#step2.1))

```sh
gcloud iap web enable --resource-type=backend-services \
--oauth2-client-id=${OAUTH_CLIENT_ID?} \
--oauth2-client-secret=${OAUTH_CLIENT_SECRET?} \
--service=${DICOM_PROXY_SERVICE_ID?}


gcloud iap web enable --resource-type=backend-services \
--oauth2-client-id=1053568465268-t2coh1p3ke4lrhu6o042squicec9toed.apps.googleusercontent.com \
--oauth2-client-secret=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 \
--service=${DICOM_PROXY_SERVICE_ID?}
```

## Notes on Deployment without IAP (02/13/2025)

**Background:** have not been able to get a GCP DICOM_PROXY to work with IAP when hosted in Cloud Run. Believe it is something to do with how Cloud Run handles HTTP requests before passing them into the container. (https://cloud.google.com/iap/docs/enabling-cloud-run)

**New Configuration:**

- Disable IAP for Load Balancer Backend routing traffic to Cloud Run Container
  - Rely on user authentication against the ACL of the specific DICOM Store.
- Disable IAP on DICOM_PROXY env config
- Disable "Use_IAP" flag in Viewr-OHIF and Viewer-Clinical config file
  - Causes viewer to use standard OAuth BEARER token instead of JWT
- Point Viewer config at DICOM_PROXY w/o IAP

**Issues:**

- Confirm Access Control List is providing expected protection.
