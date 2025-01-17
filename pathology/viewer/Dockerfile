# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This is an example of how one could deploy this viewer as a docker image.
# Steps:
# 1. docker build -t path-viewer .
# 2. docker run -p 5432:5432 path-viewer
# 3. http://localhost:5432


FROM node:18 AS builder
WORKDIR /app
COPY . .
RUN npm install
RUN npm run build

FROM nginx:latest
COPY --from=builder /app/dist/viewer/ /usr/share/nginx/html/
COPY nginx.conf /etc/nginx/nginx.conf
RUN mkdir -p /var/cache/nginx/client_temp /var/cache/nginx/proxy_temp /var/cache/nginx/fastcgi_temp /var/cache/nginx/uwsgi_temp /var/cache/nginx/scgi_temp /tmp/nginx
RUN chmod -R 777 /var/cache/nginx /tmp/nginx
EXPOSE 5432
CMD ["nginx", "-g", "daemon off; pid /tmp/nginx/nginx.pid;"]
