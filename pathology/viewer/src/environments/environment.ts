/**
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {EnvironmentSettings, defaultEnvironment} from './environment-types';

/**
 * Environment settings for the Pathology Image Library.
 */
export const environment: EnvironmentSettings = {
  ...defaultEnvironment,
  // Setup your dicomstore base url.
  'IMAGE_DICOM_STORE_BASE_URL': '',
  'OAUTH_CLIENT_ID': '',
  'APP_BASE_SERVER_PATH': '',
  'USE_HASH_LOCATION_STRATEGY': false,
  //// (optional) Annotations.
  'ANNOTATIONS_DICOM_STORE_BASE_URL': '',
  'ENABLE_ANNOTATIONS': true,
  'ENABLE_ANNOTATION_WRITING': true,
  'ANNOTATION_HASH_STORED_USER_EMAIL': false,
};

environment.ENABLE_ANNOTATION_WRITING =
    environment.ENABLE_ANNOTATIONS && environment.ENABLE_ANNOTATION_WRITING;
