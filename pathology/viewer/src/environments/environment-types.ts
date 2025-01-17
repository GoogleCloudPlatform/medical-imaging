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

/**
 * Environment settings for the application. See README.md for more details.
 */
export interface EnvironmentSettings {
  // ---------------------------------------------------------------------------
  // 1) Configuring the tissue viewer
  // ---------------------------------------------------------------------------

  /**
   * Name of the application shown in the viewer’s UI.
   */
  VIEWER_APP_NAME: string;

  /**
   * (REQUIRED) Base DICOM store URL for retrieving images (e.g. Whole Slide
   * Images). Example for Google Cloud:
   *   https://healthcare.googleapis.com/v1/projects/PROJECT_ID/locations/LOCATION/datasets/DATASET_ID/dicomStores/STORE_ID/dicomWeb
   */
  IMAGE_DICOM_STORE_BASE_URL: string;

  /**
   * Enables proprietary server-side features (e.g., color correction, layering)
   * when using the Pathology DICOM Proxy. See README.md on how to enable
   * server.
   */
  ENABLE_SERVER_INTERPOLATION: boolean;

  // ---------------------------------------------------------------------------
  // 2) Authentication
  // ---------------------------------------------------------------------------

  /**
   * OAuth2 client ID for the viewer (if using authenticated DICOM/FHIR
   * endpoints).
   */
  OAUTH_CLIENT_ID: string;

  /**
   * OAuth scopes for authenticating the user.
   * Typical default: "https://www.googleapis.com/auth/cloud-healthcare email"
   */
  OAUTH_SCOPES: string;

  // ---------------------------------------------------------------------------
  // 3) Configuring Angular Routing
  // ---------------------------------------------------------------------------

  /**
   * Base path where the viewer is served, e.g., "/pathology".
   * Useful if deploying to a subdirectory on a server.
   */
  APP_BASE_SERVER_PATH: string;

  /**
   * If true, the path after "#" is used for client-side navigation.
   * E.g., example.com/#/viewer
   */
  USE_HASH_LOCATION_STRATEGY: boolean;

  // ---------------------------------------------------------------------------
  // 4) Configuration of Annotations
  // ---------------------------------------------------------------------------

  /**
   * DICOM store URL for storing and reading annotations.
   * Same format as IMAGE_DICOM_STORE_BASE_URL.
   */
  ANNOTATIONS_DICOM_STORE_BASE_URL: string;

  /**
   * If true, enables annotation features in the UI (viewing, listing).
   */
  ENABLE_ANNOTATIONS: boolean;

  /**
   * If true, allows writing (creating, editing) of DICOM annotations.
   * Otherwise, annotations are view-only.
   */
  ENABLE_ANNOTATION_WRITING: boolean;

  /**
   * If true, stores hashed user emails in the annotation data
   * for privacy instead of plain-text emails.
   */
  ANNOTATION_HASH_STORED_USER_EMAIL: boolean;

  /**
   * Identifier used in the DICOM standard to uniquely identify proprietary data
   * added to stored DICOM annotations.
   */
  DICOM_GUID_PREFIX: string;

  // ---------------------------------------------------------------------------
  // 5) Pathology FHIR Search
  // ---------------------------------------------------------------------------

  /**
   * Base FHIR store URL for searching DiagnosticReports (FHIR).
   * Leave empty to disable FHIR search.
   */
  FHIR_STORE_BASE_URL: string;

  /**
   * Extra query parameters appended to FHIR search requests.
   * E.g., "&category=pat".
   */
  FHIR_STORE_SEARCH_QUERY_PARAMETERS: string;

  /**
   * If true, forces search queries to uppercase (useful for case-sensitive
   * backends).
   */
  SEARCH_UPPERCASE_ONLY: boolean;

  // ---------------------------------------------------------------------------
  // 6) Additional Cohort related settings requiring a special server
  // ---------------------------------------------------------------------------
  ID_DELIMITER: string;
  ID_VALIDATOR: string;
  ORCHESTRATOR_BASE_URL: string;
  IMAGE_DEID_DICOM_STORE_BASE_URL: string;
  ENABLE_COHORTS: boolean;
  ANNOTATIONS_DICOM_STORE_PARENT: string;

  /**
   * (Deprecated) Use FHIR_STORE_BASE_URL instead.
   */
  FHIR_STORE_PARENT: string;
}


/**
 * Default environment settings for the application. See above and README.md for
 * more details.
 */
export const defaultEnvironment: EnvironmentSettings = {
  //// Core settings.
  'VIEWER_APP_NAME': 'Pathology Image Library',
  'IMAGE_DICOM_STORE_BASE_URL': '',  // (required)
  'OAUTH_CLIENT_ID': '',
  'OAUTH_SCOPES': 'https://www.googleapis.com/auth/cloud-healthcare email',
  'APP_BASE_SERVER_PATH': '',
  'USE_HASH_LOCATION_STRATEGY': false,
  'ENABLE_SERVER_INTERPOLATION': false,  // Requires Dicom Proxy.
  'ANNOTATIONS_DICOM_STORE_BASE_URL': '',
  'ENABLE_ANNOTATIONS': true,
  'ENABLE_ANNOTATION_WRITING': true,
  // Identifier used in the DICOM standard to uniquely identify proprietary data
  // elements or extensions specific to Google, Inc. within medical imaging
  // applications.
  'DICOM_GUID_PREFIX': '1.3.6.1.4.1.11129.5.7.0.1',
  'ANNOTATION_HASH_STORED_USER_EMAIL': false,
  'FHIR_STORE_BASE_URL': '',
  'FHIR_STORE_SEARCH_QUERY_PARAMETERS': '\'\'',  // Emptry string.
  'SEARCH_UPPERCASE_ONLY': false,
  //// Cohorts, unavailable.
  'ORCHESTRATOR_BASE_URL': '',
  'ENABLE_COHORTS': false,
  'ID_DELIMITER': ',;\\s\\t\\n\\r',  // Regex for delimiting IDs
  'ID_VALIDATOR': '\\w\\d-',         // Regex for valid characters in IDs
  'IMAGE_DEID_DICOM_STORE_BASE_URL': '',
  // deprecated include parent in base url.
  'FHIR_STORE_PARENT': '',
  'ANNOTATIONS_DICOM_STORE_PARENT': '',
};
