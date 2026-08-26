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
 * Path components for a DICOMweb URL.
 */
export declare interface DicomWebPath {
  studyUID?: string;
  seriesUID?: string;
  instanceUID?: string;
}

/**
 * Query parameters for a DICOMweb URL.
 */
export declare interface DicomWebQueryParams {
  includefield?: string;
  limit?: number | string;
  offset?: number | string;
  modality?: string;
  SOPInstanceUID?: string;
  AccessionNumber?: string;
  PatientID?: string;
}

/**
 * Configuration for DICOMweb URLs resources.
 */
export interface DicomWebUrlConfig {
  baseUrl?: string;
  path: DicomWebPath;
  resource?: string;  // examples series|metadata|rendered|frames|...
  queryParams?: DicomWebQueryParams;
}

/**
 * Parses a DICOMweb path URL into its components: baseURL (optional), studyUID,
 * seriesUID, and instanceUID.
 * https://www.dicomstandard.org/using/dicomweb/query-qido-rs
 */
export function parseDICOMwebUrl(url: string): DicomWebUrlConfig {
  // This pattern allows for optional groups for the base URL, studyUID,
  // seriesUID, and instanceUID
  const baseUrlWhenResourcePresentRegex = /^([^?]*?)\/(?:studies|series|instances|rs)/;  // *? -> non-greedy
  const studyUIDRegex = /(?:^|\/)studies\/([0-9.]+)/;
  const seriesUIDRegex = /(?:^|\/)series\/([0-9.]+)/;
  const instanceUIDRegex = /(?:^|\/)instances\/([0-9.]+)/;
  const queryParamsRegex = /\?(.*)$/;

  // Match each part using the regex patterns
  const baseUrlMatch = url.match(baseUrlWhenResourcePresentRegex);
  const studyUIDMatch = url.match(studyUIDRegex);
  const seriesUIDMatch = url.match(seriesUIDRegex);
  const instanceUIDMatch = url.match(instanceUIDRegex);
  const queryParamsMatch = url.match(queryParamsRegex);

  // Extract the matched parts or set them as null if not found
  const baseUrl =
    baseUrlMatch ? baseUrlMatch[1] : url;
  const studyUID = studyUIDMatch ? studyUIDMatch[1] : undefined;
  const seriesUID = seriesUIDMatch ? seriesUIDMatch[1] : undefined;
  const instanceUID = instanceUIDMatch ? instanceUIDMatch[1] : undefined;
  const queryParamsString = queryParamsMatch ? queryParamsMatch[1] : undefined;

  // Parse query params into an object
  const queryParams = queryParamsString ?
    queryParamsString.split('&').reduce(
      (acc: { [k: string]: string }, param) => {
        const [key, value] = param.split('=');
        acc[key] = value;
        return acc;
      },
      {}) :
    undefined;

  const path = {
    ...(studyUID && { studyUID }),
    ...(seriesUID && { seriesUID }),
    ...(instanceUID && { instanceUID })
  };

  // Return an object with the captured values, omitting unavailable
  return {
    ...(baseUrl && { baseUrl }),
    ...(path && { path }),
    ...(queryParams && { queryParams })
  };
}

/**
 * Constructs a DICOMweb Path using the provided configuration.
 */
export function constructDicomWebPath(config: DicomWebPath): string {
  let url = '';
  if (config.studyUID) {
    url += `/studies/${config.studyUID}`;
  }
  if (config.seriesUID) {
    url += `/series/${config.seriesUID}`;
  }
  if (config.instanceUID) {
    url += `/instances/${config.instanceUID}`;
  }
  return url;
}

/**
 * Constructs a DICOMweb URL using the provided configuration.
 * The URL is built by appending the study, series, and instance identifiers
 * to the base URL as needed. If an identifier is not provided, the URL is
 * constructed up to the last provided level of the DICOM data hierarchy.
 */
export function constructDicomWebUrl(config: DicomWebUrlConfig): string {
  let url = (config.baseUrl || '');
  url += constructDicomWebPath(config.path);
  if (config.resource) {
    url += `/${config.resource}`;
  }

  // Build the query parameters string if queryParams is provided
  if (config.queryParams) {
    const queryParamsArray =
      Object.entries(config.queryParams)
        .map(
          ([key, value]) =>
            `${encodeURIComponent(key)}=${encodeURIComponent(value)}`);
    const queryString = queryParamsArray.join('&');
    if (queryString) {
      url += `?${queryString}`;
    }
  }
  return url;
}