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

import {environment} from "../environments/environment";

/**
 * The DicomStores this service communicates with.
 */
export enum DicomStores {
  IMAGE,  // Dicom store that contains the original data assuming with PHI.
  DEID,  // Dicom store that contains only data that has been de-ided.
}

function createDicomStoreObject() {
  return {
    [DicomStores.IMAGE]: environment.IMAGE_DICOM_STORE_BASE_URL,
    [DicomStores.DEID]: environment.IMAGE_DEID_DICOM_STORE_BASE_URL,
  };
}

/**
 * Configure the supported DICOM stores in the app.
 */
export const DICOM_STORE = createDicomStoreObject();

// Match valid DICOMweb study, series, or instance resource paths.
const UID_SEGMENT = '[A-Za-z0-9._-]+';
const DICOM_RESOURCE_PATH_REGEX = new RegExp(
    `^\\/studies\\/${UID_SEGMENT}(?:\\/series\\/${UID_SEGMENT}(?:\\/instances\\/${UID_SEGMENT})?)?$`);

// Match valid outbound DICOMweb QIDO-RS, WADO-RS, and STOW-RS request paths.
const INSTANCE_SUBPATH =
    '(?:\\/(?:metadata|rendered|frames\\/[0-9,]+(?:\\/rendered)?))?';
const SERIES_SUBPATH =
    `(?:\\/(?:metadata|rendered|instances(?:\\/${UID_SEGMENT}${INSTANCE_SUBPATH})?))?`;
const STUDY_SUBPATH =
    `(?:\\/(?:metadata|rendered|series(?:\\/${UID_SEGMENT}${SERIES_SUBPATH})?|instances(?:\\/${UID_SEGMENT}${INSTANCE_SUBPATH})?))?`;
const DICOM_REQUEST_PATH_REGEX = new RegExp(
    `^\\/(?:studies(?:\\/${UID_SEGMENT}${STUDY_SUBPATH})?|series|instances)$`);

// Detect raw or percent-encoded path traversal segments and unsafe characters.
const UNSAFE_PATH_TOKEN_REGEX = /(?:^|\/)\.{1,2}(?:\/|$)|%2e|%2f|%5c|\\/i;

// Match URLs that begin with an explicit HTTP or HTTPS scheme.
const HTTP_SCHEME_REGEX = /^https?:\/\//i;

// Match any URI that begins with an explicit scheme prefix.
const ANY_URI_SCHEME_REGEX = /^[a-zA-Z][a-zA-Z0-9+.-]*:/;

// Resolve fallback origin for parsing relative store paths.
function getFallbackOrigin(): string {
  if (typeof window !== 'undefined' && window.location?.origin) {
    return window.location.origin;
  }
  return 'https://localhost';
}

// Collect non-empty configured DICOM store base URLs.
export function getConfiguredDicomStores(): string[] {
  const imageStore =
      DICOM_STORE[DicomStores.IMAGE] || environment.IMAGE_DICOM_STORE_BASE_URL;
  const deidStore =
      DICOM_STORE[DicomStores.DEID] ||
      environment.IMAGE_DEID_DICOM_STORE_BASE_URL;
  return [imageStore, deidStore]
      .map((store) => (store ?? '').trim())
      .filter((store) => store.length > 0);
}

// Return configured annotations DICOM store base URL if non-empty.
function getConfiguredAnnotationStore(): string {
  const base = (environment.ANNOTATIONS_DICOM_STORE_BASE_URL ?? '').trim();
  if (!base) {
    return '';
  }
  const parent = (environment.ANNOTATIONS_DICOM_STORE_PARENT ?? '').trim();
  return `${base}${parent}`;
}

// Collect all non-empty configured backend base URLs allowed for auth tokens.
function getConfiguredBackendBaseUrls(): string[] {
  const candidates = [
    ...getConfiguredDicomStores(),
    getConfiguredAnnotationStore(),
    (environment.FHIR_STORE_BASE_URL ?? '').trim(),
    (environment.ORCHESTRATOR_BASE_URL ?? '').trim(),
  ];
  return candidates.filter((url) => url.length > 0);
}

// Check whether a raw URL path contains traversal tokens or unsafe characters.
function hasUnsafePathCharacters(rawPath: string): boolean {
  if (!rawPath || /\s/.test(rawPath)) {
    return true;
  }
  return UNSAFE_PATH_TOKEN_REGEX.test(rawPath);
}

// Verify candidate and store URLs use compatible HTTP(S) or relative forms.
function isSchemeFormCompatible(
    candidateUrl: string, storeBaseUrl: string): boolean {
  const storeIsAbsolute = HTTP_SCHEME_REGEX.test(storeBaseUrl);
  const candidateIsAbsolute = HTTP_SCHEME_REGEX.test(candidateUrl);
  if (storeIsAbsolute) {
    return candidateIsAbsolute;
  }
  if (!storeBaseUrl.startsWith('/')) {
    return false;
  }
  return candidateUrl.startsWith('/') || candidateIsAbsolute;
}

// Parse URLs and return candidate pathname relative to store base URL.
function parseAndValidateUrlMatch(
    candidateUrl: string, storeBaseUrl: string): string | null {
  const trimmedCandidate = candidateUrl.trim();
  const trimmedStore = storeBaseUrl.trim();
  if (!isSchemeFormCompatible(trimmedCandidate, trimmedStore)) {
    return null;
  }
  try {
    const baseOrigin = getFallbackOrigin();
    const parsedCandidate = new URL(trimmedCandidate, baseOrigin);
    const parsedStore = new URL(trimmedStore, baseOrigin);
    if (!verifyOriginAndCredentials(parsedCandidate, parsedStore)) {
      return null;
    }
    const storePath = parsedStore.pathname.replace(/\/+$/, '');
    const candidatePath = parsedCandidate.pathname.replace(/\/+$/, '');
    if (candidatePath === storePath) {
      return '';
    }
    if (!candidatePath.startsWith(`${storePath}/`)) {
      return null;
    }
    return candidatePath.slice(storePath.length);
  } catch {
    return null;
  }
}

// Verify protocol, origin, and absence of embedded credentials or hashes.
function verifyOriginAndCredentials(
    parsedCandidate: URL, parsedStore: URL): boolean {
  const isHttpProtocol =
      parsedStore.protocol === 'https:' || parsedStore.protocol === 'http:';
  if (!isHttpProtocol || parsedCandidate.protocol !== parsedStore.protocol) {
    return false;
  }
  if (parsedCandidate.username || parsedCandidate.password ||
      parsedStore.username || parsedStore.password) {
    return false;
  }
  if (parsedCandidate.hash || parsedStore.hash) {
    return false;
  }
  return parsedCandidate.origin === parsedStore.origin &&
      parsedCandidate.host === parsedStore.host;
}

// Check whether a slide or study ID belongs to a specific DICOM store.
function isValidDicomStoreResourceUrl(
    id: string, storeBaseUrl: string): boolean {
  if (!id || id.includes(',') || id.includes('?') || id.includes('#')) {
    return false;
  }
  if (hasUnsafePathCharacters(id) || hasUnsafePathCharacters(storeBaseUrl)) {
    return false;
  }
  const relativePath = parseAndValidateUrlMatch(id, storeBaseUrl);
  if (!relativePath) {
    return false;
  }
  return DICOM_RESOURCE_PATH_REGEX.test(relativePath);
}

// Check whether a configured store base URL ends with the partial server path.
function matchesStoreSuffix(
    normalizedStore: string, serverPath: string): boolean {
  if (!serverPath) {
    return true;
  }
  return normalizedStore === serverPath ||
      normalizedStore.endsWith(`/${serverPath}`);
}

/**
 * Reset dicom store configuration based on an update enviroument.
 */
export function resetDicomStores() {
  Object.assign(DICOM_STORE, createDicomStoreObject());
}

/**
 * Tests is a slide is in the de-id dicom store.
 */
export function isSlideDeId(slideId: string) {
  const deidStore =
      DICOM_STORE[DicomStores.DEID] ||
      environment.IMAGE_DEID_DICOM_STORE_BASE_URL;
  if (!deidStore || !deidStore.trim()) {
    return false;
  }
  return isValidDicomStoreResourceUrl(slideId, deidStore);
}

/**
 * Adds the appropriate DICOM store prefix to a partial slide id.
 *
 * @param slideId The partial slide id, which may be missing the DICOM store
 *     prefix.
 * @returns The complete slide id with the DICOM store prefix.
 */
export function addDicomStorePrefixIfMissing(slideId: string) {
  if (!slideId || slideId.includes(',') || ANY_URI_SCHEME_REGEX.test(slideId)) {
    return slideId;
  }
  const normalizedInput =
      slideId.startsWith('studies/') ? `/${slideId}` : slideId;
  const urlPathSeg = normalizedInput.split('/studies/');
  if (urlPathSeg.length < 2) {
    return slideId;
  }
  const serverPath = urlPathSeg[0].replace(/^\/+|\/+$/g, '');
  const studySuffix = urlPathSeg.slice(1).join('/studies/');
  for (const store of getConfiguredDicomStores()) {
    const normalizedStore = store.replace(/\/+$/, '');
    if (matchesStoreSuffix(normalizedStore, serverPath)) {
      return `${normalizedStore}/studies/${studySuffix}`;
    }
  }
  return slideId;
}

/**
 * Looks up dicom store from dicom id. The prefix of an id should match the
 * store parent.
 */
export function dicomIdToDicomStore(id: string) {
  for (const store of getConfiguredDicomStores()) {
    if (isValidDicomStoreResourceUrl(id, store)) {
      return store;
    }
  }
  throw new Error(`Invalid Dicom id prefix ${id}`);
}

/**
 * Validates that an outbound DICOMweb HTTP request URL targets a configured
 * DICOM store and a valid DICOMweb endpoint path.
 */
export function isAllowedDicomWebRequestUrl(url: string): boolean {
  if (!url || url.includes('#')) {
    return false;
  }
  const pathPart = url.split('?')[0];
  if (hasUnsafePathCharacters(pathPart)) {
    return false;
  }
  const allowedStores = [
    ...getConfiguredDicomStores(),
    getConfiguredAnnotationStore(),
  ].filter((store) => store.length > 0);
  for (const store of allowedStores) {
    if (hasUnsafePathCharacters(store)) {
      continue;
    }
    const relativePath = parseAndValidateUrlMatch(url, store);
    if (relativePath && DICOM_REQUEST_PATH_REGEX.test(relativePath)) {
      return true;
    }
  }
  return false;
}

/**
 * Validates that an outbound HTTP request URL targets a configured backend
 * service before attaching an OAuth bearer token.
 */
export function isAllowedAuthenticatedRequestUrl(url: string): boolean {
  if (!url || url.includes('#')) {
    return false;
  }
  const pathPart = url.split('?')[0];
  if (hasUnsafePathCharacters(pathPart)) {
    return false;
  }
  for (const baseUrl of getConfiguredBackendBaseUrls()) {
    if (hasUnsafePathCharacters(baseUrl)) {
      continue;
    }
    const relativePath = parseAndValidateUrlMatch(url, baseUrl);
    if (relativePath !== null) {
      return true;
    }
  }
  return false;
}
