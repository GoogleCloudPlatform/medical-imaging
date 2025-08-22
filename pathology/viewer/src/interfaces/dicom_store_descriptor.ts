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

import { environment } from "../environments/environment";

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
  return Boolean(
    environment.IMAGE_DEID_DICOM_STORE_BASE_URL &&
      slideId.startsWith(environment.IMAGE_DEID_DICOM_STORE_BASE_URL));
}

/**
 * Adds the appropriate DICOM store prefix to a partial slide id.
 *
 * @param slideId The partial slide id, which may be missing the DICOM store
 *     prefix.
 * @returns The complete slide id with the DICOM store prefix.
 */
export function addDicomStorePrefixIfMissing(slideId: string) {
  if (!slideId) return slideId;

  // Normalize configured store bases (trim trailing slashes)
  const stores = Object.values(DICOM_STORE)
      .filter((s): s is string => !!s)
      .map((s) => s.replace(/\/+$/, ''));

  // If already fully-qualified, leave unchanged
  for (const base of stores) {
    if (slideId === base || slideId.startsWith(base + '/')) {
      return slideId;
    }
  }

  // Only process if "/studies" exists in the path
  const parts = slideId.split('/studies');
  if (parts.length < 2) {
    return slideId;
  }

  // The server-relative prefix before the first "/studies"
  const serverPath = parts[0].replace(/\/+$/, '');
  if (!serverPath) {
    return slideId;  // avoid accidental matches on empty prefix
  }

  // Choose the longest configured base that ends with serverPath
  let chosen: string|undefined;
  let bestLen = -1;
  for (const base of stores) {
    if (base.endsWith(serverPath) && base.length > bestLen) {
      chosen = base;
      bestLen = base.length;
    }
  }
  if (!chosen) return slideId;

  // Preserve everything after the first "/studies" without duplicating slashes
  const tail = parts.slice(1).join('/studies').replace(/^\/+/, '');
  return `${chosen}/studies${tail ? '/' + tail : ''}`;
}

/**
 * Looks up dicom store from dicom id. The prefix of an id should match the
 * store parent.
 */
export function dicomIdToDicomStore(id: string) {
  for (const store of Object.values(DICOM_STORE)) {
    if ((id.startsWith(store))) {
      return store;
    }
  }
  throw new Error(`Invalid Dicom id prefix ${id}`);
}
