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
  const urlPathSeg = slideId.split('/studies');
  if (urlPathSeg.length < 1) {
    return slideId;
  }
  const serverPath = urlPathSeg[0];
  for (const value of Object.values(DICOM_STORE)) {
    if (value.endsWith(serverPath)) {
      return `${value}/studies${urlPathSeg[1] ? urlPathSeg[1] : ''}`;
    }
  }
  return slideId;
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
