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

import { DicomWebQueryParams } from './dicomweb';
import { PathologySlide } from './slide_descriptor';
/**
 * Pathology medical organization and hierarchy.
 */

/**
 * Representation of data about a given case (study).
 */
export interface Case {
  accessionNumber: string;
  caseId: string;  // dicom study path.
  date: string;
  failedToLoad?: boolean;
  slides: PathologySlide[];
}

/**
 * Representation of data about the patient whose cases are being shown.
 */
export interface Patient {
  latestCaseAccessionNumber?: string;
  latestCaseDate?: string;
  name?: string;
  patientId?: string;
}

/**
 * Representation of data about a given slide (series).
 */
export interface Slide {
  slideId: string;         // dicom series path.
  slideRecordId?: string;  // the ID on the slide label.
}

/**
 * Metadata associated with a record ID type.
 */
export declare interface RecordIdMetaType {
  dicomWebSearchToken?: keyof DicomWebQueryParams;
  displayText: string;
}

/**
 * Type of const RecordId map.
 */
export declare interface RecordIdMetaMap {
  caseId: RecordIdMetaType;
  patientId: RecordIdMetaType;
  slideId: RecordIdMetaType;
}

/**
 * Type of supported record IDs.
 */
export const RECORD_ID_TYPE_META: RecordIdMetaMap = {
  caseId: {displayText: 'Case Number', dicomWebSearchToken: 'AccessionNumber'},
  patientId: {displayText: 'Patient ID', dicomWebSearchToken: 'PatientID'},
  slideId: {displayText: 'Slide ID'}
};

/**
 * Type of supported record IDs.
 */
export type RecordIdType = keyof RecordIdMetaMap;