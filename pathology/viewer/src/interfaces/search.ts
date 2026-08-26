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


import {DiagnosticReport, DiagnosticReportResourceText} from './fhir_store';
import {Case, Slide} from './hierarchy_descriptor';
import {PathologySlide} from './slide_descriptor';

/** Interface for case widget */
export interface CaseWidget {
  id: string;
  slides: PathologySlide[];
  caseDate: string;
  diagnosticReportDate?: string;
  text?: DiagnosticReportResourceText;
  hasLinking?: boolean;
  diagnosticReport?: DiagnosticReport;
  caseInfo?: Case;
}

/** Interface encapsulating search params sent to subscribers by search() */
export interface SearchParams {
  searchText: string;
}


/** Types of search available */
export enum SearchType {
  CASE_ID = 'caseId',
  PATIENT_ID = 'patientId',
}

/** DICOM web slide metadatas keyed by their DPAS record IDs. */
export declare interface RecordIdToSlideIds {
  recordId: string;
  slideIds: Slide[];
}
