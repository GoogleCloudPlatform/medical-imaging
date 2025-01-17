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

/** Fhir search results */
export declare interface FhirSearchResults {
  entry: DiagnosticReport[];
  link: Array<{
    relation: string,
    url: string,
  }>;
  resourceType: string;
  total: number;
  type: string;
}

/** Modified Diagnostic Report Resource Text from FHIR store. */
export declare interface DiagnosticReportResourceText {
  div: string;
  status: string;
  sanitizedHtml?: string;
  tagsRemovedSanitized?: string;
  snippet?: string;
}

/** Modified Diagnostic Report Resource from FHIR store. */
export declare interface DiagnosticReportResource {
  caseAccessionId?: string;
  code: {
    coding: Array<{code: string, system: string}>,
  };
  id: string;
  identifier?: Array<{value: string}>;
  meta: {
    lastUpdated: string,
  };
  resourceType: string;
  status: string;
  text: DiagnosticReportResourceText;
}

/** Modified Diagnostic Report from FHIR store. */
export declare interface DiagnosticReport {
  fullUrl?: string;
  resource: DiagnosticReportResource;
  search?: {};
}

/** Supported sort operator for searching DiagnosticReports. */
export enum SearchDiagnosticReportSort {
  LAST_MODIFIED_ASC = '_lastUpdated',
  LAST_MODIFIED_DESC = '-_lastUpdated',
}
