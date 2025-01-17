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

import {PathologySlide} from './slide_descriptor';
import {PathologyUserAccess} from './users';

/**
 * FieldMask
 */
export type FieldMask = string;

/**
 * Status
 */
export interface Status {
  code: number;
  message: string;
  details: unknown[];  // Assuming you have the necessary import
}

/**
 * PathologyCohortAccess
 */
export type PathologyCohortAccess = 'PATHOLOGY_COHORT_ACCESS_UNSPECIFIED'|
    'PATHOLOGY_COHORT_ACCESS_RESTRICTED'|'PATHOLOGY_COHORT_ACCESS_OPEN_EDIT'|
    'PATHOLOGY_COHORT_ACCESS_OPEN_VIEW_ONLY';

/**
 * Enum for PathologyCohortAccess
 */
declare enum NumericPathologyCohortAccess {
  PATHOLOGY_COHORT_ACCESS_UNSPECIFIED = 0,
  PATHOLOGY_COHORT_ACCESS_RESTRICTED = 1,
  PATHOLOGY_COHORT_ACCESS_OPEN_EDIT = 2,
  PATHOLOGY_COHORT_ACCESS_OPEN_VIEW_ONLY = 3,
}

/**
 * PathologyCohortBehaviorConstraints
 */
export type PathologyCohortBehaviorConstraints =
    'PATHOLOGY_COHORT_BEHAVIOR_CONSTRAINTS_UNSPECIFIED'|
    'PATHOLOGY_COHORT_BEHAVIOR_CONSTRAINTS_FULLY_FUNCTIONAL'|
    'PATHOLOGY_COHORT_BEHAVIOR_CONSTRAINTS_CANNOT_BE_EXPORTED';
/**
 * Enum for PathologyCohortBehaviorConstraints
 */
declare enum NumericPathologyCohortBehaviorConstraints {
  PATHOLOGY_COHORT_BEHAVIOR_CONSTRAINTS_UNSPECIFIED = 0,
  PATHOLOGY_COHORT_BEHAVIOR_CONSTRAINTS_FULLY_FUNCTIONAL = 1,
  PATHOLOGY_COHORT_BEHAVIOR_CONSTRAINTS_CANNOT_BE_EXPORTED = 2,
}

/**
 * PathologyCohortLifecycleStage
 */
export type PathologyCohortLifecycleStage =
    'PATHOLOGY_COHORT_LIFECYCLE_STAGE_UNSPECIFIED'|
    'PATHOLOGY_COHORT_LIFECYCLE_STAGE_ACTIVE'|
    'PATHOLOGY_COHORT_LIFECYCLE_STAGE_SUSPENDED'|
    'PATHOLOGY_COHORT_LIFECYCLE_STAGE_UNAVAILABLE';

declare enum NumericPathologyCohortLifecycleStage {
  PATHOLOGY_COHORT_LIFECYCLE_STAGE_UNSPECIFIED = 0,
  PATHOLOGY_COHORT_LIFECYCLE_STAGE_ACTIVE = 1,
  PATHOLOGY_COHORT_LIFECYCLE_STAGE_SUSPENDED = 2,
  PATHOLOGY_COHORT_LIFECYCLE_STAGE_UNAVAILABLE = 3,
}

/**
 * PathologyCohortView
 */
export type PathologyCohortView = 'PATHOLOGY_COHORT_VIEW_UNSPECIFIED'|
    'PATHOLOGY_COHORT_VIEW_METADATA_ONLY'|'PATHOLOGY_COHORT_VIEW_FULL';
/**
 * Enum for PathologyCohortView
 */
declare enum NumericPathologyCohortView {
  PATHOLOGY_COHORT_VIEW_UNSPECIFIED = 0,
  PATHOLOGY_COHORT_VIEW_METADATA_ONLY = 1,
  PATHOLOGY_COHORT_VIEW_FULL = 2,
}

/**
 * PathologyCohortMetadata
 */
export interface PathologyCohortMetadata {
  displayName?: string;
  description?: string;
  cohortStage?: PathologyCohortLifecycleStage;
  isDeid?: boolean;
  cohortBehaviorConstraints?: PathologyCohortBehaviorConstraints;
  updateTime?: string;
  cohortAccess?: PathologyCohortAccess;
  expireTime?: string;
}

/**
 * PathologyCohort
 */
export interface PathologyCohort {
  name?: string;
  cohortMetadata?: PathologyCohortMetadata;
  slides?: PathologySlide[];
  userAccess?: PathologyUserAccess[];
}

/**
 * CreatePathologyCohortRequest
 */
export interface CreatePathologyCohortRequest {
  parent?: string;
  pathologyCohort?: PathologyCohort;
}

/**
 * DeletePathologyCohortRequest
 */
export interface DeletePathologyCohortRequest {
  name?: string;
  /** @deprecated */
  linkToken?: string;
}

/**
 * TransferDeIdPathologyCohortRequest
 */
export interface TransferDeIdPathologyCohortRequest {
  name?: string;
  destDicomImages?: string;
  displayNameTransferred?: string;
  descriptionTransferred?: string;
  notificationEmails?: string[];
  /** @deprecated */
  linkToken?: string;
}

/**
 * TransferDeIdPathologyCohortResponse
 */
export interface TransferDeIdPathologyCohortResponse {
  status?: Status;
}

/**
 * ExportPathologyCohortRequest
 */
export interface ExportPathologyCohortRequest {
  name?: string;
  gcsDestPath?: string;
  notificationEmails?: string[];
  /** @deprecated */
  linkToken?: string;
}

/**
 * ExportPathologyCohortResponse
 */
export interface ExportPathologyCohortResponse {
  status?: Status;
}

/**
 * GetPathologyCohortRequest
 */
export interface GetPathologyCohortRequest {
  name?: string;
  view?: PathologyCohortView;
  /** @deprecated */
  linkToken?: string;
}

/**
 * ListPathologyCohortsRequest
 */
export interface ListPathologyCohortsRequest {
  parent?: string;
  pageSize?: /* int32 */ number;
  pageToken?: string;
  view?: PathologyCohortView;
  filter?: string;
}

/**
 * ListPathologyCohortsResponse
 */
export interface ListPathologyCohortsResponse {
  /** @deprecated */
  ownedPathologyCohorts?: PathologyCohort[];
  /** @deprecated */
  editablePathologyCohorts?: PathologyCohort[];
  /** @deprecated */
  viewOnlyPathologyCohorts?: PathologyCohort[];
  nextPageToken?: string;
  pathologyCohorts?: PathologyCohort[];
}

/**
 * UpdatePathologyCohortRequest
 */
export interface UpdatePathologyCohortRequest {
  pathologyCohort?: PathologyCohort;
  view?: PathologyCohortView;
  updateMask?: FieldMask;
  /** @deprecated */
  linkToken?: string;
}

/**
 * UndeletePathologyCohortRequest
 */
export interface UndeletePathologyCohortRequest {
  name?: string;
  /** @deprecated */
  linkToken?: string;
}

/**
 * CopyPathologyCohortRequest
 */
export interface CopyPathologyCohortRequest {
  name?: string;
  displayNameCopied?: string;
  descriptionCopied?: string;
  /** @deprecated */
  linkToken?: string;
  view?: PathologyCohortView;
}

/**
 * SavePathologyCohortRequest
 */
export interface SavePathologyCohortRequest {
  name?: string;
}

/**
 * SavePathologyCohortResponse
 */
export interface SavePathologyCohortResponse {}
/**
 * SharePathologyCohortRequest
 */
export interface SharePathologyCohortRequest {
  name?: string;
  userAccess?: PathologyUserAccess[];
  cohortAccess?: PathologyCohortAccess;
  view?: PathologyCohortView;
}
/**
 * UnsavePathologyCohortRequest
 */
export interface UnsavePathologyCohortRequest {
  name?: string;
}
/**
 * UnsavePathologyCohortResponse
 */
export interface UnsavePathologyCohortResponse {}
/**
 * CreatePathologySlideRequest
 */
export interface CreatePathologySlideRequest {
  pathologySlide?: PathologySlide;
}
