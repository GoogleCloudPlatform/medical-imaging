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

import ol from 'ol';

/**
 * Side navigation annotation layer information.
 */
export declare interface SideNavLayer extends AnnotationKey {
  feature: ol.Feature;
}

/**
 * Annotation Key, for ol features.
 */
export declare interface AnnotationKey {
  names: string;
  notes: string;
  annotatorId: string;
  index: number;
}

/** Default annotation key for ol features. */
export const DEFAULT_ANNOTATION_KEY: AnnotationKey = {
  names: '',
  notes: '',
  annotatorId: '',
  index: 0,
};


/**
 * Regular expression to validate a DICOM URI.
 */
export const DICOM_URI_VALIDATOR = new RegExp('^.*studies/[0-9.]*');

/**
 * Compression type for images.
 */
export enum CompressionType {
  DEFAULT = 'image/webp, image/jpeg, image/png',
  JPEG_OR_PNG = 'image/png, image/jpeg',
  JPEG = 'image/jpeg',
  WEBP = 'image/webp',
  PNG = 'image/png',
}

/**
 * Icc profile settings.
 */
export enum IccProfileType {
  NONE = '',  // if none option needed, assuming yes for default
  NO = 'no',
  YES = 'yes',
  ADOBERGB = 'adobergb',  // Adobe RGB
  ROMMRGB = 'rommrgb',
  SRGB = 'srgb',
}

/**
 * Icc profile setting to readable labels
 */
export const iccProfileTypeToLabel = new Map<IccProfileType, string>([
  [IccProfileType.NONE, 'None'],
  [IccProfileType.NO, 'No'],
  [IccProfileType.YES, 'Yes'],
  [IccProfileType.ADOBERGB, 'Adobe RGB'],
  [IccProfileType.ROMMRGB, 'Reference Output Medium Metric (ROMM RGB)'],
  [IccProfileType.SRGB, 'Standard RGB'],
]);

/** Unknown case id for non-deid cases.  */
export const UNKNOWN_CASE_ID = 'Unknown Case ID';
/** Unknown case id for deid cases.  */
export const UNKNOWN_DEID_CASE_ID = 'Unknown Case ID';
