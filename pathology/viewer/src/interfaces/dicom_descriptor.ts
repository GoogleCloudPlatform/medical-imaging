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
 * List of Dicom Tag numbers.
 * Should be sorted by this number for easy reference.
 */
export enum DicomTag {
  IMAGE_TYPE = '00080008',
  SOP_CLASS_UID = '00080016',
  SOP_INSTANCE_UID = '00080018',
  STUDY_DATE = '00080020',
  ACCESSION_NUMBER = '00080050',
  MODALITY = '00080060',

  PATIENT_NAME = '00100010',
  PATIENT_ID = '00100020',
  PATIENT_BIRTH_DATE = '00100030',
  PATIENT_SEX = '00100040',
  STUDY_TIME = '00080030',
  STUDY_ID = '00200010',

  FRAME_OF_REFERENCE_UID = '00200052',

  DATE_OF_SECONDARY_CAPTURE = '00181012',
  SECONDARY_CAPTURE_DEVICE_MANUFACTURER = '00181016',
  SECONDARY_CAPTURE_DEVICE_MANUFACTURERS_MODEL_NAME = '00181018',
  SECONDARY_CAPTURE_DEVICE_SOFTWARE_VERSIONS = '00181019',
  SPECIMEN_DESCRIPTION_SEQUENCE = '00400560',

  STUDY_INSTANCE_UID = '0020000D',
  SERIES_INSTANCE_UID = '0020000E',
  INSTANCE_NUMBER = '00200013',
  OFFSET = '00209228',

  NUMBER_OF_FRAMES = '00280008',
  // The width of a tile in pixels in the DICOM image.
  // Throughout app we assume:
  // - width + height are the same in case of tiles.
  // - tile size is consistent across zoom levels.
  // - width and height are likely to be different for secondary captures.
  ROWS = '00280010',
  COLS = '00280011',

  PIXEL_SPACING = '00280030',  // Array of two numbers.

  SPECIMEN_SHORT_DESCRIPTION = '00400600',

  // Used as slide display name.
  CONTAINER_IDENTIFIER = '00400512',

  // Width in millimeters.
  IMAGE_VOLUME_WIDTH = '00480001',
  // Height in millimeters.
  IMAGE_VOLUME_HEIGHT = '00480002',
  // Total number of columns in pixel matrix.
  TOTAL_PIXEL_MATRIX_COLUMNS = '00480006',
  // Total number of rows in pixel matrix.
  TOTAL_PIXEL_MATRIX_ROWS = '00480007',
  // Location of the top leftmost pixel of the pixel matrix.
  TOTAL_PIXEL_MATRIX_ORIGIN_SEQUENCE = '00480008',

  // Annotation fields.
  INSTANCE_CREATION_DATE = '00080012',
  INSTANCE_CREATION_TIME = '00080013',
  CONTENT_DATE = '00080023',
  CONTENT_TIME = '00080024',
  ANNOTATION_COORDINATE_TYPE = '006A0001',
  ANNOTATION_GROUP_SEQUENCE = '006A0002',
  ANNOTATION_GROUP_NUMBER = '0040A180',
  ANNOTATION_GROUP_UID = '006A0003',
  ANNOTATION_APPLIES_TO_ALL_OPTICAL_PATHS = '006A000D',

  // Comma separated labels.
  ANNOTATION_GROUP_LABEL = '006A0005',
  ANNOTATION_GROUP_DESCRIPTION = '006A0006',
  ANNOTATION_GROUP_GENERATION_TYPE = '006A0007',
  // Sequence for each label associated with the geometry.
  ANNOTATION_PROPERTY_CATEGORY_CODE_SEQUENCE = '006A0009',
  CODE_VALUE = '00080010',
  // Annotation meaning (label)
  ANNOTATION_PROPERTY_TYPE_CODE_SEQUENCE = '006A000A',
  // Task id.
  CODING_SCHEME_DESIGNATOR = '00080102',
  // Task last modified time stamp.
  CODING_SCHEME_VERSION = '00080103',
  CONTEXT_IDENTIFIER = '0008010F',
  // Geometry type.
  GRAPHIC_TYPE = '00700023',
  DOUBLE_POINT_COORDINATES_DATA = '00660022',
  POINT_COORDINATES_DATA = '00660016',
  LONG_PRIMITIVE_POINT_INDEX_LIST = '00660040',
  NUMBER_OF_ANNOTATIONS = '006A000C',  // should match INDEX_LIST.length
  // Email of Annotator.
  OPERATOR_IDENTIFICATION_SEQUENCE = '00081072',
  PERSON_IDENTIFICATION_CODE_SEQUENCE = '00401101',
  LONG_CODE_VALUE = '00080119',
  CODE_MEANING = '00080104',
  // Series & instance of image being annotated.
  REFERENCED_SERIES_SEQUENCE = '00081115',
  REFERENCED_INSTANCE_SEQUENCE = '0008114A',
  REFERENCED_SOP_CLASS_UID = '00081150',
  REFERENCED_SOP_INSTANCE_UID = '00081155',
  REFERENCED_IMAGE_SEQUENCE = '00081140',

  // Storage
  LOSSY_IMAGE_COMPRESSION_RATIO = '00282112',
  SAMPLES_PER_PIXEL = '00280002',
  BITS_ALLOCATED = '00280100',

  TRANSFER_SYNTAX_UID = '00020010',
}

/**
 * Dicom transfer syntax
 * https://dicom.nema.org/medical/dicom/current/output/chtml/part06/chapter_a.html
 */
export enum TransferSyntaxUID {
  IMPLICIT_VR_LITTLE_ENDIAN = '1.2.840.10008.1.2',
  EXPLICIT_VR_LITTLE_ENDIAN = '1.2.840.10008.1.2.1',
}

/**
 * The SOP Classes values to identify the type of image. See standard values:
 * https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_B.5.html
 */
export enum SopClassUid {
  VL_MICROSCOPIC_IMAGE_STORAGE = '1.2.840.10008.5.1.4.1.1.77.1.2',
  VL_SLIDE_COORDINATES_MICROSCOPIC_IMAGE_STORAGE =
  '1.2.840.10008.5.1.4.1.1.77.1.3',
  TILED_MICROSCOPE = '1.2.840.10008.5.1.4.1.1.77.1.6',
  TILED_SECONDARY_CAPTURE = '1.2.840.10008.5.1.4.1.1.7',
  MICROSCOPY_BULK_SIMPLE_ANNOTATIONS_STORAGE = '1.2.840.10008.5.1.4.1.1.91.1',
}

/**
 * Dicom API resturns a list of these.
 * Details: go/dicomspec/part18.html#sect_F.2.2
 * JSON example: go/dicomspec/part18.html#sect_F.2.1.1.2
 */
export declare interface DicomModel {
  [key: string]: Attribute;
}

/**
 * Possible types of a JSON DICOM attribute's Value property.
 */
export type AttributeValue =
  number[] | number[][] | string[] | PersonName[] | DicomModel[] | string;

/**
 * JSON DICOM representation of the Attribute corresponding to a given tag.
 */
export declare interface Attribute {
  // tslint:disable-next-line:enforce-name-casing comes from API
  Value?: AttributeValue;
  InlineBinary?: string;
  BulkDataURI?: string;
  vr?: AttributeValue;
}

/**
 * Validate if Attribute is a patient name.
 */
export function isPatients(
  attributeValue: AttributeValue | undefined,
): attributeValue is PersonName[] {
  if (attributeValue?.constructor !== Array) {
    return false;
  }
  return (attributeValue[0] as PersonName).Alphabetic !== undefined;
}

/**
 * JSON DICOM representation of names (e.g. PatientName).
 */
export declare interface PersonName {
  // An ordered, caret-separated string of name components:
  //   Family name(s)
  //   Given name(s)
  //   Middle name(s)
  //   Prefix(es)
  //   Suffix(es)
  // For example: Shabadoo^Joey^Jo-Jo Junior^Dr.^PhD
  //
  // Documentation: go/dicomspec/part05.html#sect_6.2.1.1
  // JSON examples: go/dicomspec/part18.html#table_F.3.1-1
  // tslint:disable-next-line:enforce-name-casing comes from API
  Alphabetic: string;
}

/**
 * Type guard to narrow AttributeValue to number[].
 */
export function isNumbers(value: AttributeValue | undefined): value is number[] {
  if (typeof value !== 'object'  // typeof array is 'object'
    || !value) {               // null/empty check
    return false;
  }

  return typeof value[0] === 'number';
}

/**
 * Type guard to narrow AttributeValue to string[].
 */
export function isStrings(value: AttributeValue | undefined): value is string[] {
  if (typeof value !== 'object'  // typeof array is 'object'
    || !value) {               // null/empty check
    return false;
  }

  return typeof value[0] === 'string';
}

/**
 * Type guard to narrow AttributeValue to PersonName[].
 */
export function isPersonNames(value: AttributeValue |
  undefined): value is PersonName[] {
  if (typeof value !== 'object'  // typeof array is 'object'
    || !value) {               // null/empty check
    return false;
  }

  const firstValue = value[0];
  return typeof firstValue === 'object' && 'Alphabetic' in firstValue;
}

/**
 * Type guard to narrow AttributeValue to DicomModel[].
 *
 * Note: since this currently works by exhaustion, it needs to be updated if
 * any more types are added to AttributeValue.
 */
export function isDicomModels(value: AttributeValue |
  undefined): value is DicomModel[] {
  if (typeof value !== 'object'  // typeof array is 'object'
    || !value) {               // null/empty check
    return false;
  }

  return !(isNumbers(value) && isStrings(value) && isPersonNames(value));
}

/**
 * Type of associated images.
 */
export enum AssociatedImageType {
  THUMBNAIL = 'THUMBNAIL',
  LABEL = 'LABEL',
  OVERVIEW = 'OVERVIEW',
}

/**
 * Different types of modalities in the application
 */
export enum DicomModality {
  ANY = '',
  SLIDE_MICROSCOPY = 'SM',
  ANNOTATION = 'ANN',
}

/**
 * Input: PersonName.
 *   See documentation on PersonName for details.
 * Output: "Prefix Given Middle Family Suffix" or undefined.
 */
export function formatName(dicomName?: PersonName): string | undefined {
  if (!dicomName?.Alphabetic) {
    return undefined;
  }
  const [family, given, middle, prefix, suffix] =
    dicomName.Alphabetic.split('^');
  let result = '';
  for (const name of [prefix, given, middle, family, suffix]) {
    if (name) {
      result += ' ' + name;
    }
  }
  return result.trimStart();
}

/**
 * Input: DICOM date format (YYYYMMDD).
 * See dicom.nema.org/medical/dicom/current/output/html/part05.html#table_6.2-1
 * Output: YYYY-MM-DD, 'Unknown date', or 'Invalid date'.
 */
export function formatDate(dicomDate?: string): string {
  if (!dicomDate) {
    return 'Unknown date';
  }
  const [, year, month, day] = dicomDate.match(/(\d{4})(\d{2})(\d{2})/) ?? [];
  if (!year || !month || !day) {
    return 'Invalid date';
  }
  return `${year}-${month}-${day}`;
}

/**
 * Retrieves the first string value from a DICOM model's tag or nested tag>tag2,
 * or returns an empty string if not present.
 */
export function getValue(
  dicomModel: DicomModel, tag: string, tag2?: string): string {
  return tag2 ?
    getValue((dicomModel?.[tag]?.Value as DicomModel[] ?? [])[0], tag2) :
    (dicomModel?.[tag]?.Value as string[] ?? [''])[0];
}

/**
 * Retrieves an array of DICOM models from a specified sequence tag within a
 * DICOM model, or an empty array if not present.
 */
export function getSequenceValue(
  dicomModel: DicomModel, tag: string): DicomModel[] {
  return dicomModel?.[tag]?.Value as DicomModel[] ?? [];
}

/**
 * Creates an Attribute object from the VR and value. If value is undefined,
 * the Value key won't be created.
 *
 * See dicom.nema.org/medical/dicom/current/output/html/part05.html#table_6.2-1
 */
export function dicomAttr(vr: string, value?: AttributeValue): Attribute {
  return {
    'vr': vr,
    ...(value !== undefined ?
      { 'Value': Array.isArray(value) ? value : [value] } :
      {})
  };
}