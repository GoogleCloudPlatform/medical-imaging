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

import {DicomModel} from './dicom_descriptor';
import {DicomWebPath} from './dicomweb';
import {PixelSize} from './slide_descriptor';

/**
 * Interface for defining an instance of a DICOM annotation. It encapsulates
 * the unique identifiers for the study, series, and specific image (SOP
 * Instance) along with details about the type of annotation coordinate and the
 * person or system that created the annotation.
 */

/**
 * Defines an annotation instance with its graphics. This is used only
 * used as an interface to convert between DICOM model and back.
 */
export declare interface DicomAnnotation {
  instance: DicomAnnotationInstance;
  annotationCoordinateType: string;
  annotationGroupSequence: AnnotationGroup[];
}

/**
 * Represents are annotations by a specific user of a specific series.
 * Beyond the date, this does not change when the geometry changes.
 */
export declare interface DicomAnnotationInstance {
  path: DicomWebPath;
  // OperatorIdentificationSequence > PersonIdentificationCodeSequence >
  // LongCodeValue.
  annotatorId: string;
  creationDateAndTime?: Date;
  referencedSeries: ReferencedSeries;  // Annotation relates to single series.
}

/** Defines an annotation group, represents a polygon. */
export declare interface AnnotationGroup {
  idNumber: number;
  annotationGroupUid: string;
  annotationGroupLabel: string;
  annotationGroupDescription: string;
  annotationGroupGenerationType: 'MANUAL'|'SEMIAUTOMATIC'|'AUTOMATIC';
  // Only a single item can be contained in the sequence.
  annotationPropertyCategoryCodeSequence: AnnotationPropertyCategoryCode[];
  graphicType: 'POINT'|'POLYGON'|'RECTANGLE'|'POLYLINE'|'ELLIPSE';
  pointCoordinatesData: number[][];
  longPrimitivePointIndexList: number[];
  error?: string;
}

/** Defines the general category of a polygon.  */
export declare interface AnnotationPropertyCategoryCode {
  codeValue: string;
  codingSchemeDesignator: number;
  codingSchemeVersion: string;
  contextIdentifier?: string;
}

/**
 * Defines a referenced image dicom instance. This is used to indicate
 * which image is being annotated.
 */
export declare interface ReferencedImage {
  sopClassUid: string;
  sopInstanceUid: string;
  dicomModel: DicomModel;
  pixelSize?: PixelSize;
}

/** Defines a referenced series for the annotation (ie. associated slide). */
export declare interface ReferencedSeries {
  seriesInstanceUid: string;
  referencedImage: ReferencedImage;  // Annotation relates to single instance.
}
