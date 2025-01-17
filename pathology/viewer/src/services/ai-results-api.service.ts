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

import { DicomTag, SopClassUid } from '../interfaces/dicom_descriptor';

import { AISlideResult } from '../interfaces/slide_descriptor';
import { DicomwebService } from './dicomweb.service';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

const OOF_SCORE_PRIVATE = '30211002';

// Denotes a machine learning-generated result as part of an ImageType field
// (see spec for more details on the field
//    dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.8.16.html)
// Field is being populated in the follow file:
//   transformation_pipeline/ingestion_lib/dicom_gen/ai_to_dicom/png_to_dicom.py
const IMAGE_TYPE_SLIDE_ML_RESULT_VALUE = 'AI_RESULT';

/**
 * Retrieves AI results info and images from the Healthcare API.
 */
@Injectable({
  providedIn: 'root',
})
export class AIResultsApiService {
  constructor(private readonly dicomwebService: DicomwebService) { }

  getAIResultsForSlide(id: string): Observable<AISlideResult[]> {
    const TAGS = [
      OOF_SCORE_PRIVATE,
      DicomTag.DATE_OF_SECONDARY_CAPTURE,
      DicomTag.SECONDARY_CAPTURE_DEVICE_MANUFACTURER,
      DicomTag.SECONDARY_CAPTURE_DEVICE_MANUFACTURERS_MODEL_NAME,
      DicomTag.SECONDARY_CAPTURE_DEVICE_SOFTWARE_VERSIONS,
      DicomTag.IMAGE_TYPE,
      DicomTag.SOP_CLASS_UID,
      DicomTag.SOP_INSTANCE_UID,
    ];
    return this.dicomwebService.getAIMetadata(id, TAGS).pipe(map(instances => {
      if (!instances) {
        return [];
      }
      const aiSlideResults: AISlideResult[] = [];
      for (const instance of instances) {
        const classUid = instance[DicomTag.SOP_CLASS_UID]?.Value?.[0];
        // Secondary capture image type semantic meaning is the 3rd value in
        // the array according to Dicom standard.
        const imageType =
          instance?.[DicomTag.IMAGE_TYPE]?.Value?.[2] as
          string ??
          'unknown';
        // Skip any instance that is not a secondary capture.
        if (classUid !== SopClassUid.TILED_SECONDARY_CAPTURE ||
          imageType !==
          IMAGE_TYPE_SLIDE_ML_RESULT_VALUE) {
          continue;
        }

        aiSlideResults.push({
          captureDate: instance[DicomTag.DATE_OF_SECONDARY_CAPTURE]?.Value?.[0] as
            string,
          modelManufacturer:
            instance[DicomTag.SECONDARY_CAPTURE_DEVICE_MANUFACTURER]
              ?.Value?.[0] as string,
          modelName:
            instance[DicomTag
              .SECONDARY_CAPTURE_DEVICE_MANUFACTURERS_MODEL_NAME]
              ?.Value?.[0] as string,
          modelVersion:
            instance[DicomTag.SECONDARY_CAPTURE_DEVICE_SOFTWARE_VERSIONS]
              ?.Value?.[0] as string,
          score: instance[OOF_SCORE_PRIVATE]?.Value?.[0] as string,
          instanceUid: instance[DicomTag.SOP_INSTANCE_UID]?.Value?.[0] as
            string,
        });
      }
      return aiSlideResults;
    }));
  }

  getAIResultImage(volumeName: string, instanceUid: string):
    Observable<string | null> {
    return this.dicomwebService.getAIImageSecondaryCapture(
      volumeName, instanceUid);
  }
}