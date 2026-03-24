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

import { Injectable } from '@angular/core';
import { EMPTY, Observable } from 'rxjs';
import { map, tap } from 'rxjs/operators';

import { SlideApiService } from './slide-api.service';
import { SlideInfo, SlideExtraMetadata } from '../interfaces/slide_descriptor';

@Injectable({
  providedIn: 'root',
})
export class SlideDataService {
  constructor(
    private slideApiService: SlideApiService,
  ) {}

  fetchSlideInfo(
    slideDescriptorId: string,
    existingSlideInfo: Map<string, SlideInfo>,
    slideInfoSubject: { next: (value: Map<string, SlideInfo>) => void }
  ): Observable<SlideInfo> {
    if (existingSlideInfo.has(slideDescriptorId)) {
      return EMPTY;
    }

    return this.slideApiService.getSlideInfo(slideDescriptorId).pipe(
      tap((selectedSlideInfo) => {
        const updatedSlideInfo = new Map(existingSlideInfo);
        updatedSlideInfo.set(slideDescriptorId, selectedSlideInfo);
        slideInfoSubject.next(updatedSlideInfo);
      })
    );
  }

  fetchSlideExtraMetadata(
    slideDescriptorId: string,
    existingMetadata: Map<string, SlideExtraMetadata>,
    metadataSubject: { next: (value: Map<string, SlideExtraMetadata>) => void }
  ): Observable<Map<string, SlideExtraMetadata>> {
    if (existingMetadata.has(slideDescriptorId)) {
    console.log(`slideDescriptorId from slide-data service: ${slideDescriptorId}`);
      return EMPTY;
    }

    return this.slideApiService.getSlideExtraMetadata(slideDescriptorId).pipe(
      map((slideMetadata) => {
        const updatedMetadata = new Map(existingMetadata);
        updatedMetadata.set(slideDescriptorId, slideMetadata);
        console.log(`metadata from slide-data service: ${JSON.stringify(updatedMetadata, null, 2)}`);
        return updatedMetadata;
      }),
      tap((updatedMetadata) => {
        metadataSubject.next(updatedMetadata);
      })
    );
  }
}