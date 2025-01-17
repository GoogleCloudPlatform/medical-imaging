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

import { Pipe, PipeTransform } from '@angular/core';

import { CohortInfo } from '../services/cohort.service';
import { ImageViewerPageParams } from '../app/app.routes';
import { SlideDescriptor } from '../interfaces/slide_descriptor';

/**
 * Pipe to convert SlideDescriptor and CohortInfo to viewer url params
 */
@Pipe({
  name: 'SlideDescriptorToViewerUrlParamsPipe',
  standalone: true
})
export class SlideDescriptorToViewerUrlParamsPipe implements PipeTransform {

  
  transform(slideDescriptor: SlideDescriptor, cohortInfo?: CohortInfo):
    ImageViewerPageParams {
    let params: ImageViewerPageParams = {
      series: String(slideDescriptor.id),
    };

    if (cohortInfo?.name) {
      params = { ...params, cohortName: cohortInfo.name };
    }
    return params;
  }

}
