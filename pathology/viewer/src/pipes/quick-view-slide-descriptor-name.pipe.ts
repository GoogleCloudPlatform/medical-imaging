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
import { SlideExtraMetadata, SlideInfo} from '../interfaces/slide_descriptor';


/**
 * Pipe to shorten the slide descriptor name for display if case id in slide.
 */
@Pipe({name: 'QuickViewSlideDescriptorNamePipe',
standalone: true})
export class QuickViewSlideDescriptorNamePipe implements PipeTransform {
  transform(slideInfo: SlideInfo, slideExtraMetadata?: SlideExtraMetadata):
      string {
    let slideDescriptorName: string = slideInfo?.slideName ?? '';
    if (slideExtraMetadata?.caseId) {
      slideDescriptorName =
          slideDescriptorName.replace(slideExtraMetadata.caseId + '-', '');
    }
    return slideDescriptorName;
  }
}
