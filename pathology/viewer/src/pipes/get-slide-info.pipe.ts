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

import { SlideDescriptor } from '../interfaces/slide_descriptor';
import { SlideInfo } from '../interfaces/slide_descriptor';

/**
 * Pipe to get slideInfo based on splitViewSlideDescriptor
 */
@Pipe({
  name: 'GetSlideInfoPipe',
  standalone: true
})
export class GetSlideInfoPipe implements PipeTransform {
  transform(
    slideInfoBySlideDescriptorId: Map<string, SlideInfo>,
    splitViewSlideDescriptor: SlideDescriptor): SlideInfo | undefined {
    const result =
      slideInfoBySlideDescriptorId.get(splitViewSlideDescriptor.id as string);

    return result;
  }
}
