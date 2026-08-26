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

import { ImageViewerPageParams } from '../app/app.routes';
import { PathologySlide } from '../interfaces/slide_descriptor';

/**
 * Pipe to convert PathologySlide into params for Viewer
 */
@Pipe({
  name: 'PathologySlideToViewerParamsPipe',
  standalone: true
})
export class PathologySlideToViewerParamsPipe implements PipeTransform {

  transform(pathologySlide: PathologySlide): ImageViewerPageParams {
    if (!pathologySlide?.dicomUri) {
      return {};
    }
    return { series: pathologySlide.dicomUri };
  }

}
