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



/**
 * Pipe for highliting a substring within a longer string.
 */
@Pipe({
  name: 'HighlightPipe',
  standalone: true
})
export class HighlightPipe implements PipeTransform {

  transform(
    textToHighlight: string,
    words: Set<string>,
    className = 'highlighted-text',
  ): string {
    //'gi' for case insensitive and can use 'g' if you want the search to be
    // case sensitive.
    // \\b for whole word match
    const regex = new RegExp('\\b' + [...words].join('\\b|\\b') + '\\b', 'gi');

    return textToHighlight.replace(regex, (matchedWord: string) => {
      return `<span class="${className}">` + matchedWord + '</span>';
    });
  }

}
