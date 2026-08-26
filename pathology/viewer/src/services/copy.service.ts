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

import { DOCUMENT } from '@angular/common';
import { Inject, Injectable } from '@angular/core';
import { BusyService } from './busy.service';

/**
 * Provides a single facility for copying content to the clipboard.
 */
@Injectable({
  providedIn: 'root'
})
export class CopyService {

  constructor(
    @Inject(DOCUMENT) private readonly document: Document,
    private readonly busyService: BusyService) { }

  copy(content: string) {
    const textArea = this.document.createElement('textarea');
    textArea.value = content;
    this.document.body.appendChild(textArea);
    textArea.select();
    try {
      this.document.execCommand('copy');
      this.busyService.setIsBusy('Copied to clipboard');
      setTimeout(() => {
        this.busyService.setIsNotBusy();
      }, 1000);
    } catch (error: unknown) {
      // Cannot use dialog service here.
      // Creates a cycle in the dependency graph.
      // Silently fail instead.
    }
    this.document.body.removeChild(textArea);
  }
}
