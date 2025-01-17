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

import { isPlatformBrowser } from '@angular/common';
import { Inject, Injectable, PLATFORM_ID } from '@angular/core';

/**
 * The key used to store the credentials in the local storage.
 */
export const CREDENTIAL_STORAGE_KEY = 'credential';

/**
 * A service that provides access to the window object.
 */
@Injectable({
  providedIn: 'root'
})
export class WindowService {
  window?: Window = undefined;
  constructor(@Inject(PLATFORM_ID) private platformId: {}) {
    if (isPlatformBrowser(this.platformId)) {
      this.window = window;
    }
  }

  getWindowOrigin(): string {
    if (!this.window) return '';
    return this.window.location.origin || '';
  }

  getLocalStorageItem(key: string): string | undefined {
    // return '';
    if (!this.window) return;

    const result = this.window.localStorage.getItem(key);
    return result || undefined;

  }
  setLocalStorageItem(key: string, value: unknown) {
    if (!this.window) return;

    this.window.localStorage.setItem(key, JSON.stringify(value));

  }

  removeLocalStorageItem(key: string) {
    if (!this.window) return;

    this.window.localStorage.removeItem(key);
  }


  sanitizeHtmlAssertUnchanged(value: string): string {
    if (!this.window) return '';
    return '';
  }

  safelySetInnerHtml<HTMLDivElement>(element: Element, value: string): void {
    element.innerHTML = value;
  }

  extractContent(html: string): string {
    return new DOMParser()
      .parseFromString(html, "text/html")
      .documentElement.textContent ?? '';
  }

  
}

