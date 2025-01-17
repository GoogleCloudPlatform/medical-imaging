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

import {TrustedResourceUrl} from 'safevalues';
import {setScriptSrc} from 'safevalues/dom';

const loading = new Map<string, Promise<void>>();

/** Loads a script from |url| if it doesn't already exist. */
function load(
    url: TrustedResourceUrl, exists: (url: TrustedResourceUrl) => boolean,
    create: (url: TrustedResourceUrl) => Promise<void>): Promise<void> {
  const key = url.toString();
  const currentlyLoading = loading.get(key);
  if (currentlyLoading) {
    return currentlyLoading;
  }
  if (exists(url)) {
    const definedAlready = Promise.resolve();
    loading.set(key, definedAlready);
    return definedAlready;
  }
  const loadPromise = create(url);
  loading.set(key, loadPromise);
  return loadPromise;
}

/** Checks if a script is already loaded */
function scriptExists(url: TrustedResourceUrl): boolean {
  const value = url.toString();
  return Array.from(document.querySelectorAll('script'))
      .some((el) => el.src === value);
}

/** Loads a script into the DOM. */
function scriptAppend(url: TrustedResourceUrl): Promise<void> {
  const script: HTMLScriptElement = document.createElement('script');
  const loadPromise = new Promise<void>((resolve, reject) => {
    script.onload = () => {
      resolve();
    };
    script.onerror = () => {
      reject('script load failed');
    };
  });
  setScriptSrc(script, url);
  const root = document.body || document.head || document;
  root.appendChild(script);
  return loadPromise;
}

/** Loads a script. */
export function loadScript(url: TrustedResourceUrl): Promise<void> {
  return load(url, scriptExists, scriptAppend);
}