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

import { provideHttpClient, withFetch } from '@angular/common/http';

import { ApplicationConfig, Injectable } from '@angular/core';
import { provideAnimationsAsync } from '@angular/platform-browser/animations/async';
import { provideClientHydration } from '@angular/platform-browser';
import { provideRouter } from '@angular/router';
import { routes } from './app.routes';
import { environment } from '../environments/environment';
import { APP_BASE_HREF, HashLocationStrategy, LocationStrategy } from '@angular/common';

// Strategy suitable for serving from GCS.
@Injectable()
class CustomLocationStrategy extends HashLocationStrategy {
  override prepareExternalUrl(internal: string): string {
    const url = this.getBaseHref() + '/index.html#' + internal;
    return url;
  }
}

/**
 * Application configuration.
 */
export const appConfig: ApplicationConfig = {
  providers: [provideRouter(routes), provideClientHydration(), provideAnimationsAsync(), provideHttpClient(withFetch()),
    {provide: APP_BASE_HREF, useValue: environment.APP_BASE_SERVER_PATH},
  ...(environment.USE_HASH_LOCATION_STRATEGY ?
          [{provide: LocationStrategy, useClass: CustomLocationStrategy}] :
          []),
  ]
};
