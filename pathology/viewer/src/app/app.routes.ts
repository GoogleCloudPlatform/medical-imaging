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

import {Routes} from '@angular/router';
import {environment} from '../environments/environment';

/**
 * App routes.
 */
export const routes: Routes = [
  {path: '', redirectTo: environment.OAUTH_CLIENT_ID ? 'auth' : 'search', pathMatch: 'full'},  // Default route
  {
    path: 'auth',
    loadComponent: () => import('../components/login-page/login-page.component')
                             .then(m => m.LoginPageComponent)
  },
  {
    path: 'search',
    loadComponent: () =>
        import('../components/search-page/search-page.component')
            .then(m => m.SearchPageComponent)
  },
  {
    path: 'cohorts',
    loadComponent: () =>
        import('../components/cohorts-page/cohorts-page.component')
            .then(m => m.CohortsPageComponent)
  },
  {
    path: 'viewer',
    loadComponent: () =>
        import('../components/image-viewer-page/image-viewer-page.component')
            .then(m => m.ImageViewerPageComponent)
  },
  {
    path: 'config',
    loadComponent: () => import('../components/config/config.component')
                             .then(m => m.ConfigComponent)
  },
];

/**
 * Parameters for how the ImageViewerPage can be accessed.
 */
export declare interface ImageViewerPageParams {
  series?: string;
  cohortName?: string;
  r?: string;
  x?: string;
  y?: string;
  z?: string;
}

/**
 * Parameters for how the CohortPage can be accessed.
 */
export declare interface CohortPageParams {
  cohortName?: string;
}

/**
 * Parameters for how the SearchPage can be accessed.
 */
export declare interface SearchPageParams {
  q?: string;
}

/**
 * Parameters for how the InspectPage can be accessed.
 */
export declare interface InspectPageParams {
  series?: string;
  cohortName?: string;
}


/**
 * Default URL for the Cohorts page.
 */
export const DEFAULT_COHORT_URL = '/cohorts';
/**
 * Default URL for the Search page.
 */
export const DEFAULT_SEARCH_URL = '/search';
/**
 * Default URL for the ImageViewer page.
 */
export const DEFAULT_VIEWER_URL = '/viewer';
/**
 * Default URL for the Auth page.
 */
export const DEFAULT_AUTH_URL = '/auth';
/**
 * Default URL for the Inspect page.
 */
export const DEFAULT_INSPECT_URL = '/inspect';
