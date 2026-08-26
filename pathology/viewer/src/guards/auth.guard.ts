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

import { inject } from '@angular/core';
import { CanActivateFn, Router, UrlTree } from '@angular/router';
import { environment } from '../environments/environment';
import { WindowService } from '../services/window.service';
import { ACCESS_TOKEN_CACHE_KEY, RETURN_URL_KEY } from '../services/auth.service';
import { stripBasePath } from '../utils/auth-helper.utils';

/**
 * Auth guard that ensures the user has a valid OAuth token before accessing protected routes.
 * 
 * This guard checks for:
 * 1. Whether OAuth is enabled (OAUTH_CLIENT_ID is set)
 * 2. Whether a valid, non-expired OAuth token exists in localStorage
 * 
 * If no valid token exists, the guard:
 * 1. Saves the current URL (including query params) to localStorage
 * 2. Redirects to /auth for Google OAuth sign-in
 * 
 * After successful sign-in, the auth flow will redirect back to the saved URL.
 * 
 * Note: This guard does NOT handle IAP authentication - IAP handles that at the
 * infrastructure level before the request even reaches the Angular app.
 */
export const authGuard: CanActivateFn = (route, state): boolean | UrlTree => {
  // If OAuth is not configured, allow access (dev mode or IAP-only setup)
  if (!environment.OAUTH_CLIENT_ID) {
    return true;
  }

  const router = inject(Router);
  const windowService = inject(WindowService);

  // Check for valid OAuth token
  const hasValidToken = checkForValidToken(windowService);

  if (hasValidToken) {
    return true;
  }

  // No valid token - save the intended URL and redirect to auth
  saveReturnUrl(state.url, windowService);
  
  return router.createUrlTree(['/auth']);
};

/**
 * Check if there's a valid, non-expired OAuth token in localStorage.
 */
function checkForValidToken(windowService: WindowService): boolean {
  const tokenJson = windowService.getLocalStorageItem(ACCESS_TOKEN_CACHE_KEY);
  if (!tokenJson) {
    return false;
  }

  try {
    const token = JSON.parse(tokenJson);
    const expirationTime = token?.oauthTokenInfo?.expirationTime;
    
    if (!expirationTime) {
      return false;
    }

    const expMs = Number(expirationTime);
    if (Number.isNaN(expMs)) {
      return false;
    }

    // Check if token is expired (with 4 second buffer)
    const BUFFER_TIME = 4000;
    return expMs > Date.now() + BUFFER_TIME;
  } catch {
    return false;
  }
}

/**
 * Save the URL to return to after authentication.
 */
function saveReturnUrl(url: string, windowService: WindowService): void {
  // Strip base path and normalize for Angular router
  const strippedUrl = stripBasePath(url);
  
  // Don't save auth page or invalid URLs
  if (strippedUrl && strippedUrl !== '/auth' && !strippedUrl.startsWith('/auth?')) {
    windowService.setLocalStorageItem(RETURN_URL_KEY, strippedUrl);
  }
}
