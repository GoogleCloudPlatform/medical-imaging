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

/// <reference types="google.accounts" />

import {isPlatformBrowser} from '@angular/common';
import {Inject, Injectable, NgZone, PLATFORM_ID} from '@angular/core';
import {MatSnackBar, MatSnackBarConfig} from '@angular/material/snack-bar';
import { AuthStateService } from './auth-state.service';
import {Router} from '@angular/router';
import { BehaviorSubject, EMPTY, from, Observable, of } from 'rxjs';
import {map} from 'rxjs/operators';
import {DialogService} from '../services/dialog.service';

import {environment} from '../environments/environment';
import {stripBasePath} from '../utils/auth-helper.utils';

import {LogService} from './log.service';
import {UserService} from './user.service';
import {DPAS_IAP_KEY, WindowService} from './window.service';

/// <reference types="google" />

/** Token for valid user */
export declare interface AppToken {
  email: string;
  oauthTokenInfo: {
    error: string,
    expirationTime: string,
    token: string,
  };
}

interface DecodedToken {
  email?: string;
  name?: string;
  exp?: number;
  [key: string]: unknown;
}


declare global {
  interface Window {
    onGoogleLibraryLoad: () => Promise<void>;
  }
}

/** Key for Token */
export const ACCESS_TOKEN_CACHE_KEY = 'DPAS_ACCESS_TOKEN';
export const RETURN_URL_KEY = 'DPAS_RETURN_URL';
export const INITIAL_URL_KEY = 'DPAS_INITIAL_URL';
const TIME_CONVERSION_MULTIPLIER = 1000;

// Buffer time of 4 seconds. The cached OAuth token needs to be valid at least
// until Now() + BUFFER_TIME. The buffer time will be useful to ensure that we
// don't return soon to expire tokens.
const BUFFER_TIME = 4000;
/**
 * Service to handle Auth.
 */
@Injectable({
  providedIn: 'root',
})
export class AuthService {
  scope = environment.OAUTH_SCOPES;
  appToken?: AppToken;
  readonly reauthInProgress$ = new BehaviorSubject<boolean>(false);
  snackBarConfig = new MatSnackBarConfig();
  constructor(
      @Inject(NgZone) private readonly ngZone: NgZone,
      @Inject(PLATFORM_ID) private readonly platformId: object,

      private readonly logService: LogService,
      private readonly dialogService: DialogService,
      private readonly router: Router,
      private readonly snackBar: MatSnackBar,
      private readonly authState: AuthStateService,
      private readonly userService: UserService,
      private readonly windowService: WindowService,
  ) {
  }

  setupGoogleLogin() {
    if (!isPlatformBrowser(this.platformId) || !window ||
        !environment.OAUTH_CLIENT_ID) {
      return;
    }

    const initializeGoogleAuth = async () => {
      google.accounts.id.initialize({
        // Ref:
        // https://developers.google.com/identity/gsi/web/reference/js-reference#IdConfiguration
        client_id: environment.OAUTH_CLIENT_ID,
        callback: this.handleCredentialResponse.bind(this),
        auto_select: false,
        cancel_on_tap_outside: false
      });

      const loginBtn = document.getElementById('loginBtn');
      if (loginBtn) {
        google.accounts.id.renderButton(
            loginBtn,
            {theme: 'outline', size: 'large', width: 200, type: 'standard'});
      }

      if (this.windowService.getLocalStorageItem(DPAS_IAP_KEY)) {
        let accessToken = this.getCachedAccessToken();
        if (accessToken && this.isTokenExpired(accessToken)) {
          await this.fetchAccessToken();
          accessToken = this.getCachedAccessToken();
        }
        if (accessToken && !this.isTokenExpired(accessToken)) {
          this.navigateToSearchIfAuth();
        }
        return;
      }

      this.saveReturnUrl();
      google.accounts.id.prompt();
    };

    if (typeof google !== 'undefined' && google.accounts && google.accounts.id) {
      initializeGoogleAuth();
    } else {
      window.onGoogleLibraryLoad = initializeGoogleAuth;
    }
  }

  private async fetchAccessToken(): Promise<AppToken> {
    if (!environment.OAUTH_CLIENT_ID) {
      return Promise.resolve({
        email: '',
        oauthTokenInfo: {
          error: '',
          expirationTime: '',
          token: '',
        }
      });
    }

    const decodedToken = this.handleCredentialToken();
    if (decodedToken?.email) {
      this.userService.setCurrentUser(decodedToken.email);
    } else {
      return Promise.resolve({
        email: '',
        oauthTokenInfo: {
          error: '',
          expirationTime: '',
          token: '',
        }
      });
    }

    const tokenPromise = new Promise<AppToken>((resolve, reject) => {
      const currentUserEmailAddress =
          this.userService.getCurrentUser() ?? undefined;
      const tokenClient = google.accounts.oauth2.initTokenClient({
        client_id: environment.OAUTH_CLIENT_ID,
        scope: this.scope,
        hint: currentUserEmailAddress,
        prompt: '',
        callback: (response: google.accounts.oauth2.TokenResponse) => {
          if (response.error) {
            // User clicked cancel on the consent screen.
            const errorMessage = 'Consent needed. Access denied.';
            const error = new Error(errorMessage);
            this.logService.error(error);

            this.dialogService.error(errorMessage);

            this.logout({ preserveReturnUrl: true });
            reject(errorMessage);
            return;
          }

          const scopes = this.scope.split(' ') as [string, ...string[]];
          if (!google.accounts.oauth2.hasGrantedAllScopes(
                  response, ...scopes)) {
            const errorMessage = 'Not all scopes are granted. Try again.';
            const error = new Error();
            this.logService.error(error);

            this.dialogService.error(errorMessage);

            this.logout({ preserveReturnUrl: true });
            reject(errorMessage);
            return;
          }

          const token = this.convertResponseToToken(response);
          if (!token) {
            const errorMessage = 'Access denied.';
            this.logService.error(new Error(response.error));

            this.dialogService.error(errorMessage);

            this.logout({ preserveReturnUrl: true });
            reject(response.error);
            return;
          }

          resolve(token);
        },
      });

      if (!tokenClient) {
        const errorMessage = 'No oauth client initialized.';
        this.logService.error(new Error(errorMessage));
        reject(errorMessage);
        return;
      }

      tokenClient.requestAccessToken();
    });

    const token = await tokenPromise;

    this.setCachedAccessToken(token);
    this.authState.setReauthInProgress(false);
    this.navigateToSearchIfAuth();
    return token;
  }

  private navigateToSearchIfAuth() {
    const appToken = this.getCachedAccessToken();

    if (!appToken?.oauthTokenInfo.token) {
      return;
    }

    const win = this.windowService.window;
    const browserUrlRaw = win ? `${win.location.pathname}${win.location.search}` : this.router.url;
    const browserUrl = stripBasePath(browserUrlRaw) ?? (browserUrlRaw || '/');
    const isOnAuthPage = browserUrl === '/auth' ||
               browserUrl.startsWith('/auth?') ||
               browserUrl === '/' ||
               browserUrl.startsWith('/?');

    if (isOnAuthPage) {
      this.ngZone.run(() => {
        const returnUrl = this.getAndClearReturnUrl();
        if (returnUrl && returnUrl !== '/auth' && !returnUrl.startsWith('/auth?')) {
          this.router.navigateByUrl(returnUrl, {replaceUrl: true});
        } else {
          this.router.navigate(['search'], {replaceUrl: true});
        }
      });
    }
  }

  private convertResponseToToken(res: google.accounts.oauth2.TokenResponse):
      AppToken|undefined {
    if (!res.access_token || !res.expires_in) {
      return;
    }
    const currentUserEmailAddress = this.userService.getCurrentUser() ?? '';
    // milliseconds
    const expiresAt =
        Date.now() + this.secondsToMilliseconds(Number(res.expires_in));
    const token = {
      // Email is set in fetchTokenAndEmail() instead of here.
      email: currentUserEmailAddress,
      oauthTokenInfo: {
        error: res.error ?? '',
        expirationTime: String(expiresAt),
        token: res.access_token,
      }
    };
    return token;
  }

  private async handleCredentialResponse(response:
                                       google.accounts.id.CredentialResponse) {
    try {
      if (response?.credential) {
        this.windowService.setLocalStorageItem(
            DPAS_IAP_KEY, response?.credential);
      }

      await this.fetchAccessToken();

    } catch (e) {
      console.error('Error during credential response handling:', e);
    }
  }

  private handleCredentialToken() {
    const credential =
        this.windowService.getLocalStorageItem(DPAS_IAP_KEY);
    if (!credential) {
      console.error(  // CONSOLE LOG OK
          'Error while trying to decode token, no credentials');
      return;
    }

    let decodedToken: DecodedToken|null = null;
    decodedToken = JSON.parse(atob(credential.split('.')[1]));

    return decodedToken;
  }

  getOAuthToken(retry = 1): Observable<string> {
    if (!environment.OAUTH_CLIENT_ID) {
      return of('');
    }

    const cachedToken = this.getCachedAccessToken();
    if (cachedToken && !this.isTokenExpired(cachedToken)) {
      return of(cachedToken.oauthTokenInfo.token);
    }

    this.clearCachedAccessToken();

    if (retry) {
      return this.fetchAndMapToken();
    }

    return EMPTY;
  }

  refreshOAuthToken(): Observable<string> {
    if (!environment.OAUTH_CLIENT_ID) {
      return of('');
    }

    this.clearCachedAccessToken();
    return this.fetchAndMapToken();
  }

  private fetchAndMapToken(): Observable<string> {
    return from(this.fetchAccessToken())
        .pipe(map((appToken) => appToken.oauthTokenInfo.token));
  }

  private getCachedAccessToken(): AppToken|undefined {
    const cachedToken = this.getCachedAccessTokenFromLocalStorage();
    if (!cachedToken) {
      return;
    }

    if (this.isTokenExpired(cachedToken)) {
      // The token has expired or will expire soon. Clear the old item.
      this.clearCachedAccessToken();
      this.windowService.removeLocalStorageItem(DPAS_IAP_KEY);
      return;
    }

    return cachedToken;
  }

  private isTokenExpired(token: AppToken) {
    try {
      if (!token.oauthTokenInfo.expirationTime) {
        return false;
      }

      const expirationTimeMillis = Number(token.oauthTokenInfo.expirationTime);
      if (Number.isNaN(expirationTimeMillis)) {
        return true;
      }

      return expirationTimeMillis < Date.now() + BUFFER_TIME;
    } catch (error: unknown) {
      this.logService.error(error as Error);
      return true;
    }
  }

  logout(options?: { preserveReturnUrl?: boolean }) {
    this.clearCurrentUser();
    this.clearCachedAccessToken();
    this.authState.setReauthInProgress(true);
    this.snackBar.dismiss();
    this.windowService.clearAllCookies();    
    google.accounts.id.disableAutoSelect();

    if (!options?.preserveReturnUrl) {
      this.clearReturnUrl();
    }

    setTimeout(() => {
      this.router.navigate(['/auth']).then(() => {
        setTimeout(() => {
          this.windowService.forceReload();
        }, 100);
      });
    }, 100);
  }

  private getCachedAccessTokenFromLocalStorage(): AppToken|undefined {
    const item = this.windowService.getLocalStorageItem(ACCESS_TOKEN_CACHE_KEY);
    if (!item) {
      return;
    }
    const token = JSON.parse(item) as AppToken;
    if (!this.userService.getCurrentUser()) {
      this.userService.setCurrentUser(token.email);
    }

    return token;
  }

  private setCachedAccessToken(token: AppToken): void {
    this.userService.setCurrentUser(token.email);
    this.windowService.setLocalStorageItem(ACCESS_TOKEN_CACHE_KEY, token);
  }

  private clearCurrentUser() {
    this.userService.setCurrentUser(null);
  }

  private clearCachedAccessToken() {
    this.windowService.removeLocalStorageItem(ACCESS_TOKEN_CACHE_KEY);
  }

  /**
   * Save the current URL to return to after authentication.
   * Call this before redirecting unauthenticated users to the auth page.
   */
  saveReturnUrl(url?: string): void {
    const existingReturnUrl = this.windowService.getLocalStorageItem(RETURN_URL_KEY);
    if (existingReturnUrl) {
      return;
    }
    
    let urlToSave = url;
    if (!urlToSave) {
      const initialUrl = this.windowService.getLocalStorageItem(INITIAL_URL_KEY);
      if (initialUrl) {
        try {
          urlToSave = JSON.parse(initialUrl) as string;
        } catch {
          urlToSave = initialUrl;
        }
        this.windowService.removeLocalStorageItem(INITIAL_URL_KEY);
      }
    }
    
    if (!urlToSave) {
      urlToSave = this.router.url;
    }
    
    const strippedUrl = stripBasePath(urlToSave);
    
    if (strippedUrl && strippedUrl !== '/auth' && !strippedUrl.startsWith('/auth?')) {
      this.windowService.setLocalStorageItem(RETURN_URL_KEY, strippedUrl);
    }
  }

  /**
   * Get and clear the saved return URL.
   */
  private getAndClearReturnUrl(): string | undefined {
    this.windowService.removeLocalStorageItem(INITIAL_URL_KEY);
    
    const raw = this.windowService.getLocalStorageItem(RETURN_URL_KEY);
    if (raw) {
      this.clearReturnUrl();
      let url: string;
      try {
        url = JSON.parse(raw) as string;
      } catch {
        url = raw;
      }
      
      return stripBasePath(url);
    }
    return undefined;
  }

  private clearReturnUrl(): void {
    this.windowService.removeLocalStorageItem(RETURN_URL_KEY);
  }


  private secondsToMilliseconds(seconds: number) {
    return seconds * TIME_CONVERSION_MULTIPLIER;
  }

  // Returns the signed-in user's email (from in-memory or cached token)
  getUserEmail(): string | undefined {
    if (this.appToken?.email) return this.appToken.email;
    try {
      const raw =
        (this as any).windowService?.getLocalStorageItem?.(ACCESS_TOKEN_CACHE_KEY) ??
        (typeof window !== 'undefined' ? window.localStorage.getItem(ACCESS_TOKEN_CACHE_KEY) : null);
      if (!raw) return undefined;
      const token = JSON.parse(raw) as AppToken;
      return token?.email;
    } catch {
      return undefined;
    }
  }
}
