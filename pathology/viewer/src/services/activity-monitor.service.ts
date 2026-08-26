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

import { Injectable, Inject, NgZone } from '@angular/core';
import { AuthService, ACCESS_TOKEN_CACHE_KEY } from './auth.service';
import { WindowService } from './window.service';
import { environment } from '../environments/environment';

interface CachedTokenShape {
  email: string;
  oauthTokenInfo?: {
    error?: string;
    expirationTime?: string;
    token?: string;
  };
}

/**
 * Monitors user activity to:
 * 1. Keep active users logged in by refreshing OAuth tokens before expiry.
 * 2. Log out inactive users after 1 hour of no activity.
 *
 * Activity is tracked via mouse, keyboard, touch, and scroll events.
 * Activity state is synced across browser tabs via localStorage.
 */
@Injectable({ providedIn: 'root' })
export class ActivityMonitorService {
  /** Logout threshold: 1 hour of inactivity. */
  private readonly INACTIVITY_LIMIT_MS = 60 * 60 * 1000;
  /** How often to check activity and token expiry. */
  private readonly CHECK_INTERVAL_MS = 30 * 1000;
  /** Refresh token this many ms before expiry. */
  private readonly REFRESH_MARGIN_MS = 10 * 60 * 1000;
  /** User is considered "recently active" within this window for logout purposes. */
  private readonly RECENT_ACTIVITY_MS = 10 * 60 * 1000;
  /** Debounce activity events to avoid excessive checks. */
  private readonly ACTIVITY_DEBOUNCE_MS = 1000;

  private lastActivity = Date.now();
  private lastActivityCheck = 0;
  private intervalId?: number;
  private refreshInFlight = false;
  private started = false;

  constructor(
    private readonly authService: AuthService,
    private readonly windowService: WindowService,
    @Inject(NgZone) private readonly ngZone: NgZone,
  ) {}

  start(): void {
    if (this.started) return;
    this.started = true;

    // Only run in authenticated flows
    if (!environment.OAUTH_CLIENT_ID) return;

    this.lastActivity = Date.now();

    const w = this.windowService.window;
    if (w) {
      const mark = () => this.markActivity();
      const events = ['mousemove', 'mousedown', 'keydown', 'touchstart', 'scroll'];
      events.forEach((evt) => w.addEventListener(evt, mark, { passive: true }));

      // Sync activity across tabs
      w.addEventListener('storage', (e: StorageEvent) => {
        if (e.key === 'DPAS_LAST_ACTIVITY' && e.newValue) {
          const ts = Number(e.newValue);
          if (!Number.isNaN(ts)) this.lastActivity = Math.max(this.lastActivity, ts);
        }
      });

      w.document?.addEventListener('visibilitychange', () => {
        if (w.document?.visibilityState === 'visible') {
          this.checkTokenOnReturn();
        }
      });
    }

    this.ngZone.runOutsideAngular(() => {
      this.intervalId = (w?.setInterval || setInterval)(() => {
        try {
          this.checkInactivity();
          this.proactiveRefresh();
        } catch {
          // noop
        }
      }, this.CHECK_INTERVAL_MS) as unknown as number;
    });
  }

  stop(): void {
    if (this.intervalId) {
      (this.windowService.window?.clearInterval || clearInterval)(this.intervalId);
      this.intervalId = undefined;
    }
    this.started = false;
  }

  private markActivity(): void {
    const now = Date.now();
    const previousActivity = this.lastActivity;
    this.lastActivity = now;
    
    try {
      this.windowService.window?.localStorage.setItem('DPAS_LAST_ACTIVITY', String(now));
    } catch {}

    if (now - this.lastActivityCheck < this.ACTIVITY_DEBOUNCE_MS) return;
    this.lastActivityCheck = now;

    const wasAway = now - previousActivity > this.CHECK_INTERVAL_MS;
    if (wasAway) {
      this.checkTokenOnReturn();
    }
  }

  /**
   * Check if the token is expired when user returns after being away.
   * If expired, try to refresh. Only redirect to auth as a last resort.
   * 
   * NOTE: We do NOT trigger logout when there's no token at all.
   * No token typically means first visit - let the normal Google sign-in flow
   * handle it without redirecting away from the current page. IAP preserves
   * the original URL, so we shouldn't fight against that behavior.
   */
  private checkTokenOnReturn(): void {
    const raw = this.windowService.getLocalStorageItem(ACCESS_TOKEN_CACHE_KEY);
    if (!raw) {
      return;
    }

    try {
      const token = JSON.parse(raw) as CachedTokenShape;
      const expStr = token?.oauthTokenInfo?.expirationTime;
      if (!expStr) return;
      
      const exp = Number(expStr);
      if (Number.isNaN(exp)) return;

      const now = Date.now();
      if (now >= exp) {
        this.proactiveRefresh();
      } else if (exp - now <= this.REFRESH_MARGIN_MS) {
        this.proactiveRefresh();
      }
    } catch {
      // Parse error - ignore
    }
  }

  private checkInactivity(): void {
    const now = Date.now();
    if (now - this.lastActivity > this.INACTIVITY_LIMIT_MS) {
      this.authService.saveReturnUrl();
      this.ngZone.run(() => this.authService.logout({ preserveReturnUrl: true }));
      this.stop();
    }
  }

  private proactiveRefresh(): void {
    if (this.refreshInFlight) return;

    const raw = this.windowService.getLocalStorageItem(ACCESS_TOKEN_CACHE_KEY);
    if (!raw) return;

    try {
      const token = JSON.parse(raw) as CachedTokenShape;
      const expStr = token?.oauthTokenInfo?.expirationTime;
      if (!expStr) return;
      const exp = Number(expStr);
      if (Number.isNaN(exp)) return;

      const now = Date.now();
      const timeUntilExpiry = exp - now;
      
      // Always refresh if token is expiring soon, regardless of recent activity.
      // This prevents auth errors when user is passively viewing (e.g., looking at slides).
      // The inactivity logout (1 hour) handles truly idle users.
      if (timeUntilExpiry <= this.REFRESH_MARGIN_MS && timeUntilExpiry > 0) {
        this.refreshInFlight = true;
        this.authService.refreshOAuthToken().subscribe({
          next: () => { this.refreshInFlight = false; },
          error: () => { this.refreshInFlight = false; },
        });
      }
    } catch {
      // ignore parse errors
    }
  }
}
