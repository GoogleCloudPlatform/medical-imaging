import { Injectable } from '@angular/core';
import {
  HttpErrorResponse,
  HttpEvent,
  HttpHandler,
  HttpInterceptor,
  HttpRequest,
} from '@angular/common/http';
import { Observable, throwError, EMPTY } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { AuthService } from '../services/auth.service';
import { isAuthHttpError } from '../utils/auth-helper.utils';

/**
 * Interceptor to handle authentication errors that indicate IAP session expiration.
 * 
 * This interceptor specifically handles the case where IAP session expires during
 * an active SPA session. When IAP returns a 302 redirect for an expired session,
 * browsers cannot follow cross-origin redirects for AJAX/fetch requests due to CORS.
 * This results in a status 0 "network error".
 * 
 * When this happens, we trigger a full page reload to let IAP handle re-authentication.
 * IAP will preserve the current URL and redirect back after auth.
 * 
 * NOTE: Standard 401/403 errors from our APIs (not IAP) are handled by
 * AuthTokenInterceptor which attempts token refresh before failing.
 */
@Injectable()
export class AuthErrorInterceptor implements HttpInterceptor {
  constructor(private readonly authService: AuthService) {}

  intercept(req: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    const authHeader = req.headers.get('Authorization') || '';
    const hasBearer = authHeader.startsWith('Bearer ');

    return next.handle(req).pipe(
      catchError((err: unknown) => {
        if (err instanceof HttpErrorResponse) {
          // Status 0 with Bearer token - likely IAP redirect that browser couldn't follow.
          // This happens when IAP session expires and returns a 302 to login page.
          // Browsers can't follow cross-origin redirects for AJAX requests.
          // 
          // Solution: Full page reload. IAP will handle auth and preserve current URL.
          if (err.status === 0 && hasBearer && this.isLikelyIapRedirect(err, req)) {
            console.warn('Detected likely IAP session expiration (status 0). Reloading page to re-authenticate via IAP.');
            // Full page reload - IAP will handle auth and return user to current URL
            if (typeof window !== 'undefined') {
              window.location.reload();
            }
            return EMPTY;
          }

          // For 401/403 errors that look like OAuth token issues (not IAP),
          // let AuthTokenInterceptor handle the retry. Only if that fails
          // and the error propagates here, we pass it through.
          // The component/service making the request can decide how to handle it.
        }
        return throwError(() => err);
      })
    );
  }

  /**
   * Heuristic to determine if a status 0 error is likely due to IAP session expiration.
   */
  private isLikelyIapRedirect(err: HttpErrorResponse, req: HttpRequest<unknown>): boolean {
    const url = req.url || err.url || '';
    
    // Skip external URLs (CDNs, third-party APIs)
    const isExternalUrl = url.startsWith('http://') || url.startsWith('https://');
    const isSameOrigin = !isExternalUrl || 
      (typeof window !== 'undefined' && url.startsWith(window.location.origin));
    
    if (!isSameOrigin) {
      return false;
    }

    // Check error characteristics typical of CORS-blocked redirects
    const errorMessage = (err.message || '').toLowerCase();
    const hasCorsSigns = 
      errorMessage.includes('unknown error') ||
      errorMessage.includes('network error') ||
      errorMessage.includes('cors') ||
      (err.error === null && err.statusText === 'Unknown Error');

    const isNotTimeout = !errorMessage.includes('timeout');
    const isNotAbort = !errorMessage.includes('abort');

    return hasCorsSigns && isNotTimeout && isNotAbort;
  }
}