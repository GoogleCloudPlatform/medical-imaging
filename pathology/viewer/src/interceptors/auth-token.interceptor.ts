import { Injectable } from '@angular/core';
import {
  HttpErrorResponse,
  HttpEvent,
  HttpHandler,
  HttpInterceptor,
  HttpRequest,
} from '@angular/common/http';
import { Observable, throwError, of, from } from 'rxjs';
import { catchError, finalize, shareReplay, switchMap } from 'rxjs/operators';
import { AuthService } from '../services/auth.service';
import { AuthStateService } from '../services/auth-state.service';

@Injectable()
export class AuthTokenInterceptor implements HttpInterceptor {
  private refreshToken$?: Observable<string>;
  private static readonly RETRY_HEADER = 'X-IAP-Retry';

  constructor(
    private readonly authService: AuthService,
    private readonly authState: AuthStateService,
  ) {}

  intercept(req: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    return this.addToken(req).pipe(
      switchMap((authedReq) => next.handle(authedReq)),
      catchError((err: unknown) => {
        if (
          err instanceof HttpErrorResponse &&
          (err.status === 401 || err.status === 403) &&
          !req.headers.has(AuthTokenInterceptor.RETRY_HEADER) // retry once per request
        ) {
          return this.refreshAndRetry(req, next, err);
        }
        return throwError(() => err);
      })
    );
  }

  private addToken(req: HttpRequest<unknown>): Observable<HttpRequest<unknown>> {
    // If an Authorization header already exists, keep it.
    const existingAuth = req.headers.get('Authorization') || req.headers.get('authorization');
    if (existingAuth) return of(req);

    // Try to get a cached token without triggering refresh; if absent, let request go without header.
    const token = this.authService['getCachedAccessToken']?.() || undefined;
    const bearer = token?.oauthTokenInfo?.token;
    if (!bearer) return of(req);

    const authed = req.clone({ setHeaders: { Authorization: `Bearer ${bearer}` } });
    return of(authed);
  }

  private refreshAndRetry(
    req: HttpRequest<unknown>,
    next: HttpHandler,
    originalError: HttpErrorResponse,
  ): Observable<HttpEvent<unknown>> {
    if (!this.refreshToken$) {
      this.authState.setReauthInProgress(true);
      // Use getOAuthToken() which will refresh if needed.
      this.refreshToken$ = this.authService.getOAuthToken().pipe(
        shareReplay(1),
        finalize(() => {
          this.refreshToken$ = undefined;
          this.authState.setReauthInProgress(false);
        }),
      );
    }

    return this.refreshToken$.pipe(
      switchMap((newToken: string) => {
        if (!newToken) return throwError(() => originalError);
        const updated = req.clone({
          setHeaders: {
            Authorization: `Bearer ${newToken}`,
            [AuthTokenInterceptor.RETRY_HEADER]: '1',
          },
        });
        return next.handle(updated);
      }),
      catchError((error) => {
        return throwError(() => originalError);
      })
    );
  }
}
