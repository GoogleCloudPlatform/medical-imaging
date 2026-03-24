import { HttpErrorResponse } from '@angular/common/http';
import { environment } from '../environments/environment';

/**
 * Extracts just the path portion from APP_BASE_SERVER_PATH.
 * Handles cases where it might be a full URL (e.g., https://example.com/dpas/)
 * or just a path (e.g., /dpas/).
 */
function extractBasePath(basePath: string): string {
  if (!basePath) return '/';
  
  // Check if it's a full URL
  try {
    const url = new URL(basePath);
    return url.pathname;
  } catch {
    // Not a valid URL, treat as path
    return basePath;
  }
}

/**
 * Strips the APP_BASE_SERVER_PATH prefix from a URL to get an Angular-relative URL.
 * Angular's router.navigateByUrl expects URLs without the base href.
 * 
 * Handles all variations of the base path:
 * - /basePath/route -> /route
 * - /basePath -> /
 * - basePath/route -> /route
 * - basePath -> /
 * 
 * @param url The URL to strip the base path from
 * @returns The URL without the base path, or undefined if it's just the base path/root
 */
export function stripBasePath(url: string): string | undefined {
  if (!url) return undefined;
  
  let result = url;
  const rawBasePath = environment.APP_BASE_SERVER_PATH;
  // Extract just the path portion in case APP_BASE_SERVER_PATH is a full URL
  const basePath = extractBasePath(rawBasePath);
  
  if (basePath && basePath !== '/') {
    // Normalize base path - remove leading/trailing slashes for comparison
    const basePathNormalized = basePath.replace(/^\/|\/$/g, '');
    
    // Handle all variations: /basePath/, /basePath, basePath/, basePath
    const patterns = [
      `/${basePathNormalized}/`,
      `/${basePathNormalized}`,
      `${basePathNormalized}/`,
      `${basePathNormalized}`,
    ];
    
    for (const pattern of patterns) {
      if (result === pattern || result.startsWith(pattern)) {
        result = result.substring(pattern.length) || '/';
        break;
      }
    }
  }
  
  // Ensure URL starts with / for Angular router
  if (result && !result.startsWith('/')) {
    result = '/' + result;
  }
  
  // Return undefined for root or empty - these aren't meaningful return URLs
  if (!result || result === '/') {
    return undefined;
  }
  
  return result;
}

/**
 * Checks if a URL is just the base path (not a meaningful route).
 */
export function isJustBasePath(url: string): boolean {
  if (!url || url === '/') return true;
  
  const rawBasePath = environment.APP_BASE_SERVER_PATH;
  const basePath = extractBasePath(rawBasePath);
  if (!basePath || basePath === '/') return url === '/';
  
  const basePathNormalized = basePath.replace(/^\/|\/$/g, '');
  return url === `/${basePathNormalized}` || url === `/${basePathNormalized}/`;
}

function parseMaybeJson(raw: any): any {
  if (typeof raw === 'string') {
    try { return JSON.parse(raw); } catch { return raw; }
  }
  return raw;
}

function unwrapPayload(raw: any): any {
  const data = parseMaybeJson(raw);

  if (Array.isArray(data)) {
    const firstObj = data.find((it: any) => it && typeof it === 'object') ?? data[0];
    const inner = firstObj?.error ?? firstObj;
    return inner ?? {};
  }

  if (data && typeof data === 'object') {
    if (data.error && typeof data.error === 'object') return data.error;
    return data;
  }

  return {};
}

export function isAuthHttpError(err: unknown): err is HttpErrorResponse {
  if (!(err instanceof HttpErrorResponse)) return false;
  if (err.status !== 401 && err.status !== 403) return false;

  const marked = Boolean(err.error && (err.error as any).__authError === true);

  const wwwAuth = (err.headers?.get('www-authenticate') || '').toLowerCase();
  const wwwAuthIndicatesOauth =
    wwwAuth.includes('bearer') &&
    (wwwAuth.includes('invalid_token') ||
     wwwAuth.includes('expired') ||
     wwwAuth.includes('invalid'));

  const payload = unwrapPayload(err.error);

  const statusStr =
    String((payload?.status ?? payload?.error?.status ?? '')).toUpperCase();
  const message =
    String(payload?.message ?? payload?.error?.message ?? '');

  const details = Array.isArray(payload?.details ?? payload?.error?.details)
    ? (payload?.details ?? payload?.error?.details)
    : [];

  const reasons: string[] = details
    .filter((d: any) => d && (d['@type']?.includes('google.rpc.ErrorInfo') || d.reason))
    .map((d: any) => String(d.reason ?? '').toUpperCase());

  const bodyIndicatesOauth =
    statusStr === 'UNAUTHENTICATED' ||
    /oauth|authentication|invalid token|expired|credentials/i.test(message) ||
    reasons.some(r =>
      r.includes('ACCESS_TOKEN') ||
      r.includes('ACCESS_TOKEN_TYPE_UNSUPPORTED') ||
      r.includes('CREDENTIAL') ||
      r.includes('UNAUTH')
    );

  return marked || wwwAuthIndicatesOauth || bodyIndicatesOauth;
}

export function markAsAuthError(err: HttpErrorResponse, urlOverride?: string): HttpErrorResponse {
  return new HttpErrorResponse({
    error: { ...(err.error ?? {}), __authError: true },
    headers: err.headers,
    status: err.status,
    statusText: err.statusText,
    url: urlOverride ?? (err.url ?? undefined),
  });
}