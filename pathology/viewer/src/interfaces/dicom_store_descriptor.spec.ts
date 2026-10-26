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

/// <reference types="node" />
import {HttpClient, HttpErrorResponse, HttpHandler, HttpRequest, HttpResponse} from '@angular/common/http';
import {ElementRef, ChangeDetectorRef} from '@angular/core';
import {ActivatedRoute, Router} from '@angular/router';
import {BehaviorSubject, of, throwError} from 'rxjs';

import {InspectPageComponent} from '../components/inspect-page/inspect-page.component';
import {environment} from '../environments/environment';
import {AuthTokenInterceptor} from '../interceptors/auth-token.interceptor';
import {AuthStateService} from '../services/auth-state.service';
import {AuthService} from '../services/auth.service';
import {CohortService} from '../services/cohort.service';
import {DialogService} from '../services/dialog.service';
import {DicomAnnotationsService} from '../services/dicom-annotations.service';
import {DicomwebService} from '../services/dicomweb.service';
import {LogService} from '../services/log.service';
import {SlideApiService} from '../services/slide-api.service';
import {SlideDataService} from '../services/slide-data.service';
import {UserService} from '../services/user.service';
import {ImageViewerPageStore} from '../stores/image-viewer-page.store';
import {SlideInfo} from './slide_descriptor';
import {
  addDicomStorePrefixIfMissing,
  DICOM_STORE,
  dicomIdToDicomStore,
  DicomStores,
  isAllowedAuthenticatedRequestUrl,
  isAllowedDicomWebRequestUrl,
  isSlideDeId,
  resetDicomStores,
} from './dicom_store_descriptor';

const PRIMARY_STORE =
    'https://healthcare.googleapis.com/v1/projects/hosp-proj/locations/us/datasets/ds/dicomStores/main-store/dicomWeb';
const DEID_STORE =
    'https://healthcare.googleapis.com/v1/projects/hosp-proj/locations/us/datasets/ds/dicomStores/deid-store/dicomWeb';
const ANNOTATIONS_STORE =
    'https://healthcare.googleapis.com/v1/projects/hosp-proj/locations/us/datasets/ds/dicomStores/ann-store/dicomWeb';
const FHIR_STORE =
    'https://healthcare.googleapis.com/v1/projects/hosp-proj/locations/us/datasets/ds/fhirStores/fhir-store/fhir';
const ORCHESTRATOR_URL = 'https://orchestrator.hospital.org/api';

const VALID_SERIES_1 = `${PRIMARY_STORE}/studies/1.2.3/series/4.5.6`;
const VALID_SERIES_2 = `${PRIMARY_STORE}/studies/1.2.3/series/7.8.9`;
const VALID_STUDY_1 = `${PRIMARY_STORE}/studies/1.2.3`;
const VALID_DEID_SERIES = `${DEID_STORE}/studies/9.8.7/series/6.5.4`;

describe('OAuth Token Leak Prevention & DICOM Store URL Validation (b/543919319)', () => {
  beforeEach(() => {
    environment.IMAGE_DICOM_STORE_BASE_URL = PRIMARY_STORE;
    environment.IMAGE_DEID_DICOM_STORE_BASE_URL = '';
    environment.ANNOTATIONS_DICOM_STORE_BASE_URL = ANNOTATIONS_STORE;
    environment.ANNOTATIONS_DICOM_STORE_PARENT = '';
    environment.FHIR_STORE_BASE_URL = FHIR_STORE;
    environment.ORCHESTRATOR_BASE_URL = ORCHESTRATOR_URL;
    resetDicomStores();
  });

  describe('Layer 1: dicomIdToDicomStore & addDicomStorePrefixIfMissing', () => {
    it('accepts valid study, series, and instance URLs for configured stores', () => {
      expect(dicomIdToDicomStore(VALID_STUDY_1)).toBe(PRIMARY_STORE);
      expect(dicomIdToDicomStore(VALID_SERIES_1)).toBe(PRIMARY_STORE);
      expect(dicomIdToDicomStore(`${VALID_SERIES_1}/instances/1.1.1`))
          .toBe(PRIMARY_STORE);
    });

    it('Scenario 1: rejects comma-separated multi-view series strings with attacker URL', () => {
      const craftedSeries =
          `${VALID_SERIES_1},https://attacker.example.com/capture`;
      expect(() => dicomIdToDicomStore(craftedSeries)).toThrow();
    });

    it('Scenario 2A: does not match empty default DEID store (startsWith("") bypass)', () => {
      expect(DICOM_STORE[DicomStores.DEID]).toBe('');
      expect(() => dicomIdToDicomStore(
                 'https://attacker.example.com/studies/1.2.3/series/4.5.6'))
          .toThrow();
      expect(isSlideDeId(
                 'https://attacker.example.com/studies/1.2.3/series/4.5.6'))
          .toBeFalse();
    });

    it('Scenario 2B: rejects subdomain, userinfo (@), credentials, and prefix boundary confusion', () => {
      // Subdomain confusion
      expect(() => dicomIdToDicomStore(
                 'https://healthcare.googleapis.com.attacker.com/v1/projects/hosp-proj/locations/us/datasets/ds/dicomStores/main-store/dicomWeb/studies/1.2/series/3.4'))
          .toThrow();

      // Userinfo @ host confusion
      expect(() => dicomIdToDicomStore(
                 'https://healthcare.googleapis.com@attacker.com/v1/projects/hosp-proj/locations/us/datasets/ds/dicomStores/main-store/dicomWeb/studies/1.2/series/3.4'))
          .toThrow();

      // Embedded credentials on valid host
      expect(() => dicomIdToDicomStore(
                 'https://user:pass@healthcare.googleapis.com/v1/projects/hosp-proj/locations/us/datasets/ds/dicomStores/main-store/dicomWeb/studies/1.2/series/3.4'))
          .toThrow();

      // Prefix boundary confusion (/dicomWebEvil vs /dicomWeb)
      expect(() => dicomIdToDicomStore(
                 `${PRIMARY_STORE}Evil/studies/1.2.3/series/4.5.6`))
          .toThrow();
    });

    it('Scenario 2B: rejects raw and percent-encoded path traversal segments', () => {
      expect(() => dicomIdToDicomStore(
                 `${PRIMARY_STORE}/../../other-store/dicomWeb/studies/1.2/series/3.4`))
          .toThrow();
      expect(() => dicomIdToDicomStore(
                 `${PRIMARY_STORE}/studies/1.2/series/../../other`))
          .toThrow();
      expect(() => dicomIdToDicomStore(
                 `${PRIMARY_STORE}/%2e%2e/studies/1.2/series/3.4`))
          .toThrow();
      expect(() => dicomIdToDicomStore(
                 `${PRIMARY_STORE}/studies/1.2%2f..%2f/series/3.4`))
          .toThrow();
    });

    it('rejects query parameters, fragments, and non-DICOM resource paths in slide IDs', () => {
      expect(() => dicomIdToDicomStore(`${VALID_SERIES_1}?redirect=https://attacker.com`))
          .toThrow();
      expect(() => dicomIdToDicomStore(`${VALID_SERIES_1}#fragment`))
          .toThrow();
      expect(() => dicomIdToDicomStore(`${PRIMARY_STORE}/arbitrary/path`))
          .toThrow();
    });

    it('normalizes legacy partial paths without rewriting full attacker URLs', () => {
      expect(addDicomStorePrefixIfMissing('/studies/1.2.3/series/4.5.6'))
          .toBe(VALID_SERIES_1);
      expect(addDicomStorePrefixIfMissing(
                 'projects/hosp-proj/locations/us/datasets/ds/dicomStores/main-store/dicomWeb/studies/1.2.3/series/4.5.6'))
          .toBe(VALID_SERIES_1);

      const attackerUrl = 'https://attacker.example.com/studies/1.2.3/series/4.5.6';
      expect(addDicomStorePrefixIfMissing(attackerUrl)).toBe(attackerUrl);
    });

    it('identifies slides in configured DEID store only when DEID store is non-empty', () => {
      environment.IMAGE_DEID_DICOM_STORE_BASE_URL = DEID_STORE;
      resetDicomStores();
      expect(isSlideDeId(VALID_DEID_SERIES)).toBeTrue();
      expect(isSlideDeId(VALID_SERIES_1)).toBeFalse();
    });
  });

  describe('Layer 2: ImageViewerPageStore route handling (Scenarios 1 & 3)', () => {
    let queryParams$: BehaviorSubject<Record<string, string>>;
    let routerSpy: jasmine.SpyObj<Router>;
    let dialogSpy: jasmine.SpyObj<DialogService>;
    let slideApiSpy: jasmine.SpyObj<SlideApiService>;
    let store: ImageViewerPageStore;

    beforeEach(() => {
      queryParams$ = new BehaviorSubject<Record<string, string>>({});
      routerSpy = jasmine.createSpyObj<Router>('Router', ['navigate'], {
        url: '/viewer?series=test',
      });
      dialogSpy = jasmine.createSpyObj<DialogService>('DialogService', ['error']);
      dialogSpy.error.and.returnValue(of(true));
      slideApiSpy = jasmine.createSpyObj<SlideApiService>(
          'SlideApiService', ['getSlidesForCase']);
      slideApiSpy.getSlidesForCase.and.returnValue(of([]));

      const cohortSpy = {
        fetchPathologyCohort: jasmine.createSpy('fetchPathologyCohort'),
        selectedPathologyCohort$: new BehaviorSubject(undefined),
        loading$: new BehaviorSubject(false),
      } as unknown as CohortService;
      const dicomAnnotationsSpy = {
        fetchDicomAnnotationInstances: jasmine.createSpy('fetchDicomAnnotationInstances'),
        getDeduplicatedInstancesMap: jasmine.createSpy('getDeduplicatedInstancesMap'),
        fetchDicomAnnotationModels: jasmine.createSpy('fetchDicomAnnotationModels'),
        loadingDicomAnnotations$: new BehaviorSubject(false),
      } as unknown as DicomAnnotationsService;
      const slideDataSpy = {
        fetchSlideInfo: jasmine.createSpy('fetchSlideInfo').and.returnValue(of({} as never)),
        fetchSlideExtraMetadata:
            jasmine.createSpy('fetchSlideExtraMetadata').and.returnValue(of(new Map())),
      } as unknown as SlideDataService;
      const userSpy = {
        getCurrentUser$: jasmine.createSpy('getCurrentUser$').and.returnValue(of('clinician@hospital.org')),
        getCurrentUserHash$: jasmine.createSpy('getCurrentUserHash$').and.returnValue(of('hash')),
      } as unknown as UserService;

      store = new ImageViewerPageStore(
          {queryParams: queryParams$.asObservable()} as unknown as ActivatedRoute,
          cohortSpy,
          dialogSpy,
          dicomAnnotationsSpy,
          userSpy,
          slideApiSpy,
          routerSpy,
          slideDataSpy,
      );
    });

    it('Scenario 1: blocks split-view comma-separated series containing an attacker URL', () => {
      queryParams$.next({
        series: `${VALID_SERIES_1},https://attacker.example.com/capture`,
      });

      expect(dialogSpy.error).toHaveBeenCalled();
      expect(routerSpy.navigate).toHaveBeenCalledWith(['/search']);
      expect(slideApiSpy.getSlidesForCase).not.toHaveBeenCalled();
      expect(store.splitViewSlideDescriptors$.value).toEqual([]);
    });

    it('Scenario 3: blocks pre-split caseId extraction when first element lacks /series/', () => {
      queryParams$.next({
        series: `${PRIMARY_STORE},https://attacker.example.com/series/1.2`,
      });

      expect(dialogSpy.error).toHaveBeenCalled();
      expect(slideApiSpy.getSlidesForCase).not.toHaveBeenCalled();
      expect(store.caseId).toBe('');
    });

    it('loads split-view descriptors when all comma-separated series belong to configured stores', () => {
      queryParams$.next({
        series: `${VALID_SERIES_1},${VALID_SERIES_2}`,
      });

      expect(dialogSpy.error).not.toHaveBeenCalled();
      expect(slideApiSpy.getSlidesForCase).toHaveBeenCalledWith(VALID_STUDY_1);
      expect(store.splitViewSlideDescriptors$.value).toEqual([
        {id: VALID_SERIES_1},
        {id: VALID_SERIES_2},
      ]);
    });
  });

  describe('Layer 3: InspectPageComponent route validation (Scenario 4)', () => {
    let slideApiSpy: jasmine.SpyObj<SlideApiService>;
    let component: InspectPageComponent;

    beforeEach(() => {
      slideApiSpy = jasmine.createSpyObj<SlideApiService>(
          'SlideApiService', ['getSlideInfo']);
      slideApiSpy.getSlideInfo.and.returnValue(
          of({levelMap: []} as unknown as SlideInfo));

      component = new InspectPageComponent(
          {nativeElement: document.createElement('div')} as ElementRef<HTMLElement>,
          {} as DicomwebService,
          slideApiSpy,
          {markForCheck: () => {}} as ChangeDetectorRef,
          {queryParams: of({})} as unknown as ActivatedRoute,
          jasmine.createSpyObj<Router>('Router', ['navigate']),
      );
    });

    it('Scenario 4: blocks attacker series URL in /inspect without calling getSlideInfo', () => {
      component.urlSeriesUid = 'https://attacker.example.com/capture';
      component.load();

      expect(slideApiSpy.getSlideInfo).not.toHaveBeenCalled();
      expect(component.errorMsg).toContain('Invalid Dicom id prefix');
    });

    it('allows valid configured store series URL in /inspect', () => {
      component.urlSeriesUid = VALID_SERIES_1;
      component.load();

      expect(slideApiSpy.getSlideInfo).toHaveBeenCalledWith(VALID_SERIES_1);
    });
  });

  describe('Layer 4: DicomwebService & AuthTokenInterceptor outbound token scoping (Scenario 5)', () => {
    let httpSpy: jasmine.SpyObj<HttpClient>;
    let authSpy: jasmine.SpyObj<AuthService>;
    let logSpy: jasmine.SpyObj<LogService>;
    let dicomwebService: DicomwebService;

    beforeEach(() => {
      httpSpy = jasmine.createSpyObj<HttpClient>('HttpClient', ['get']);
      authSpy = jasmine.createSpyObj<AuthService>('AuthService', ['getOAuthToken']);
      authSpy.getOAuthToken.and.returnValue(of('mock-oauth-token'));
      logSpy = jasmine.createSpyObj<LogService>('LogService', ['error']);
      dicomwebService = new DicomwebService(httpSpy, authSpy, logSpy);
    });

    it('Scenario 5: DicomwebService blocks metadata & tile requests to untrusted URLs before getting OAuth token', () => {
      let metadataError: unknown;
      dicomwebService
          .getInstancesMetadata('https://attacker.example.com/capture', [])
          .subscribe({error: (err) => (metadataError = err)});

      expect(metadataError).toBeDefined();
      expect(authSpy.getOAuthToken).not.toHaveBeenCalled();
      expect(httpSpy.get).not.toHaveBeenCalled();

      let tileError: unknown;
      dicomwebService
          .getEncodedImageTile('https://attacker.example.com/capture', '1.2.3', 1)
          .subscribe({error: (err) => (tileError = err)});

      expect(tileError).toBeDefined();
      expect(authSpy.getOAuthToken).not.toHaveBeenCalled();
      expect(httpSpy.get).not.toHaveBeenCalled();
    });

    it('DicomwebService allows valid configured DICOM store requests and attaches Bearer token', () => {
      httpSpy.get.and.returnValue(of('[]' as never));
      dicomwebService.getInstancesMetadata(VALID_SERIES_1, ['0020000D']).subscribe();

      expect(authSpy.getOAuthToken).toHaveBeenCalledTimes(1);
      expect(httpSpy.get).toHaveBeenCalledTimes(1);
      const [calledUrl, options] = httpSpy.get.calls.mostRecent().args;
      expect(calledUrl).toBe(`${VALID_SERIES_1}/instances?includefield=0020000D`);
      expect((options as {headers: Record<string, string>}).headers['Authorization'])
          .toBe('Bearer mock-oauth-token');
    });

    it('AuthTokenInterceptor withholds OAuth token from untrusted origins and attaches it to configured stores', () => {
      const cachedAuthService = {
        getCachedAccessToken: () => ({
          oauthTokenInfo: {token: 'mock-cached-token'},
        }),
        getOAuthToken: () => of('mock-refreshed-token'),
      } as unknown as AuthService;
      const authStateSpy = jasmine.createSpyObj<AuthStateService>(
          'AuthStateService', ['setReauthInProgress']);
      const interceptor = new AuthTokenInterceptor(cachedAuthService, authStateSpy);

      let capturedUntrustedReq: HttpRequest<unknown> | undefined;
      const untrustedHandler: HttpHandler = {
        handle: (req) => {
          capturedUntrustedReq = req;
          return of(new HttpResponse({status: 200}));
        },
      };
      interceptor
          .intercept(
              new HttpRequest('GET', 'https://attacker.example.com/capture'),
              untrustedHandler)
          .subscribe();
      expect(capturedUntrustedReq?.headers.get('Authorization')).toBeNull();

      // Verify 401 on untrusted URL does NOT trigger refreshAndRetry with token
      const untrusted401Handler: HttpHandler = {
        handle: () =>
            throwError(() => new HttpErrorResponse({status: 401, url: 'https://attacker.example.com'})),
      };
      let untrusted401Err: unknown;
      interceptor
          .intercept(
              new HttpRequest('GET', 'https://attacker.example.com/capture'),
              untrusted401Handler)
          .subscribe({error: (err) => (untrusted401Err = err)});
      expect(untrusted401Err).toBeDefined();
      expect(authStateSpy.setReauthInProgress).not.toHaveBeenCalled();

      // Verify trusted configured store request receives Authorization header
      let capturedTrustedReq: HttpRequest<unknown> | undefined;
      const trustedHandler: HttpHandler = {
        handle: (req) => {
          capturedTrustedReq = req;
          return of(new HttpResponse({status: 200}));
        },
      };
      interceptor
          .intercept(
              new HttpRequest('GET', `${VALID_SERIES_1}/instances`),
              trustedHandler)
          .subscribe();
      expect(capturedTrustedReq?.headers.get('Authorization'))
          .toBe('Bearer mock-cached-token');
    });

    it('validates outbound DICOMweb and authenticated backend URLs accurately', () => {
      expect(isAllowedDicomWebRequestUrl(`${VALID_SERIES_1}/instances?includefield=0020000D`))
          .toBeTrue();
      expect(isAllowedDicomWebRequestUrl(`${PRIMARY_STORE}/series?modality=SM`))
          .toBeTrue();
      expect(isAllowedDicomWebRequestUrl('https://attacker.example.com/studies/1.2/instances'))
          .toBeFalse();

      expect(isAllowedAuthenticatedRequestUrl(`${FHIR_STORE}/DiagnosticReport`))
          .toBeTrue();
      expect(isAllowedAuthenticatedRequestUrl(`${ORCHESTRATOR_URL}/cohorts`))
          .toBeTrue();
      expect(isAllowedAuthenticatedRequestUrl('https://attacker.example.com/api'))
          .toBeFalse();
    });
  });
});
