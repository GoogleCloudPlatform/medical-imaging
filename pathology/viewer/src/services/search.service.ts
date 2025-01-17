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

import { BehaviorSubject, EMPTY, Observable, ReplaySubject, defer, forkJoin, from, of } from 'rxjs';
import { Case, Patient, RECORD_ID_TYPE_META, RecordIdType } from '../interfaces/hierarchy_descriptor';
import { DiagnosticReport, FhirSearchResults, SearchDiagnosticReportSort } from '../interfaces/fhir_store';
import { DicomwebService, createOrUpdateCase, studyDicomModelToSlide, updatePatient } from './dicomweb.service';
import { Injectable, OnDestroy } from '@angular/core';
import { catchError, defaultIfEmpty, finalize, map, mergeAll, switchMap, takeUntil, tap, toArray } from 'rxjs/operators';

import { DialogService } from './dialog.service';
import { FhirStoreService } from './fhir-store.service';
import { LogService } from './log.service';
import { RecordIdToSlideIds } from '../interfaces/search';
import { SearchType } from '../interfaces/search';
import { environment } from '../environments/environment';
import { formatDate } from '../interfaces/dicom_descriptor';

/**
 * Search service for searching for cases and slides.
 */
@Injectable({
  providedIn: 'root'
})
export class SearchService implements OnDestroy {
  cases$ = new BehaviorSubject<Case[]>([]);
  casesByCaseId$ = new BehaviorSubject<Map<string, Case>>(new Map());
  diagnosticReports$ = new BehaviorSubject<DiagnosticReport[]>([]);
  loading$ = new BehaviorSubject<boolean>(false);
  loadingMore$ = new BehaviorSubject<boolean>(false);
  loadingText$ = new BehaviorSubject<string>('');
  nextDiagnosticReportLink$ = new BehaviorSubject<string>('');
  patient$ = new BehaviorSubject<Patient | undefined>(undefined);
  searchText$ = new BehaviorSubject<string>('');
  searchKeywords$ = new BehaviorSubject<Set<string>>(new Set());
  totalResults$ = new BehaviorSubject<number>(0);

  enableFhirSearch = Boolean(environment.FHIR_STORE_BASE_URL);

  private readonly destroyed$ = new ReplaySubject<boolean>(1);

  constructor(
    private readonly dialogService: DialogService,
    private readonly dicomwebService: DicomwebService,
    private readonly fhirStoreService: FhirStoreService,
    private readonly logService: LogService,
  ) {
    this.initializeSearchService();
  }

  initializeSearchService() {
    this.diagnosticReports$ = this.fhirStoreService.diagnosticReports$;
    this.nextDiagnosticReportLink$ =
      this.fhirStoreService.nextDiagnosticReportLink$;
    this.searchText$.pipe(takeUntil(this.destroyed$))
      .subscribe((searchText) => {
        const searchWords = searchText.replaceAll('|', ' ')
          .split(' ')
          .map((word) => word.trim())
          .filter((word) => {
            return !word.startsWith('-');
          })
          .filter((word) => word);
        const keywords = new Set<string>([...searchWords]);
        this.searchKeywords$.next(keywords);
      });
  }

  ngOnDestroy() {
    this.destroyed$.next(true);
    this.destroyed$.complete();
  }

  // Convert from record ids (clinical patient ids, case ids or slide labels) to
  // slide dicom path (also referred as slide id throughout the viewer).
  // This returns an array of dicom paths per record id. If the array is empty
  // this means the id was not found.
  getSlideDicomPathFromListOfRecordIds(ids: string[], searchType: RecordIdType):
    Observable<RecordIdToSlideIds[]> {
    const CONCURRENCY = 5;
    return from(new Set(ids))
      .pipe(
        map(recordId =>
          // Defer so it doesn't execute immediately.
          defer(
            () =>
              // Search for recordId in DICOM.
              this.dicomwebService.searchSeriesById(recordId, searchType)
                .pipe(
                  // DICOM service errors if nothing found.
                  catchError(() => {
                    return of([]);
                  }),
                  // Create a RecordIdToSlideIds for each provided ID.
                  map(dicomModelArr => ({
                    recordId,
                    // Parse the dicom result to Slides.
                    slideIds: dicomModelArr ?
                      dicomModelArr.map(
                        dicomModel => studyDicomModelToSlide(
                          environment.IMAGE_DICOM_STORE_BASE_URL!,
                          dicomModel)) :
                      []
                  }))))),
        mergeAll(CONCURRENCY),
        toArray(),
      );
  }

  getCasesByIds(caseIds: string[]): void {
    let casesByCaseIds = this.casesByCaseId$.getValue();

    const casesObservable =
      caseIds.filter((caseId) => !casesByCaseIds.has(caseId))
        .map((caseId) => {
          return this.searchCases(caseId);
        });

    forkJoin(casesObservable)
      .pipe(takeUntil(this.destroyed$))
      .subscribe((response) => {
        const modifiedResponse = response.flat(1);
        casesByCaseIds = new Map(modifiedResponse.map((res) => {
          return [res.accessionNumber, res];
        }));
        this.casesByCaseId$.next(casesByCaseIds);
      });
  }

  resetSearchResults(): void {
    this.cases$.next([]);
    this.diagnosticReports$.next([]);
    this.patient$.next(undefined);
    this.totalResults$.next(0);
    this.casesByCaseId$.next(new Map());
    this.nextDiagnosticReportLink$.next('');
  }

  searchCasesAndFhirStore(searchText: string) {
    this.searchText$.next(searchText);
    this.loading$.next(true);
    this.resetSearchResults();
    const searchTextWordCount =
      searchText.split(' ').filter((word) => word !== '').length;

    let searchObservable: Observable<Case | {
      patient?: Patient;
      cases: Case[];
    }
      | FhirSearchResults> = EMPTY;

    if (searchTextWordCount === 1) {
      // Search for patient then Cases then Fhir store.
      searchObservable =
        this.searchPatient(searchText).pipe(
          defaultIfEmpty(undefined), // Provide a default value if empty
          switchMap((patientResponse) => {
            if (patientResponse?.patient) {
              this.handleSuccessfulPatient(
                patientResponse.patient, patientResponse.cases);
              return of(patientResponse);
            }
            return this.searchCases(searchText)
              .pipe(
                switchMap(
                  (cases: Case[]):
                    Case[] | Observable<FhirSearchResults> => {
                    if (cases.length) {
                      this.handleSuccessfulCases(cases);
                      return cases;
                    }

                    if (!this.enableFhirSearch) {
                      return [];
                    }
                    return this.searchFhirStore(searchText);
                  }),
              );
          }));
    } else if (this.enableFhirSearch) {
      searchObservable = this.searchFhirStore(searchText);
    }

    return searchObservable.pipe(
      finalize(() => {
        this.loading$.next(false);
      }),
    );
  }

  searchFhirDiagnosticReportNextPage(nextPageToken: string):
    Observable<FhirSearchResults> {
    if (!nextPageToken) {
      const errorMessage = `Invalid token for next page: ${nextPageToken}`;
      throw new Error(errorMessage);
    }
    this.loadingMore$.next(true);
    return this.fhirStoreService
      .searchDiagnosticReport({
        nextPageToken,
        searchKeywords: this.searchKeywords$.getValue(),
      })
      .pipe(finalize(() => {
        this.loadingMore$.next(false);
      }));
  }

  searchSortBy(diagnosticReportsSortBy: SearchDiagnosticReportSort):
    Observable<FhirSearchResults> {
    this.loading$.next(true);
    return this.fhirStoreService
      .diagnosticReportBySort(
        diagnosticReportsSortBy, this.searchKeywords$.getValue())
      .pipe(finalize(() => {
        this.loading$.next(false);
      }));
  }

  private handleSuccessfulCases(cases: Case[]) {
    const casesByCaseIds = this.casesByCaseId$.getValue();
    cases.forEach((c) => {
      if (c.caseId) {
        casesByCaseIds.set(c.caseId, c);
      }
    });

    this.casesByCaseId$.next(casesByCaseIds);
    this.cases$.next(cases);
    this.totalResults$.next(cases.length);
  }

  private handleSuccessfulPatient(patient: Patient | undefined, cases: Case[]) {
    this.patient$.next(patient ?? undefined);
    this.handleSuccessfulCases(cases);
  }

  private searchCases(searchText: string): Observable<Case[]> {
    this.loadingText$.next('cases');
    return this.dicomwebService.searchSeriesById(searchText, SearchType.CASE_ID)
      .pipe(
        map((serieses) => {
          if (!serieses) {
            return [];
          }
          const casesByInstanceId = new Map<string, Case>();
          for (const series of serieses) {
            createOrUpdateCase(
              environment.IMAGE_DICOM_STORE_BASE_URL!, series, casesByInstanceId);
          }

          const cases = [...casesByInstanceId.values()];
          return cases;
        }),
        catchError(err => {
          const message = (typeof err === 'string' ? err : 'Error ') +
            ' while looking up ' + RECORD_ID_TYPE_META.caseId.displayText;
          this.logService.error({
            name: 'Error dialog',
            message,
            stack: 'SearchService/searchCases'
          });
          this.dialogService.error(message);
          return EMPTY;
        }));
  }

  private searchPatient(searchText: string):
    Observable<{ patient?: Patient; cases: Case[]; }> {
    if (searchText === '*') {
      return EMPTY;
    }
    this.loadingText$.next('patient');
    return this.dicomwebService
      .searchSeriesById(searchText, SearchType.PATIENT_ID)
      .pipe(
        map((serieses) => {
          if (!serieses || serieses.length === 0) {
            return { patient: undefined, cases: [] };
          }

          const cases = new Map<string, Case>();
          const patient: Patient = {};
          for (const series of serieses) {
            updatePatient(series, patient);
            createOrUpdateCase(environment.IMAGE_DICOM_STORE_BASE_URL!, series, cases);
          }

          patient.name ??= 'Unknown Patient Name';
          patient.patientId ??= 'Unknown Patient ID';
          patient.latestCaseDate = formatDate(patient.latestCaseDate);
          patient.latestCaseAccessionNumber ??= 'Unknown Case ID';
          return { patient, cases: Array.from(cases.values()) };
        }),
        catchError(err => {
          const message = (typeof err === 'string' ? err : 'Error ') +
            ' while looking up ' +
            RECORD_ID_TYPE_META.patientId.displayText;
          this.logService.error({
            name: 'Error dialog',
            message,
            stack: 'SearchService/searchPatient'
          });
          this.dialogService.error(message);
          return EMPTY;
        }));
  }

  private searchFhirStore(searchText: string): Observable<FhirSearchResults> {
    this.loadingText$.next('FHIR store');
    return this.fhirStoreService
      .searchDiagnosticReport({
        searchText,
        searchKeywords: this.searchKeywords$.getValue(),
      })
      .pipe((tap(() => {
        this.totalResults$.next(
          this.fhirStoreService.totalDiagnosticReports$.getValue());
        this.nextDiagnosticReportLink$.next(
          this.fhirStoreService.nextDiagnosticReportLink$.getValue());
        const caseIds = new Set<string>(
          this.fhirStoreService.diagnosticReports$.getValue().map(
            (report) => {
              return report.resource.caseAccessionId ??
                report.resource.id;
            }));
        this.getCasesByIds([...caseIds]);
      })));
  }
}
