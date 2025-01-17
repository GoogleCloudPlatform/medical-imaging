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

import { BehaviorSubject, Observable, ReplaySubject } from 'rxjs';
import { DiagnosticReport, FhirSearchResults } from '../interfaces/fhir_store';
import { Injectable, OnDestroy, isDevMode } from '@angular/core';
import { catchError, finalize, map, switchMap, takeUntil, tap } from 'rxjs/operators';

import { AuthService } from './auth.service';
import { HttpClient } from '@angular/common/http';
import { LogService } from './log.service';
import { SearchDiagnosticReportSort } from '../interfaces/fhir_store';
import { WindowService } from './window.service';
import { environment } from '../environments/environment';
import { textToSnippet } from '../utils/common';

declare interface SearchParams {
  _text: string | undefined;
  _elements: string;
  _count: number | undefined;
  _sort: SearchDiagnosticReportSort | undefined;
}

enum DiagnosticReportLinkRelation {
  SEARCH = 'search',
  NEXT = 'next',
}

declare interface SearchDiagnosticReportParams {
  nextPageToken?: string;             // Token to fetch next page
  pageSize?: number;                  // Max items in a single page
  searchKeywords: Set<string>;        // Unique words in search
  searchText?: string;                // Text to search
  sort?: SearchDiagnosticReportSort;  // Available sorting for search
  sortByParams?: string;              // Modified sort by search link
}

const DEFAULT_PAGE_SIZE = 20;
const DEFAULT_SNIPPET_WORDS_PER_LINE = 95;
const DEFAULT_SNIPPET_LINES = 4;
const DEFAULT_SNIPPET_LENGTH =
  DEFAULT_SNIPPET_WORDS_PER_LINE * DEFAULT_SNIPPET_LINES;
const SORT_DELIMITER = '&_sort=';


/** Fhir search results */
@Injectable({
  providedIn: 'root'
})
export class FhirStoreService
  implements OnDestroy {
  diagnosticReports$ = new BehaviorSubject<DiagnosticReport[]>([]);
  loadingDiagnosticReports$ = new BehaviorSubject<boolean>(false);
  loadingMoreDiagnosticReports$ = new BehaviorSubject<boolean>(false);
  nextDiagnosticReportLink$ = new BehaviorSubject<string>('');
  searchResults$ = new BehaviorSubject<FhirSearchResults | undefined>(undefined);
  totalDiagnosticReports$ = new BehaviorSubject<number>(0);

  private readonly destroyed$ = new ReplaySubject<boolean>(1);

  constructor(
    private readonly authService: AuthService,
    private readonly http: HttpClient,
    private readonly logService: LogService,
    private readonly windowService: WindowService,
  ) { }

  ngOnDestroy() {
    this.destroyed$.next(true);
    this.destroyed$.complete();
  }

  searchDiagnosticReport(params: SearchDiagnosticReportParams):
    Observable<FhirSearchResults> {
    if (!params.nextPageToken && !params.searchText && !params.sortByParams) {
      const errorMessage =
        `Invalid call to searchDiagnosticReport: ${JSON.stringify(params)}`;
      this.logService.error(
        { name: `searchDiagnosticReport`, message: errorMessage });
      throw new Error(errorMessage);
    }

    return this.authService.getOAuthToken().pipe(
      takeUntil(this.destroyed$), switchMap((accessToken) => {
        const headers: {
          authorization: string,
          prefer?: string,
        } = {
          'authorization': 'Bearer ' + accessToken,
          prefer: 'handling=strict',
        };

        if (!isDevMode()) {
          delete headers.prefer;
        }

        const searchUrl = this.computeSearchDiagnosticReportURL(params);

        if (!this.loadingMoreDiagnosticReports$.getValue()) {
          this.loadingDiagnosticReports$.next(true);
        }
        return this.http
          .get<FhirSearchResults>(
            searchUrl, { headers, withCredentials: false })
          .pipe(
            takeUntil(this.destroyed$),
            map((searchResults: FhirSearchResults) => {
              if (searchResults.entry) {
                const searchKeywords = params.searchKeywords ?? new Set();
                searchResults.entry = this.sanitizeDiagnosticReports(
                  searchResults.entry, searchKeywords);
              } else {
                searchResults.entry = [];
              }

              return searchResults;
            }),
            tap((modifiedSearchResults: FhirSearchResults) => {
              this.updateDiagnosticReportSearchResults(
                modifiedSearchResults, params.nextPageToken);
            }),
            catchError((error) => {
              this.resetSearchResults();
              error = JSON.stringify(error);
              this.logService.error(
                { name: `httpRequest: "${searchUrl}"`, message: error });
              let errorMessage =
                'Error while fetching search results from FHIR store.';
              if (params.nextPageToken) {
                errorMessage =
                  'Error while fetching next page search results from FHIR store.';
              }
              throw new Error(errorMessage);
            }),
            finalize(() => {
              this.loadingDiagnosticReports$.next(false);
            }),
          );
      }));
  }

  diagnosticReportNextPage(nextPageLink: string, searchKeywords: Set<string>):
    Observable<FhirSearchResults> {
    if (!nextPageLink) {
      const errorMessage = 'Invalid next page token';
      this.logService.error({
        name: `nextDiagnosticReportLink: "${nextPageLink}"`,
        message: errorMessage
      });

      throw new Error(errorMessage);
    }

    const nextPageToken = this.parseUrlParams(nextPageLink);
    this.loadingMoreDiagnosticReports$.next(true);
    return this.searchDiagnosticReport({ nextPageToken, searchKeywords })
      .pipe(
        finalize(() => {
          this.loadingMoreDiagnosticReports$.next(false);
        }),
      );
  }

  diagnosticReportBySort(
    diagnosticReportsSortBy: SearchDiagnosticReportSort,
    searchKeywords: Set<string>,
  ) {
    const searchLink: string =
      (this.searchResults$?.getValue() ?? {} as FhirSearchResults)
        .link
        .find(({ relation }) => {
          return relation === DiagnosticReportLinkRelation.SEARCH;
        })
        ?.url ??
      '';
    if (!searchLink) {
      const errorMessage =
        'Invalid call to diagnosticReportBySort - cannot find previous search link.';
      this.logService.error(
        { name: `searchDiagnosticReport`, message: errorMessage });
      throw new Error(errorMessage);
    }
    // Replace sort param in previous search url
    const sortParams = searchLink.slice(
      searchLink.indexOf(SORT_DELIMITER) + SORT_DELIMITER.length);
    const prevSort = sortParams.slice(0, sortParams.indexOf('&'));
    const indexOfPrevSort = searchLink.indexOf(prevSort);
    const beforePrevSort = searchLink.slice(0, indexOfPrevSort);
    const afterPrevSort = searchLink.slice(indexOfPrevSort + prevSort.length);

    const sortByParamsUrl =
      `${beforePrevSort}${diagnosticReportsSortBy}${afterPrevSort}`;
    const parsedSortByParams = this.parseUrlParams(sortByParamsUrl);

    return this.searchDiagnosticReport(
      { sortByParams: parsedSortByParams, searchKeywords });
  }

  private computeSearchDiagnosticReportURL(
    { searchText, nextPageToken, pageSize, sort, sortByParams }:
      SearchDiagnosticReportParams): string {
    if (searchText) {
      pageSize = pageSize ?? DEFAULT_PAGE_SIZE;
      sort = sort ?? SearchDiagnosticReportSort.LAST_MODIFIED_DESC;
    }
    let searchParmQuery = '';
    if (sortByParams) {
      searchParmQuery = sortByParams;
    } else if (!nextPageToken) {
      const searchParams: SearchParams = {
        _text: searchText,             // Search text
        _elements: 'text,identifier',  // Properties to fetch
        _count: pageSize,
        _sort: sort,
      };
      searchParmQuery =
        Object.entries(searchParams)
          .reduce((prev, curr) => `${prev}&${curr.join('=')}`, '');


    } else {
      searchParmQuery = this.parseUrlParams(nextPageToken);
    }

    const resultUrl =
      `${environment.FHIR_STORE_BASE_URL}DiagnosticReport?${searchParmQuery}${environment.FHIR_STORE_SEARCH_QUERY_PARAMETERS}`;
    return resultUrl;
  }

  private parseUrlParams(token: string): string {
    const delimiter = 'DiagnosticReport/?';
    const parsedToken =
      token.slice(token.indexOf(delimiter) + delimiter.length);

    return parsedToken;
  }

  private resetSearchResults(): void {
    this.searchResults$.next(undefined);
    this.nextDiagnosticReportLink$.next('');
    this.totalDiagnosticReports$.next(0);
  }

  private sanitizeDiagnosticReports(
    diagnosticReports: DiagnosticReport[],
    keywords: Set<string>,
  ): DiagnosticReport[] {
    // Sanitize display text notes
    return diagnosticReports.map((diagnosticReport: DiagnosticReport) => {
      const reportText = diagnosticReport.resource.text;
      if (reportText) {
        reportText.sanitizedHtml = reportText.div;
        reportText.tagsRemovedSanitized = this.windowService.extractContent(reportText.sanitizedHtml);
        reportText.snippet = textToSnippet(
          keywords, reportText.tagsRemovedSanitized,
          (DEFAULT_SNIPPET_LENGTH));
      }

      diagnosticReport.resource.caseAccessionId =
        (diagnosticReport.resource?.identifier ?? [])[0]?.value ?? '';

      return diagnosticReport;
    });
  }

  private updateDiagnosticReportSearchResults(
    modifiedSearchResults: FhirSearchResults,
    nextPageToken?: string,
  ): void {
    if (nextPageToken) {
      const prevDiagnosticReports = this.diagnosticReports$.getValue();
      this.diagnosticReports$.next(
        [...prevDiagnosticReports, ...modifiedSearchResults.entry]);
    } else {
      this.diagnosticReports$.next(modifiedSearchResults.entry);
      this.totalDiagnosticReports$.next(modifiedSearchResults.total);
    }

    this.searchResults$.next(modifiedSearchResults);
    const nextPageLink =
      modifiedSearchResults.link
        .find(
          ({ relation }) => relation === DiagnosticReportLinkRelation.NEXT)
        ?.url ??
      '';
    this.nextDiagnosticReportLink$.next(nextPageLink);
  }
}
