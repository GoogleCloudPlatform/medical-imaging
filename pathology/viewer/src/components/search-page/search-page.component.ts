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

import {CommonModule} from '@angular/common';
import {Component, EventEmitter, Input, OnChanges, OnDestroy, OnInit, Output} from '@angular/core';
import {FormControl, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {MatAutocompleteModule} from '@angular/material/autocomplete';
import {MatButtonModule} from '@angular/material/button';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatIconModule} from '@angular/material/icon';
import {MatInputModule} from '@angular/material/input';
import {MatTooltipModule} from '@angular/material/tooltip';
import {Title} from '@angular/platform-browser';
import {ActivatedRoute, Router, RouterModule} from '@angular/router';
import {BehaviorSubject, ReplaySubject} from 'rxjs';
import {takeUntil} from 'rxjs/operators';

import {SearchPageParams} from '../../app/app.routes';
import {environment} from '../../environments/environment';
import {DiagnosticReport, SearchDiagnosticReportSort} from '../../interfaces/fhir_store';
import {Case, Patient, RECORD_ID_TYPE_META} from '../../interfaces/hierarchy_descriptor';
import {SearchParams} from '../../interfaces/search';
import {SearchService} from '../../services/search.service';
import {SearchResultsComponent} from '../search-results/search-results.component';

/**
 * Search page component
 */
@Component({
  selector: 'search-page',
  standalone: true,
  imports: [
    SearchResultsComponent,
    CommonModule,
    MatIconModule,
    MatTooltipModule,
    FormsModule,
    ReactiveFormsModule,
    CommonModule,
    MatFormFieldModule,
    RouterModule,
    MatInputModule,
    MatButtonModule,
    MatAutocompleteModule,
  ],
  templateUrl: './search-page.component.html',
  styleUrl: './search-page.component.scss'
})
export class SearchPageComponent implements OnInit, OnChanges, OnDestroy {
  cases: Array<BehaviorSubject<Case>> = [];
  diagnosticReports: DiagnosticReport[] = [];
  loading = false;
  searchInitiated = false;
  displayHeader = true;
  patient: Patient|undefined = undefined;
  searchText = new FormControl<string>('', {nonNullable: true});

  readonly RECORD_ID_TYPE_META = RECORD_ID_TYPE_META;

  @Input() searchTextDefault?: string;
  @Input() searchOnLoad?: boolean;

  @Output() readonly searchEvent = new EventEmitter<SearchParams>();

  forceUpperCase = environment.SEARCH_UPPERCASE_ONLY;
  enableFhirSearch = false;
  enableFhirSearchTooltip = false;

  private readonly destroyed$ = new ReplaySubject<boolean>(1);
  cohortsEnabled = environment.ENABLE_COHORTS;
  viewerAppName = environment.VIEWER_APP_NAME;

  constructor(
      private readonly route: ActivatedRoute,
      private readonly router: Router,
      readonly searchService: SearchService,
      private readonly title: Title,
  ) {
    searchService.diagnosticReports$.pipe(takeUntil(this.destroyed$))
        .subscribe((diagnosticReports) => {
          this.diagnosticReports = diagnosticReports;
        });
    searchService.patient$.pipe(takeUntil(this.destroyed$))
        .subscribe((patient) => {
          this.patient = patient;
        });
  }

  ngOnInit() {
    if (!environment.IMAGE_DICOM_STORE_BASE_URL) {
      this.router.navigateByUrl('/config');
      return;
    }
    this.title.setTitle(this.viewerAppName);
    this.route.queryParams.pipe(takeUntil(this.destroyed$))
        .subscribe((params: SearchPageParams) => {
          const searchText: string = params.q ?? '';
          this.startSearch(
              searchText,
          );
        });
    this.enableFhirSearch = Boolean(environment.FHIR_STORE_BASE_URL);
    if (this.searchOnLoad && this.searchTextDefault) {
      this.search();
    }
    if (this.forceUpperCase) {
      this.searchText.valueChanges
        .pipe(takeUntil(this.destroyed$))
        .subscribe(value => {
          const upper = value.toUpperCase();
          if (value !== upper) {
            this.searchText.setValue(upper, { emitEvent: false });
          }
        });
    }
  }

  ngOnDestroy() {
    this.searchService.resetSearchResults();
    this.destroyed$.next(true);
    this.destroyed$.complete();
  }

  ngOnChanges() {
    const searchText = (this.searchTextDefault ?? '').replaceAll('\\\\', '\\');

    this.searchText.patchValue(searchText);
  }

  search() {
    if (!this.searchText) return;
    const searchText = this.forceUpperCase ?
        this.searchText.value.toUpperCase() :
        this.searchText.value;

    this.startSearch(this.cleanSearchText(searchText.trim()));
  }

  private cleanSearchText(searchText: string): string {
    searchText = searchText.replaceAll('\\', '\\\\');
    searchText = searchText.trim();

    return searchText;
  }


  loadMore(nextPageToken: string): void {
    this.searchService.searchFhirDiagnosticReportNextPage(nextPageToken)
        .pipe(takeUntil(this.destroyed$))
        .subscribe();
  }

  startSearch(searchText: string): void {
    this.searchText.patchValue(searchText);

    // Don't search with an empty query string.
    if (!searchText) {
      this.searchService.resetSearchResults();
      this.router.navigate([]);
      this.searchInitiated = false;
      this.displayHeader = true;
      return;
    }

    this.searchInitiated = true;
    this.displayHeader = false;
    if (this.route.snapshot.queryParamMap.get('q') !== searchText) {
      this.router.navigate([], {queryParams: {'q': searchText}});
    }

    this.searchService.searchCasesAndFhirStore(searchText)
        .pipe(takeUntil(this.destroyed$))
        .subscribe();
  }

  searchSortBy(sortBy: SearchDiagnosticReportSort): void {
    this.searchService.searchSortBy(sortBy)
        .pipe(takeUntil(this.destroyed$))
        .subscribe();
  }
}
