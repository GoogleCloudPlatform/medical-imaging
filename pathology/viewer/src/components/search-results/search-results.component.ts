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
import {Component, EventEmitter, Input, OnChanges, Output, SimpleChanges, TemplateRef, ViewChild} from '@angular/core';
import {MatButtonModule} from '@angular/material/button';
import {MatOptionModule} from '@angular/material/core';
import {MatDialogModule} from '@angular/material/dialog';
import {MatIconModule} from '@angular/material/icon';
import {MatProgressSpinnerModule} from '@angular/material/progress-spinner';
import {MatSelectModule} from '@angular/material/select';
import {MatSnackBar} from '@angular/material/snack-bar';
import {RouterModule} from '@angular/router';
import {take} from 'rxjs/operators';
import {setAnchorHref} from 'safevalues/dom';

import {DiagnosticReport, SearchDiagnosticReportSort} from '../../interfaces/fhir_store';
import {Case, type Patient} from '../../interfaces/hierarchy_descriptor';
import {CaseWidget} from '../../interfaces/search';
import {DICOMUriToSlideDescriptorPipe} from '../../pipes/dicom-uri-to-slide-descriptor.pipe';
import {HighlightPipe} from '../../pipes/highlight.pipe';
import {PathologySlideToViewerParamsPipe} from '../../pipes/pathology-slide-to-viewer-params.pipe';
import {DialogService} from '../../services/dialog.service';
import {ImageViewerQuickViewComponent} from '../image-viewer-quick-view/image-viewer-quick-view.component';


/**
 * List of cases (e.g. search results, cohort view, etc.).
 * Not to be confused with the viewer Case View (components/case_view).
 */
@Component({
  selector: 'search-results',
  standalone: true,
  imports: [
    MatDialogModule,
    HighlightPipe,
    CommonModule,
    MatProgressSpinnerModule,
    MatIconModule,
    DICOMUriToSlideDescriptorPipe,
    ImageViewerQuickViewComponent,
    PathologySlideToViewerParamsPipe,
    RouterModule,
    MatOptionModule,
    MatSelectModule,
    MatButtonModule,
  ],
  templateUrl: './search-results.component.html',
  styleUrl: './search-results.component.scss'
})
export class SearchResultsComponent implements OnChanges {
  @ViewChild('readDetailsTemplate', {static: true})
  readDetailsTemplate!: TemplateRef<CaseWidget>;

  @Input() allowDownloading = false;
  @Input() allowRemoving = false;
  @Input() allowSorting = false;
  @Input() allowSelecting = false;
  @Input() cases: Case[] = [];
  @Input() casesByCaseId?: Map<string, Case> = new Map<string, Case>();
  @Input() cohortName = '';
  @Input() diagnosticReports: DiagnosticReport[] = [];
  @Input() emptyMessage = '';
  @Input() showText = true;
  @Input() nextPageToken = '';
  @Input() linkToken = '';
  @Input() loading = false;
  @Input() loadingProgressSelectedPathologyCohortCases = 0;
  @Input() loadingText = '';
  @Input() loadingMore = false;
  @Input() patient?: Patient = undefined;
  @Input() searchText = '';
  @Input() showEmptyMessage = false;
  @Input() totalResults = 0;
  @Input() twoColumns = false;


  @Output('loadMore') readonly loadMore = new EventEmitter<string>();
  @Output('removeCasesFromCohort')
  readonly removeCasesFromCohort =
      new EventEmitter<{cohortName: string; selectedCaseIds: Set<string>;}>();
  @Output('searchSortBy')
  readonly searchSortBy = new EventEmitter<SearchDiagnosticReportSort>();

  allCasesSelected = false;
  caseWidgets: CaseWidget[] = [];
  diagnosticReportsSortBy: SearchDiagnosticReportSort =
      SearchDiagnosticReportSort.LAST_MODIFIED_DESC;
  selectedCases = new Set<CaseWidget>();
  searchedKeywords = new Set<string>();

  readonly searchDiagnosticReportSort = SearchDiagnosticReportSort;

  constructor(
      private readonly dialogService: DialogService,
      private readonly snackBar: MatSnackBar,
  ) {}

  ngOnChanges(changes: SimpleChanges) {
    const cases = changes['cases']?.currentValue;
    const casesByCaseId = changes['casesByCaseId']?.currentValue;
    const diagnosticReports = changes['diagnosticReports']?.currentValue;
    const searchText = changes['searchText']?.currentValue;

    if (diagnosticReports || cases || casesByCaseId) {
      this.caseWidgets = [
        ...this.casesToCaseWidgets(this.cases),
        ...this.diagnosticReportsToCaseWidgets(
            this.diagnosticReports,
            this.casesByCaseId ?? new Map(),
            ),
      ];

      // Search page results should not be sorted as we can load more results.
      if (!this.searchText) {
        this.caseWidgets.sort((a, b) => {
          const aDate =
              this.isDate(a.caseDate) ? new Date(a.caseDate).getTime() : 0;
          const bDate =
              this.isDate(b.caseDate) ? new Date(b.caseDate).getTime() : 0;

          // Sort by date and then id
          return (this.diagnosticReportsSortBy ===
                          SearchDiagnosticReportSort.LAST_MODIFIED_DESC ?
                      bDate - aDate :
                      aDate - bDate) ||
              a.id.localeCompare(b.id);
        });
      }

      this.selectedCases.clear();
      this.validateAllSelected();
    }
    if (searchText) {
      this.searchedKeywords =
          new Set(this.searchText.split(' ').map(word => word.trim()));
    }
  }

  downloadSelected(): void {
    // Ex: 'Cases - 2022-10-25' (year-month-day)
    const fileName = `Cases - ${new Date().toLocaleDateString('en-CA')}.csv`;
    try {
      // Setup data for csv
      const headers = [
        'CaseId',
        'DicomUris',
      ];
      const content: string[][] = [...this.selectedCases].map((widgetData) => {
        if (widgetData.id === widgetData.diagnosticReport?.resource?.id) {
          return ['Unknown ID'];
        }
        const slidesUris: string[] =
            widgetData.slides.map(slide => slide.dicomUri)
                .filter((uri): uri is string => !!uri);
        return [
          widgetData.id,
          slidesUris.join(', '),
        ];
      });

      const rows: string[][] = [headers, ...content];
      const csvContent = 'data:text/csv;charset=utf-8,' +
          rows.map(e => e.join(',')).join('\n');

      // Setup downloading csv
      const encodedUri = encodeURI(csvContent);
      const link = document.createElement('a');
      setAnchorHref(link, encodedUri);
      link.download = fileName;
      link.style.visibility = 'hidden';

      // Download csv
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      this.snackBar.open(`Downloaded cases: ${fileName}`);
    } catch (error) {
      this.snackBar.open(`Failed to downloaded cases: ${fileName}`);
    }
  }

  handleLoadMore(nextPageToken: string): void {
    this.loadMore.emit(nextPageToken);
  }

  isDate(date: string): boolean {
    const parsedDate = Date.parse(date);
    return !isNaN(parsedDate);
  }

  openReadDetailsDialog(caseWidget: CaseWidget, event: Event): void {
    event.stopPropagation();
    this.dialogService
        .openComponentDialog(this.readDetailsTemplate, {
          data: caseWidget,
          autoFocus: false,
        })
        .afterClosed()
        .pipe(take(1))
        .subscribe();
  }

  removeCases(): void {
    this.snackBar.open('Removing cases...');

    const selectedCaseIds: string[] =
        [...this.selectedCases]
            .map((widgetInfo) => widgetInfo?.caseInfo?.caseId)
            .filter((id): id is string => !!id);

    this.removeCasesFromCohort.emit({
      cohortName: this.cohortName,
      selectedCaseIds: new Set<string>(selectedCaseIds),
    });

    this.selectedCases.clear();
  }

  searchBySort(diagnosticReportsSortBy: SearchDiagnosticReportSort): void {
    this.searchSortBy.emit(diagnosticReportsSortBy);
  }

  selectAll(): boolean {
    const allCasesSelected = this.validateAllSelected();
    if (allCasesSelected) {
      this.selectedCases.clear();
      this.allCasesSelected = false;
    } else {
      this.selectedCases = new Set(this.caseWidgets);
      this.allCasesSelected = true;
    }

    return this.allCasesSelected;
  }

  toggleSelect(event: Event, caseWidget: CaseWidget): void {
    event.stopPropagation();
    event.preventDefault();

    if (this.selectedCases.has(caseWidget)) {
      this.selectedCases.delete(caseWidget);
    } else {
      this.selectedCases.add(caseWidget);
    }
    this.validateAllSelected();
  }

  private casesToCaseWidgets(cases: Case[]): CaseWidget[] {
    const caseWidgets: CaseWidget[] = cases.map((c) => {
      return {
        id: c.accessionNumber,
        hasLinking: Boolean(c.accessionNumber),
        caseDate: c.date,
        slides: c.slides,
        caseInfo: c,
      };
    });
    return caseWidgets;
  }

  private diagnosticReportsToCaseWidgets(
      diagnosticReports: DiagnosticReport[],
      casesByCaseId: Map<string, Case>): CaseWidget[] {
    const caseWidgets: CaseWidget[] =
        diagnosticReports.map((report: DiagnosticReport) => {
          const caseAccessionId = report.resource.caseAccessionId ?? '';
          const caseInfo: Case|undefined = casesByCaseId.get(caseAccessionId);
          return {
            id: caseAccessionId ? caseAccessionId : report.resource.id,
            hasLinking: Boolean(caseAccessionId),
            caseDate: caseInfo?.date ?? 'Not available',
            diagnosticReportDate: report.resource.meta.lastUpdated,
            text: report.resource.text,
            slides: caseInfo?.slides ?? [],
            caseInfo,
            diagnosticReport: report,
          };
        });
    return caseWidgets;
  }

  private validateAllSelected(): boolean {
    this.allCasesSelected =
        this.caseWidgets.every((widget) => this.selectedCases.has(widget));
    return this.allCasesSelected;
  }

  getFormattedDate(date?: string): string {
    if (date && !isNaN(Date.parse(date))) {
      return new Intl.DateTimeFormat('en-US').format(new Date(date));
    }
    return date || 'N/A';
  }
}
