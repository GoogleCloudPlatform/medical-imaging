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

import {_isNumberValue} from '@angular/cdk/coercion';
import {CommonModule} from '@angular/common';
import {AfterViewInit, Component, OnDestroy, OnInit, TemplateRef, ViewChild} from '@angular/core';
import {MatButtonModule} from '@angular/material/button';
import {MatDividerModule} from '@angular/material/divider';
import {MatAccordion, MatExpansionModule} from '@angular/material/expansion';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatIconModule} from '@angular/material/icon';
import {MatInputModule} from '@angular/material/input';
import {MatMenuModule} from '@angular/material/menu';
import {MatDrawer, MatSidenavModule} from '@angular/material/sidenav';
import {MatSnackBar, MatSnackBarConfig} from '@angular/material/snack-bar';
import {MatSort} from '@angular/material/sort';
import {MatTableDataSource, MatTableModule} from '@angular/material/table';
import {ActivatedRoute, Router, RouterModule} from '@angular/router';
import {BehaviorSubject, combineLatest, ReplaySubject} from 'rxjs';
import {distinctUntilChanged, filter, takeUntil, tap} from 'rxjs/operators';
import {setAnchorHref} from 'safevalues/dom';

import {CohortPageParams} from '../../app/app.routes';
import {PathologyCohort, PathologyCohortAccess, SavePathologyCohortRequest, UnsavePathologyCohortRequest} from '../../interfaces/cohorts';
import {Case} from '../../interfaces/hierarchy_descriptor';
import {PathologySlide, SlideDescriptor} from '../../interfaces/slide_descriptor';
import {DICOMUriToSlideDescriptorPipe} from '../../pipes/dicom-uri-to-slide-descriptor.pipe';
import {PathologyCohortSlideToSlideDescriptorPipe} from '../../pipes/pathology-cohort-slide-to-slide-descriptor.pipe';
import {ToViewerParamsPipe} from '../../pipes/to-viewer-params.pipe';
import {CohortInfo, CohortService} from '../../services/cohort.service';
import {DialogService} from '../../services/dialog.service';
import {UserService} from '../../services/user.service';
import {BusyOverlayComponent} from '../busy-overlay/busy-overlay.component';
import {DialogCohortCloneComponent} from '../dialog-cohort-clone/dialog-cohort-clone.component';
import {DialogCohortCreateComponent, EditModeType} from '../dialog-cohort-create/dialog-cohort-create.component';
import {DialogCohortDeIdComponent} from '../dialog-cohort-de-id/dialog-cohort-de-id.component';
import {DialogCohortDeleteComponent} from '../dialog-cohort-delete/dialog-cohort-delete.component';
import {DialogCohortExportComponent} from '../dialog-cohort-export/dialog-cohort-export.component';
import {DialogCohortShareComponent} from '../dialog-cohort-share/dialog-cohort-share.component';
import {ImageViewerQuickViewComponent} from '../image-viewer-quick-view/image-viewer-quick-view.component';

const DIALOG_CONFIG = {
  autoFocus: false,
  disableClose: false,
};

enum CohortTag {
  DE_ID,
  SHARED,
  DEFAULT,
}

/**
 * Component for the cohorts page.
 */
@Component({
  selector: 'cohorts-page',
  standalone: true,
  imports: [
    MatFormFieldModule, MatIconModule, CommonModule, MatTableModule,
    MatButtonModule, MatInputModule, MatSidenavModule,
    ImageViewerQuickViewComponent, DICOMUriToSlideDescriptorPipe,
    ToViewerParamsPipe, RouterModule,
    BusyOverlayComponent, MatExpansionModule, MatDividerModule, MatMenuModule
  ],
  templateUrl: './cohorts-page.component.html',
  styleUrl: './cohorts-page.component.scss'
})
export class CohortsPageComponent implements OnInit, AfterViewInit, OnDestroy {
  @ViewChild('cohortDetailsDrawer') cohortDetailsDrawer!: MatDrawer;
  @ViewChild('pathologyCohortCaseAccordion', {static: false})
  accordion: MatAccordion|undefined;
  @ViewChild(MatSort, {static: true}) matSort!: MatSort;
  /** Reference to the quick slide view template. */
  @ViewChild('quickviewImageDialogTemplate', {static: true})
  quickviewImageDialogTemplate!:
      TemplateRef<{slideDescriptor: SlideDescriptor}>;

  private readonly destroyed$ = new ReplaySubject<boolean>(1);
  readonly loadingSelectedPathologyCohort$ =
      new BehaviorSubject<boolean>(false);
  readonly loadingSelectedPathologyCohortCases$ =
      new BehaviorSubject<boolean>(false);
  readonly loadingProgressSelectedPathologyCohortCases$ =
      new BehaviorSubject<number>(0);
  readonly loadingCohortInfos$ = new BehaviorSubject<boolean>(false);

  dataSource = new MatTableDataSource<CohortInfo>([]);
  displayedColumns: string[] = [
    'tags',
    'displayName',
    'dateModified',
  ];

  selectedCases = new Set<Case>();
  selectedCohortInfo: CohortInfo|undefined = undefined;
  selectedPathologyCohort: PathologyCohort|undefined = undefined;
  selectedPathologyCohortCases: Case[]|undefined = [];

  allCasesSelected = false;
  isSavingCohort = false;
  someCasesSelected = false;
  // permissions
  isViewOnly = false;
  allowAppending = false;
  allowRemoving = false;
  allowDeleting = false;
  allowEditingFields = false;
  allowSharing = false;
  allowDeid = false;
  allowExport = false;
  isShared = false;
  isCohortSaved = false;
  pathologyCohortSlideToSlideDescriptorPipe:
      PathologyCohortSlideToSlideDescriptorPipe;
  constructor(
      private readonly activatedRoute: ActivatedRoute,
      private readonly cohortService: CohortService,
      private readonly dialogService: DialogService,
      private readonly router: Router,
      private readonly userService: UserService,
      private readonly snackBar: MatSnackBar,
  ) {
    this.pathologyCohortSlideToSlideDescriptorPipe =
        new PathologyCohortSlideToSlideDescriptorPipe();

    this.loadingSelectedPathologyCohort$ =
        this.cohortService.loadingSelectedPathologyCohort$;
    this.loadingCohortInfos$ = this.cohortService.loadingCohortInfos$;
    this.loadingSelectedPathologyCohortCases$ =
        this.cohortService.loadingSelectedPathologyCohortCases$;
    this.loadingProgressSelectedPathologyCohortCases$ =
        this.cohortService.loadingProgressSelectedPathologyCohortCases$;
  }

  openQuickviewImage(pathologySlide: PathologySlide) {
    if (!pathologySlide.dicomUri) return;
    const slideDescriptor =
        this.pathologyCohortSlideToSlideDescriptorPipe.transform(
            pathologySlide.dicomUri);
    const DIALOG_CONFIG = {
      autoFocus: false,
      disableClose: false,
    };
    this.dialogService
        .openComponentDialog(this.quickviewImageDialogTemplate, {
          ...DIALOG_CONFIG,
          ...{
            data: {slideDescriptor},
          }
        })
        .afterClosed()
        .subscribe(() => {});
  }

  private setupCohortData(): void {
    this.cohortService.loadAllCohorts();
    this.cohortService.cohorts$.subscribe((cohorts) => {
      this.dataSource.data = [];
      this.dataSource.data = cohorts;
    });

    combineLatest([
      this.cohortService.selectedPathologyCohort$, this.cohortService.cohorts$
    ]).subscribe(([selectedPathologyCohort, cohorts]) => {
      this.isCohortSaved = cohorts.some(
          (cohort) => cohort.name === selectedPathologyCohort?.name);
    });

    this.cohortService.selectedPathologyCohort$.subscribe(
        (selectedPathologyCohort) => {
          this.selectedPathologyCohort = selectedPathologyCohort;
        });

    this.cohortService.selectedCohortInfo$.subscribe((selectedCohortInfo) => {
      this.selectedCohortInfo = selectedCohortInfo;
    });

    this.cohortService.selectedPathologyCohortCases$
        .pipe(
            takeUntil(this.destroyed$),
            tap((selectedPathologyCohortCases) => {
              this.resetSelectedCases();


              this.selectedPathologyCohortCases =
                  selectedPathologyCohortCases.sort((a, b) => {
                    const aDate =
                        this.isDate(a.date) ? new Date(a.date).getTime() : 0;
                    const bDate =
                        this.isDate(b.date) ? new Date(b.date).getTime() : 0;

                    // Sort by date and then accessionNumber
                    return (aDate - bDate) ||
                        a.accessionNumber.localeCompare(b.accessionNumber);
                  });
            }),
            )
        .subscribe();
    combineLatest([
      this.cohortService.selectedPathologyCohort$.pipe(
          takeUntil(this.destroyed$),
          filter((selectedCohort) => selectedCohort !== undefined)),
      this.userService.getCurrentUser$(),
    ])
        .pipe(
            takeUntil(this.destroyed$),
            tap(([selectedPathologyCohort, currentUser]) => {
              if (selectedPathologyCohort && currentUser) {
                this.setAccess(
                    selectedPathologyCohort,
                    currentUser,
                );
              } else {
                this.resetAccess();
              }
            }))
        .subscribe();
  }

  private resetSelectedCases(): void {
    this.selectedCases.clear();
    this.allCasesSelected = false;
    this.someCasesSelected = false;
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
      const content: string[][] =
          [...this.selectedCases].map((selectedCase) => {
            const slidesUris: string[] =
                (selectedCase?.slides ?? [])
                    .map(slide => slide.dicomUri)
                    .filter((uri): uri is string => !!uri);
            return [
              selectedCase.accessionNumber,
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

  removeCases(): void {
    if (!this.selectedCohortInfo?.name) return;
    this.snackBar.open('Removing cases...');

    const selectedCaseIds: string[] =
        [...this.selectedCases]
            .map((selectedCase) => selectedCase.caseId)
            .filter((id): id is string => !!id);

    this.removeCasesFromCohort(
        this.selectedCohortInfo.name, new Set<string>(selectedCaseIds));

    this.selectedCases.clear();
  }

  private removeCasesFromCohort(
      cohortName: string, selectedCaseIds: Set<string>): void {
    this.cohortService.removeCasesFromCohort(cohortName, [...selectedCaseIds])
        .pipe(takeUntil(this.destroyed$))
        .subscribe((success) => {
          if (success) {
            const snackBarConfig = new MatSnackBarConfig();
            snackBarConfig.duration = 2000;
            this.snackBar.open('Cases removed.', '', snackBarConfig);
          } else {
            this.snackBar.dismiss();
          }
        });
  }

  private validateAllSelected(): boolean {
    this.allCasesSelected = (this.selectedPathologyCohortCases ??
                             []).every((pathologyCohortCase: Case) => {
      return this.selectedCases.has(pathologyCohortCase);
    });
    if (this.allCasesSelected) {
      this.someCasesSelected = false;
    }
    return this.allCasesSelected;
  }

  private validateSomeSelected(): boolean {
    this.someCasesSelected = (this.selectedPathologyCohortCases ??
                              []).some((pathologyCohortCase: Case) => {
      return this.selectedCases.has(pathologyCohortCase);
    });
    return this.someCasesSelected;
  }

  selectAll(): boolean {
    const allCasesSelected = this.validateAllSelected();
    if (allCasesSelected) {
      this.selectedCases.clear();
      this.allCasesSelected = false;
    } else {
      this.selectedCases = new Set(this.selectedPathologyCohortCases ?? []);
      this.allCasesSelected = true;
    }

    this.someCasesSelected = false;
    return this.allCasesSelected;
  }

  toggleSelect(event: Event, pathologyCohortCase: Case): void {
    event.stopPropagation();
    event.preventDefault();

    if (this.selectedCases.has(pathologyCohortCase)) {
      this.selectedCases.delete(pathologyCohortCase);
    } else {
      this.selectedCases.add(pathologyCohortCase);
    }
    this.validateSomeSelected();
    this.validateAllSelected();
  }

  cohortFilterKeyUp(filterBy: KeyboardEvent) {
    const filterValue = (filterBy.target as HTMLInputElement).value;

    this.filterCohorts(filterValue);
  }

  filterCohorts(filterBy: string) {
    this.dataSource.filter = filterBy.trim().toLowerCase();
  }

  ngOnDestroy() {
    this.destroyed$.next(true);
    this.destroyed$.complete();
  }

  ngOnInit() {
    this.setupCohortData();
    this.activatedRoute.queryParams
        .pipe(
            takeUntil(this.destroyed$),
            distinctUntilChanged(),
            )
        .subscribe((params) => {
          const name = (params as CohortPageParams).cohortName;

          if (name) {
            this.cohortService.fetchPathologyCohort(name);
          } else {
            this.cohortService.unselectCohort();
            if (this.router.url === '/cohorts') {
              this.router.navigateByUrl('/cohorts');
            }
          }
        });
  }

  selectCohortInfo(cohortInfo: CohortInfo) {
    // Already selected
    if (this.selectedCohortInfo?.name === cohortInfo.name) {
      this.cohortService.reloadSelectedCohort();
      return;
    }
    this.selectedCohortInfo = cohortInfo;
    this.cohortService.selectCohortInfo(cohortInfo);
    this.cohortService.routeToCohort(cohortInfo.name);

    this.cohortDetailsDrawer.toggle(true);
  }

  ngAfterViewInit() {
    this.dataSource.sortingDataAccessor =
        (cohortInfo: CohortInfo, property: string) => {
          return this.customCohortTableSortingDataAccessor(
              cohortInfo, property);
        };
    this.dataSource.sort = this.matSort;
  }

  customCohortTableSortingDataAccessor(data: CohortInfo, sortHeaderId: string) {
    if (sortHeaderId === 'tags') {
      if (data.isDeid) return CohortTag.DE_ID;
      if (data.isShared) return CohortTag.SHARED;
      return CohortTag.DEFAULT;
    }
    if (sortHeaderId === 'dateModified') {
      const dateTime = data.pathologyCohort.cohortMetadata?.updateTime ?
          new Date(data.pathologyCohort.cohortMetadata.updateTime).getTime() :
          0;
      return dateTime;
    }
    const value =
        (data as unknown as Record<string, string|number>)[sortHeaderId];

    if (_isNumberValue(value)) {
      const numberValue = Number(value);

      return numberValue < Number.MAX_SAFE_INTEGER ? numberValue : value;
    }
    return (value as string).toLowerCase();
  }

  isDate(date: string): boolean {
    const parsedDate = Date.parse(date);
    return !isNaN(parsedDate);
  }

  closeCohortDetailsDrawer() {
    this.cohortService.unselectCohort();
    this.cohortDetailsDrawer.toggle(false);
    this.router.navigateByUrl('/cohorts');
  }

  private setAccess(
      pathologyCohort: PathologyCohort,
      currentUser: string,
      ): void {
    const cohortInfo: CohortInfo =
        this.cohortService.pathologyCohortToCohortInfo(pathologyCohort);

    const foundUser = (pathologyCohort.userAccess ??
                       []).find(({userEmail}) => userEmail === currentUser);
    if (!foundUser) {
      this.resetAccess();
    }

    const cohortAccess: PathologyCohortAccess =
        pathologyCohort?.cohortMetadata?.cohortAccess ??
        'PATHOLOGY_COHORT_ACCESS_RESTRICTED';

    const isOpenEdit = cohortAccess === 'PATHOLOGY_COHORT_ACCESS_OPEN_EDIT';
    const isOpenViewOnly =
        cohortAccess === 'PATHOLOGY_COHORT_ACCESS_OPEN_VIEW_ONLY';

    const isOwner = !!foundUser &&
        foundUser.accessRole === 'PATHOLOGY_USER_ACCESS_ROLE_OWNER';
    const isAdmin = !!foundUser &&
        foundUser.accessRole === 'PATHOLOGY_USER_ACCESS_ROLE_ADMIN';
    const isOwnerOrAdmin = isOwner || isAdmin;
    const isEditor = !!foundUser &&
            foundUser.accessRole === 'PATHOLOGY_USER_ACCESS_ROLE_EDITOR' ||
        isOpenEdit;
    const isViewer = (!isEditor && !isOwnerOrAdmin) &&
        (!!foundUser &&
             foundUser.accessRole === 'PATHOLOGY_USER_ACCESS_ROLE_VIEWER' ||
         isOpenViewOnly);

    this.isViewOnly = isViewer && !isOpenEdit;
    this.allowAppending = (isOwnerOrAdmin || isEditor) &&
        !cohortInfo?.isExported && !cohortInfo?.isDeid;
    this.allowRemoving = isOwnerOrAdmin || isEditor;
    this.allowDeleting = isOwnerOrAdmin;
    this.allowEditingFields = (isOwnerOrAdmin || isEditor);
    this.allowSharing = isOwnerOrAdmin;
    this.allowDeid = cohortInfo?.isDeid === false && !isViewer;
    this.allowExport = !isViewer;
    this.isShared = cohortInfo?.isShared ?? false;
  }

  showAppendSlidesDialog() {
    this.dialogService.openComponentDialog(DialogCohortCreateComponent, {
      ...{
        data: {
          editMode: EditModeType.SLIDES,
        },
      },
      ...DIALOG_CONFIG,
    });
  }

  showCreateCohortDialog() {
    this.dialogService.openComponentDialog(DialogCohortCreateComponent, {
      ...{
        data: {
          editMode: EditModeType.ALL,
        },
      },
      ...DIALOG_CONFIG,
    });
  }

  showEditCohortDialog() {
    if (!this.selectedPathologyCohort?.cohortMetadata?.displayName) {
      return;
    }

    this.dialogService.openComponentDialog(DialogCohortCreateComponent, {
      ...{
        data: {
          editMode: EditModeType.NAME_DESCRIPTION,
          displayName: this.selectedPathologyCohort.cohortMetadata.displayName,
          description: this.selectedPathologyCohort.cohortMetadata.description,
        },
      },
      ...DIALOG_CONFIG,
    });
  }

  showCloneCohortDialog() {
    this.dialogService.openComponentDialog(
        DialogCohortCloneComponent, DIALOG_CONFIG);
  }

  showDeleteCohortDialog() {
    this.dialogService.openComponentDialog(
        DialogCohortDeleteComponent, DIALOG_CONFIG);
  }

  showExportCohortDialog() {
    this.dialogService.openComponentDialog(
        DialogCohortExportComponent, DIALOG_CONFIG);
  }

  showDeIdCohortDialog() {
    this.dialogService.openComponentDialog(
        DialogCohortDeIdComponent, DIALOG_CONFIG);
  }

  showShareCohortDialog() {
    this.dialogService.openComponentDialog(
        DialogCohortShareComponent, DIALOG_CONFIG);
  }


  reloadSelectedCohort() {
    this.cohortService.reloadSelectedCohort();
  }

  reloadSelectedCohortCases() {
    if (!this.selectedPathologyCohort?.name) return;

    this.cohortService.fetchPathologyCohortCases(this.selectedPathologyCohort)
        .subscribe();
  }

  saveCohort(): void {
    if (!this.selectedPathologyCohort! || !this.selectedPathologyCohort.name) {
      return;
    }

    this.isSavingCohort = true;
    this.snackBar.open('Saving cohort...');

    const saveCohortRequest: SavePathologyCohortRequest = {
      name: this.selectedPathologyCohort.name,
    };
    this.cohortService.saveCohort(saveCohortRequest)
        .pipe(takeUntil(this.destroyed$))
        .subscribe((success) => {
          if (success) {
            const snackBarConfig = new MatSnackBarConfig();
            snackBarConfig.duration = 2000;
            this.snackBar.open('Cohort saved.', '', snackBarConfig);
            this.cohortService.reloadSelectedCohort();
            this.cohortService.loadAllCohorts();
          } else {
            this.snackBar.dismiss();
          }
          this.isSavingCohort = false;
        });
  }


  unsaveCohort(): void {
    if (!this.selectedPathologyCohort! || !this.selectedPathologyCohort.name) {
      return;
    }

    this.snackBar.open('Removing cohort from your list...');

    const unsaveCohortRequest: UnsavePathologyCohortRequest = {
      name: this.selectedPathologyCohort.name,
    };
    this.cohortService.unsaveCohort(unsaveCohortRequest)
        .pipe(takeUntil(this.destroyed$))
        .subscribe((success) => {
          if (success) {
            const snackBarConfig = new MatSnackBarConfig();
            snackBarConfig.duration = 2000;
            this.snackBar.open('Cohort removed.', '', snackBarConfig);
            this.cohortService.reloadSelectedCohort();
            this.cohortService.loadAllCohorts();
          } else {
            this.snackBar.dismiss();
          }
        });
  }

  reloadCohorts() {
    this.cohortService.loadAllCohorts();
  }

  private resetAccess() {
    this.isViewOnly = false;
    this.allowAppending = false;
    this.allowRemoving = false;
    this.allowDeleting = false;
    this.allowEditingFields = false;
    this.allowSharing = false;
    this.allowDeid = false;
    this.allowExport = false;
    this.isShared = false;
    this.isCohortSaved = false;
  }
}
