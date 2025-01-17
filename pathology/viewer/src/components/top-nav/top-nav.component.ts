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
import {Component, Input, OnDestroy, OnInit} from '@angular/core';
import {MatIconModule} from '@angular/material/icon';
import {ActivatedRoute, NavigationEnd, Router} from '@angular/router';
import {combineLatest, ReplaySubject} from 'rxjs';
import {takeUntil, tap} from 'rxjs/operators';

import {Case} from '../../interfaces/hierarchy_descriptor';
import {SlideDescriptor, SlideExtraMetadata} from '../../interfaces/slide_descriptor';
import {CohortService} from '../../services/cohort.service';
import {ImageViewerPageStore} from '../../stores/image-viewer-page.store';

/**
 * Top navigation bar for the application.
 */
@Component({
  selector: 'top-nav',
  standalone: true,
  imports: [
    MatIconModule,
    CommonModule,
  ],
  templateUrl: './top-nav.component.html',
  styleUrl: './top-nav.component.scss'
})
export class TopNavComponent implements OnDestroy, OnInit {
  @Input() allowLinks = true;
  @Input() caseId?: string;
  @Input() patientId?: string;
  @Input() patientName?: string;
  @Input() slideLabel?: string;

  selectedCaseIndex: number = -1;
  cohortDisplayName = '';
  cohortName?: string;
  isViewerRoute = false;
  selectedSlideDescriptor?: SlideDescriptor;
  selectedExtraMetaData?: SlideExtraMetadata = undefined;

  private readonly destroyed$ = new ReplaySubject<boolean>(1);

  constructor(
      readonly activatedRoute: ActivatedRoute,
      readonly cohortService: CohortService,
      private readonly router: Router,
      private readonly imageViewerPageStore: ImageViewerPageStore,
  ) {}

  ngOnInit() {
    this.initializeSubscriptions();

    this.intializeViewerRouteData();
  }

  private initializeSubscriptions() {
    this.imageViewerPageStore.selectedSplitViewSlideDescriptor$
        .pipe(
            takeUntil(this.destroyed$),
            tap((selectedSplitViewSlideDescriptor) => {
              this.selectedSlideDescriptor = selectedSplitViewSlideDescriptor;
            }),
            )
        .subscribe();
    combineLatest([
      this.imageViewerPageStore.selectedSplitViewSlideDescriptor$,
      this.imageViewerPageStore.slideMetaDataBySlideDescriptorId$,
    ])
        .pipe(
            takeUntil(this.destroyed$),
            tap(([selectedSplitViewSlideDescriptor, selectedExtraMetaData]) => {
              if (!selectedSplitViewSlideDescriptor ||
                  !selectedExtraMetaData.has(
                      selectedSplitViewSlideDescriptor.id as string)) {
                return;
              }

              this.selectedExtraMetaData = selectedExtraMetaData.get(
                  selectedSplitViewSlideDescriptor.id as string);
            }),
            )
        .subscribe();


    this.cohortService.selectedCohortInfo$
        .pipe(
            takeUntil(this.destroyed$),
            tap((selectedCohortInfo) => {
              this.cohortName = selectedCohortInfo?.name ?? '';
              this.cohortDisplayName = selectedCohortInfo?.displayName ?? '';
            }),
            )
        .subscribe();

    this.router.events
        .pipe(
            takeUntil(this.destroyed$),
            tap((event) => {
              if (event instanceof NavigationEnd) {
                // Hide progress spinner or progress bar
                this.intializeViewerRouteData();
              }
            }),
            )
        .subscribe();
  }

  private intializeViewerRouteData() {
    const currentRoute = this.router.url;
    this.isViewerRoute = currentRoute.startsWith('/viewer');
    if (this.isViewerRoute) {
      this.cohortService.selectedPathologyCohortCases$
          .pipe(
              tap((selectedPathologyCohortCases: Case[]) => {
                      // this.selectedCaseIndex =
                      // selectedPathologyCohortCases.findIndex(
                      //   (pathologyCohortCase) => {
                      //     return pathologyCohortCase.caseId ===
                      //       this.workspaceService.getWorkspaceModel().caseId;
                      //   });
                  }),
              )
          .subscribe();
    }
  }

  ngOnDestroy() {
    this.destroyed$.next(true);
    this.destroyed$.complete();
  }

  prevCase(): void {
    if (this.selectedCaseIndex < 1) {
      return;
    }

    const previousPathologyCohortCases =
        this.cohortService.selectedPathologyCohortCases$
            .getValue()[this.selectedCaseIndex - 1];
    this.loadCase(previousPathologyCohortCases);
  }

  nextCase(): void {
    const selectedPathologyCohortCases =
        this.cohortService.selectedPathologyCohortCases$.getValue();
    if (this.selectedCaseIndex >= selectedPathologyCohortCases.length - 1) {
      return;
    }

    const previousPathologyCohortCases =
        this.cohortService.selectedPathologyCohortCases$
            .getValue()[this.selectedCaseIndex + 1];
    this.loadCase(previousPathologyCohortCases);
  }

  routeToCohort(): void {
    if (this.cohortName) {
      this.cohortService.routeToSelectedCohort();
    }
  }

  routeToCase(): void {
    if (this.allowLinks && this.selectedExtraMetaData?.caseId) {
      this.cohortService.unselectCohort();
      this.router.navigateByUrl(
          '/search?q=' + this.selectedExtraMetaData?.caseId + '&type=caseId');
    }
  }

  routeToHome(): void {
    this.cohortService.unselectCohort();
    this.router.navigateByUrl('/');
  }

  routeToPatient(): void {
    if (this.allowLinks && this.selectedExtraMetaData?.patientId) {
      this.cohortService.unselectCohort();
      this.router.navigateByUrl(
          '/search?q=' + this.selectedExtraMetaData.patientId +
          '&type=PatientID');
    }
  }

  private loadCase(caseToLoad: Case): void {
    const queryParams = {
      caseId: caseToLoad.caseId,
      cohortName: this.cohortService.getSelectedCohortName(),
    };
    this.router.navigate(['/viewer'], {queryParams}).then(() => {
      location.reload();
    });
  }

  goToSearchPage(): void {
    this.cohortService.unselectCohort();
    this.router.navigateByUrl('/search');
  }
}
