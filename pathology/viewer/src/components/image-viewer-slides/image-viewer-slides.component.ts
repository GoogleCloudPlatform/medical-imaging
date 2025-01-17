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
import {Component, OnDestroy, OnInit, ViewChild} from '@angular/core';
import {MatButtonModule} from '@angular/material/button';
import {MatDividerModule} from '@angular/material/divider';
import {MatExpansionModule, MatExpansionPanel} from '@angular/material/expansion';
import {MatIconModule} from '@angular/material/icon';
import {MatProgressBarModule} from '@angular/material/progress-bar';
import {MatTooltipModule} from '@angular/material/tooltip';
import {ActivatedRoute, Router} from '@angular/router';
import {Map} from 'ol';
import {BehaviorSubject, combineLatest, ReplaySubject} from 'rxjs';
import {distinctUntilChanged, filter, takeUntil, tap} from 'rxjs/operators';

import {ImageViewerPageParams} from '../../app/app.routes';
import {environment} from '../../environments/environment';
import {isSlideDeId} from '../../interfaces/dicom_store_descriptor';
import {SlideDescriptor} from '../../interfaces/slide_descriptor';
import {SplitViewSlideDescriptorToIndexPipe} from '../../pipes/split-view-slide-descriptor-to-index.pipe';
import {CohortService} from '../../services/cohort.service';
import {DialogService} from '../../services/dialog.service';
import {SlideApiService} from '../../services/slide-api.service';
import {UserService} from '../../services/user.service';
import {ImageViewerPageStore} from '../../stores/image-viewer-page.store';
import {DialogCohortSelectComponent} from '../dialog-cohort-select/dialog-cohort-select.component';
import {ImageViewerQuickViewComponent} from '../image-viewer-quick-view/image-viewer-quick-view.component';

/**
 * Slides for ImageViewerPage
 */
@Component({
  selector: 'image-viewer-slides',
  standalone: true,
  imports: [
    MatIconModule,
    CommonModule,
    SplitViewSlideDescriptorToIndexPipe,
    ImageViewerQuickViewComponent,
    MatTooltipModule,
    MatDividerModule,
    MatProgressBarModule,
    MatExpansionModule,
    MatButtonModule,
  ],
  templateUrl: './image-viewer-slides.component.html',
  styleUrl: './image-viewer-slides.component.scss'
})
export class ImageViewerSlidesComponent implements OnInit, OnDestroy {
  @ViewChild('slidesExpansionPanel', {static: true})
  slidesExpansionPanel!: MatExpansionPanel;

  activatedRouteParams: ImageViewerPageParams = {};
  cohortsEnabled = environment.ENABLE_COHORTS;
  cohortEditAllowed = new BehaviorSubject<boolean>(false);
  enableSplitView = true;
  isMultiViewSlidePicker = false;
  loadingCohorts = true;
  multiViewScreens = 0;
  olMap: Map|undefined = undefined;
  private readonly destroyed$ = new ReplaySubject<boolean>(1);
  quickViewSelectedDescriptors = new Set<SlideDescriptor>();
  selectFeatureAvailable = true;
  selectedSplitViewSlideDescriptor?: SlideDescriptor;
  slideDescriptors: SlideDescriptor[] = [];
  slideDescriptorsInCohort: SlideDescriptor[] = [];
  slideDescriptorsNotInCohort: SlideDescriptor[] = [];
  splitViewSlideDescriptors: SlideDescriptor[] = [];

  constructor(
      private readonly activatedRoute: ActivatedRoute,
      private readonly cohortService: CohortService,
      private readonly dialogService: DialogService,
      private readonly imageViewerPageStore: ImageViewerPageStore,
      private readonly router: Router,
      private readonly slideApiService: SlideApiService,
      private readonly userService: UserService,
  ) {
    this.activatedRoute.queryParams
        .pipe(
            distinctUntilChanged(),
            tap((params: ImageViewerPageParams) => {
              this.activatedRouteParams = params;
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
                const cohortAccess = selectedPathologyCohort?.userAccess;
                const foundUser =
                    (selectedPathologyCohort.userAccess ??
                     []).find(({userEmail}) => userEmail === currentUser);
                if (!cohortAccess || !foundUser) {
                  this.cohortEditAllowed.next(false);
                  return;
                }
                this.cohortEditAllowed.next(
                    foundUser.accessRole ===
                        'PATHOLOGY_USER_ACCESS_ROLE_OWNER' ||
                    foundUser.accessRole ===
                        'PATHOLOGY_USER_ACCESS_ROLE_EDITOR' ||
                    foundUser.accessRole ===
                        'PATHOLOGY_USER_ACCESS_ROLE_ADMIN');
              } else {
                this.cohortEditAllowed.next(false);
              }
            }))
        .subscribe();
  }

  ngOnDestroy() {
    this.destroyed$.next(true);
    this.destroyed$.complete();
  }

  ngOnInit() {
    this.setupOlMap();
    this.setupSlideDescriptors();
    this.setupSelectedSplitViewSlideDescriptor();
    this.setupMultiView();
  }

  private setupMultiView() {
    this.imageViewerPageStore.multiViewScreens$.subscribe(
        (multiViewScreens) => {
          this.multiViewScreens = multiViewScreens;
        });
    this.imageViewerPageStore.isMultiViewSlidePicker$.subscribe(
        (isMultiViewSlidePicker) => {
          this.isMultiViewSlidePicker = isMultiViewSlidePicker;
        });
  }

  private setupSelectedSplitViewSlideDescriptor() {
    this.imageViewerPageStore.selectedSplitViewSlideDescriptor$.subscribe(
        (selectedSplitViewSlideDescriptor) => {
          this.selectedSplitViewSlideDescriptor =
              selectedSplitViewSlideDescriptor;
        });
    this.imageViewerPageStore.splitViewSlideDescriptors$.subscribe(
        (splitViewSlideDescriptors) => {
          this.splitViewSlideDescriptors = splitViewSlideDescriptors.filter(
              (a): a is SlideDescriptor => !!a?.id);
          if (this.splitViewSlideDescriptors.length === 1) {
            this.slidesExpansionPanel.close();
          }
        });
  }

  private setupOlMap() {
    combineLatest([
      this.imageViewerPageStore.selectedSplitViewSlideDescriptor$,
      this.imageViewerPageStore.olMapBySlideDescriptorId$,
    ])
        .pipe(
            takeUntil(this.destroyed$),
            tap(([
                  selectedSplitViewSlideDescriptor,
                  olMapBySlideDescriptorId,
                ]) => {
              const olMap = olMapBySlideDescriptorId.get(
                  selectedSplitViewSlideDescriptor?.id as string);

              if (!olMap) return;
              this.olMap = olMap;
            }),
            )
        .subscribe();
  }

  private setupSlideDescriptors() {
    this.cohortService.loading$.subscribe((loading) => {
      this.loadingCohorts = loading;
    });

    combineLatest([
      this.cohortService.selectedPathologyCohort$,
      this.slideApiService.slideDescriptors$
    ]).subscribe(([selectedPathologyCohort, slideDescriptors]) => {
      let cohortSlides = new Set<string>();
      if (selectedPathologyCohort?.slides) {
        cohortSlides =
            new Set<string>(selectedPathologyCohort?.slides.map(a => a.dicomUri)
                                .filter((x): x is string => !!x));
      }

      this.slideDescriptors = [...slideDescriptors];
      if (!this.activatedRouteParams.cohortName) {
        this.slideDescriptorsInCohort = [...slideDescriptors];
      } else {
        const inCohorts: SlideDescriptor[] = [];
        const notInCohorts: SlideDescriptor[] = [];
        [...slideDescriptors].forEach(slideDescriptor => {
          const isInCohort =
              cohortSlides.has(slideDescriptor?.id as string) ?? false;
          if (isInCohort) {
            inCohorts.push(slideDescriptor);
          } else {
            notInCohorts.push(slideDescriptor);
          }
        });

        this.slideDescriptorsInCohort = inCohorts;
        this.slideDescriptorsNotInCohort = notInCohorts;
      }
    });
  }

  addToSplitView() {
    const selectedSlideDescriptors =
        [...this.quickViewSelectedDescriptors.values()];
    if (this.splitViewSlideDescriptors.length > 1 &&
        (!selectedSlideDescriptors.length &&
         this.selectedSplitViewSlideDescriptor)) {
      selectedSlideDescriptors.push(this.selectedSplitViewSlideDescriptor);
    }


    const queryParams: ImageViewerPageParams = {
      series: selectedSlideDescriptors.map(a => a.id).join(','),
    };

    this.router.navigate([], {
      relativeTo: this.activatedRoute,
      queryParams,
      queryParamsHandling:
          'merge',  // remove to replace all query params by provided
    });

    this.quickViewSelectedDescriptors.clear();
  }

  openAddToCohortDialog() {
    const selectedSlideDescriptors =
        [...this.quickViewSelectedDescriptors.values()];
    if (!selectedSlideDescriptors.length) return;

    const isAnySlideDeId = selectedSlideDescriptors.some(
        (slideDescriptor) => isSlideDeId(slideDescriptor.id as string || ''));

    const dialogRef =
        this.dialogService.openComponentDialog(DialogCohortSelectComponent, {
          data: {
            title: 'Add selected slide to cohort',
            viewOnlySharedCohorts: false,
            requiredDeidValue: isAnySlideDeId,
          },
          maxHeight: '100vh',
          autoFocus: false,
        });

    dialogRef.afterClosed().subscribe((cohortName: string) => {
      if (!cohortName) return;

      this.cohortService
          .addSlidesToCohort(
              cohortName,
              selectedSlideDescriptors.map(
                  (slideDescriptor) => slideDescriptor.id as string))
          .subscribe();
    });
  }

  private resetQuickViewSelectedDescriptors() {
    this.quickViewSelectedDescriptors.clear();
  }

  removeSlideFromCurrentCohort() {
    const selectedSlideDescriptors =
        [...this.quickViewSelectedDescriptors.values()];
    if (!selectedSlideDescriptors.length) return;

    if (this.cohortService.isCohortSelected()) {
      this.cohortService
          .removeSlidesFromCohort(
              this.cohortService.getSelectedCohortName(),
              selectedSlideDescriptors.map(
                  (slideDescriptor) => slideDescriptor.id as string))
          .pipe(
              takeUntil(this.destroyed$),
              )
          .subscribe();
    }
  }

  toggleQuickViewSelectedDescriptors(slideDescriptor: SlideDescriptor) {
    if (this.quickViewSelectedDescriptors.has(slideDescriptor)) {
      this.quickViewSelectedDescriptors.delete(slideDescriptor);
      return;
    }
    this.quickViewSelectedDescriptors.add(slideDescriptor);
  }

  toggleSelectAllQuickViewSelectedDescriptors() {
    if (this.quickViewSelectedDescriptors.size ===
        this.slideDescriptors.length) {
      this.resetQuickViewSelectedDescriptors();
      return;
    }
    this.quickViewSelectedDescriptors = new Set(this.slideDescriptors);
  }

  selectSplitViewScreen(selectedDescriptor: SlideDescriptor) {
    this.imageViewerPageStore.selectedSplitViewSlideDescriptor$.next(
        selectedDescriptor);
  }

  toggleMultiView() {
    const splitViewSlideDescriptors =
        this.imageViewerPageStore.splitViewSlideDescriptors$.value;
    this.imageViewerPageStore.syncLock$.next(false);
    this.imageViewerPageStore.multiViewScreenSelectedIndex$.next(0);

    if (splitViewSlideDescriptors.length > 1) {
      let selectedSlideDescriptor =
          splitViewSlideDescriptors[this.imageViewerPageStore
                                        .multiViewScreenSelectedIndex$.value];
      if (!selectedSlideDescriptor) {
        selectedSlideDescriptor = splitViewSlideDescriptors[0];
      }

      const viewerParams: ImageViewerPageParams = {
        series: (selectedSlideDescriptor?.id as string),
        cohortName: this.activatedRouteParams.cohortName,
      };

      this.imageViewerPageStore.multiViewScreenSelectedIndex$.next(0);
      this.router.navigate(['/viewer'], {
        queryParams: viewerParams,
        replaceUrl: true,
      });

      return;
    }

    this.imageViewerPageStore.isMultiViewSlidePicker$.next(
        !this.imageViewerPageStore.isMultiViewSlidePicker$.value);
  }
}
