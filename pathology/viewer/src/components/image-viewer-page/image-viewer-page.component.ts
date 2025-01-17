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
import {Component, OnInit} from '@angular/core';
import {MatDividerModule} from '@angular/material/divider';
import {MatIconModule} from '@angular/material/icon';
import {ActivatedRoute, Router} from '@angular/router';
import ol from 'ol';
import {EventsKey} from 'ol/events';
import {unByKey} from 'ol/Observable';
import {combineLatest, ReplaySubject} from 'rxjs';
import {distinctUntilChanged, takeUntil, tap} from 'rxjs/operators';

import {ImageViewerPageParams} from '../../app/app.routes';
import {SlideDescriptor, SlideInfo} from '../../interfaces/slide_descriptor';
import {GetSlideInfoPipe} from '../../pipes/get-slide-info.pipe';
import {ImageViewerPageStore} from '../../stores/image-viewer-page.store';
import {ImageViewerQuickViewComponent} from '../image-viewer-quick-view/image-viewer-quick-view.component';
import {ImageViewerSideNavComponent} from '../image-viewer-side-nav/image-viewer-side-nav.component';
import {ImageViewerSlidesComponent} from '../image-viewer-slides/image-viewer-slides.component';
import {OlTileViewerComponent} from '../ol-tile-viewer/ol-tile-viewer.component';

/**
 * Image viewer component
 */
@Component({
  selector: 'image-viewer-page',
  standalone: true,
  imports: [
    CommonModule, OlTileViewerComponent, GetSlideInfoPipe, MatIconModule,
    ImageViewerSideNavComponent, ImageViewerQuickViewComponent,
    MatDividerModule, ImageViewerSlidesComponent
  ],
  templateUrl: './image-viewer-page.component.html',
  styleUrl: './image-viewer-page.component.scss'
})
export class ImageViewerPageComponent implements OnInit {
  seriesId = '';
  selectedSlideDescriptor?: SlideDescriptor;
  selectedSlideInfo?: SlideInfo;
  selectedOlMap: ol.Map|undefined = undefined;
  slideDescriptors: SlideDescriptor[] = [];
  splitViewSlideDescriptors: Array<SlideDescriptor|undefined> = [];
  splitViewFirstOlMap: ol.Map|undefined = undefined;
  slideInfoBySlideDescriptorId = new Map<string, SlideInfo>();
  isMenuOpen = false;
  syncLock = false;
  syncLockListeners: EventsKey[] = [];
  activatedRouteParams: ImageViewerPageParams = {};
  isMultiViewScreenPicker = false;
  private mapAnimationSyncLock = false;
  multiViewScreenSelectedIndex = 0;

  private readonly destroy$ = new ReplaySubject();

  constructor(
      private readonly activatedRoute: ActivatedRoute,
      private readonly router: Router,
      readonly imageViewerPageStore: ImageViewerPageStore,
  ) {
    this.activatedRoute.queryParams
        .pipe(
            distinctUntilChanged(),
            tap((params: ImageViewerPageParams) => {
              this.activatedRouteParams = params;
            }),
            )
        .subscribe();
  }

  ngOnDestroy() {
    this.destroy$.next('');
    this.destroy$.complete();
  }

  ngOnInit() {
    this.imageViewerPageStore.multiViewScreens$
        .pipe(takeUntil(this.destroy$), tap((multiViewScreens) => {
                this.multiViewScreens = multiViewScreens;
              }))
        .subscribe();
    this.imageViewerPageStore.isMultiViewSlidePicker$
        .pipe(
            takeUntil(this.destroy$),
            tap((isMultiViewSlidePicker) => {
              this.isMultiViewScreenPicker = isMultiViewSlidePicker;
            }),
            )
        .subscribe();
    this.imageViewerPageStore.syncLock$
        .pipe(takeUntil(this.destroy$), tap((syncLock) => {
                this.syncLock = syncLock;
              }))
        .subscribe();
    this.imageViewerPageStore.splitViewSlideDescriptors$
        .pipe(
            takeUntil(this.destroy$),
            tap((splitViewSlideDescriptors) => {
              this.splitViewSlideDescriptors = splitViewSlideDescriptors;
            }),
            )
        .subscribe();
    this.imageViewerPageStore.slideInfoBySlideDescriptorId$
        .pipe(
            takeUntil(this.destroy$),
            tap((slideInfoBySlideDescriptorId) => {
              this.slideInfoBySlideDescriptorId =
                  new Map(slideInfoBySlideDescriptorId);
            }),
            )
        .subscribe();
    this.imageViewerPageStore.multiViewScreenSelectedIndex$
        .pipe(
            takeUntil(this.destroy$),
            tap((multiViewScreenSelectedIndex) => {
              this.multiViewScreenSelectedIndex = multiViewScreenSelectedIndex;
            }),
            )
        .subscribe();

    combineLatest([
      this.imageViewerPageStore.selectedSplitViewSlideDescriptor$,
      this.imageViewerPageStore.olMapBySlideDescriptorId$,
    ])
        .subscribe(
            ([selectedSplitViewSlideDescriptor, olMapBySlideDescriptorId]) => {
              if (!selectedSplitViewSlideDescriptor ||
                  !olMapBySlideDescriptorId ||
                  !olMapBySlideDescriptorId.has(
                      selectedSplitViewSlideDescriptor.id as string)) {
                return;
              }

              this.selectedOlMap = olMapBySlideDescriptorId.get(
                  selectedSplitViewSlideDescriptor.id as string);
            });
  }

  olMapLoaded(olMap: ol.Map, slideDescriptor: SlideDescriptor) {
    if (!olMap) return;

    const olMapBySlideDescriptor =
        this.imageViewerPageStore.olMapBySlideDescriptorId$.value;
    const olMapOriginalViewBySlideDescriptorId =
        this.imageViewerPageStore.olMapOriginalViewBySlideDescriptorId$.value;

    olMapBySlideDescriptor.set(slideDescriptor.id as string, olMap);
    olMapOriginalViewBySlideDescriptorId.set(
        slideDescriptor.id as string, olMap.getView());

    this.imageViewerPageStore.olMapBySlideDescriptorId$.next(
        olMapBySlideDescriptor);
    this.imageViewerPageStore.olMapOriginalViewBySlideDescriptorId$.next(
        olMapOriginalViewBySlideDescriptorId);
    this.splitViewFirstOlMap = olMap;
  }

  setupViewDiffInOlMaps(olMap1: ol.Map, olMap2: ol.Map) {
    const center1 = olMap1.getView().getCenter() ?? [0, 0];
    const center2 = olMap2.getView().getCenter() ?? [0, 0];

    const zoom1 = olMap1.getView().getZoom() ?? 0;
    const zoom2 = olMap2.getView().getZoom() ?? 0;
    const rotation1 = olMap1.getView().getRotation();
    const rotation2 = olMap2.getView().getRotation();

    const dz = zoom2 - zoom1;
    const dx = center2[0] - center1[0];
    const dy = center2[1] - center1[1];
    const dr = rotation2 - rotation1;

    return {dx, dy, dz, dr};
  }

  selectSlideDescriptor(slideDescriptor: SlideDescriptor) {
    if (this.imageViewerPageStore.selectedSplitViewSlideDescriptor$.value
            ?.id === slideDescriptor.id) {
      return;
    }
    this.imageViewerPageStore.selectedSplitViewSlideDescriptor$.next(
        slideDescriptor);
  }

  trackBySlideDescriptor(
      index: number, slideDescriptor: SlideDescriptor|undefined) {
    return `${index}-${slideDescriptor?.id}`;
  }

  toggleSyncLock() {
    const syncLock = !this.syncLock;

    this.imageViewerPageStore.syncLock$.next(syncLock);
    this.mapAnimationSyncLock = syncLock;

    if (this.mapAnimationSyncLock) {
      this.handleSyncLock();
    }

    if (!this.mapAnimationSyncLock) {
      this.syncLockListeners.forEach((listenr) => {
        unByKey(listenr);
      });
      this.syncLockListeners = [];
    }
  }

  private handleSyncLock() {
    const splitViewSlideDescriptors =
        this.imageViewerPageStore.splitViewSlideDescriptors$.value.filter(
            (a): a is SlideDescriptor => !!a?.id);
    const olMapBySlideDescriptorId =
        this.imageViewerPageStore.olMapBySlideDescriptorId$.value;
    const splitViewOlMaps =
        splitViewSlideDescriptors
            .map(slideDescriptor => {
              return olMapBySlideDescriptorId.get(slideDescriptor.id as string);
            })
            .filter((map): map is ol.Map => map !== undefined);

    const listners =
        splitViewOlMaps
            .map((mapToListen) => {
              const mapsToAnimateWithDiffs =
                  splitViewOlMaps
                      .filter((mapToAnimate) => {
                        return mapToAnimate !== mapToListen;
                      })
                      .map((mapToAnimate) => {
                        return {
                          mapToAnimate,
                          mapDiff: this.computeDiffBetweenTwoMaps(
                              mapToListen, mapToAnimate),
                        };
                      });

              const changeEventsToListen = mapToListen.getView().on(
                  ['change:center', 'change:resolution', 'change:rotation'],
                  () => {
                    if (!this.mapAnimationSyncLock) return;
                    this.mapAnimationSyncLock = false;

                    mapsToAnimateWithDiffs.forEach(
                        ({mapToAnimate, mapDiff}) => {
                          const animationParam = this.computeMapAnimationParmas(
                              mapToListen, mapDiff);

                          mapToAnimate.getView().animate(animationParam, () => {
                            this.mapAnimationSyncLock = true;
                          });
                        });
                  });

              return changeEventsToListen;
            })
            .flat();

    this.syncLockListeners.push(...listners);
  }

  private computeMapAnimationParmas(
      map: ol.Map, mapDiff: {dx: number, dy: number, dz: number, dr: number}) {
    const mapView = map.getView();
    const center = mapView.getCenter() ?? [0, 0];
    const zoom = mapView.getZoom() ?? 0;
    const rotation = mapView.getRotation() ?? 0;

    return {
      center: [center[0] + mapDiff.dx, center[1] + mapDiff.dy],
      zoom: zoom + mapDiff.dz,
      rotation: rotation + mapDiff.dr,
      duration: 0
    };
  }

  private computeDiffBetweenTwoMaps(olMap1: ol.Map, olMap2: ol.Map) {
    const center1 = olMap1.getView().getCenter() ?? [0, 0];
    const center2 = olMap2.getView().getCenter() ?? [0, 0];
    const dx = center2[0] - center1[0];
    const dy = center2[1] - center1[1];
    const zoom1 = olMap1.getView().getZoom() ?? 0;
    const zoom2 = olMap2.getView().getZoom() ?? 0;
    const dz = zoom2 - zoom1;
    const rotation1 = olMap1.getView().getRotation() ?? 0;
    const rotation2 = olMap2.getView().getRotation() ?? 0;
    const dr = rotation2 - rotation1;

    return {dx, dy, dz, dr};
  }

  multiViewScreens = 1;

  toggleMultiView(multiViewScreens: 1|2|4) {
    let slideDescriptor = this.splitViewSlideDescriptors[0];
    if (!slideDescriptor) {
      slideDescriptor = this.splitViewSlideDescriptors.filter(
          (slideDescriptor): slideDescriptor is SlideDescriptor =>
              !!slideDescriptor?.id)[0];
    }

    const viewerParams: ImageViewerPageParams = {
      series: slideDescriptor?.id as string + ','.repeat(multiViewScreens - 1),
      cohortName: this.activatedRouteParams.cohortName,
    };

    this.router.navigate(['/viewer'], {
      queryParams: viewerParams,
      replaceUrl: true,
    });

    this.imageViewerPageStore.isMultiViewSlidePicker$.next(false);
  }

  selectMultiViewScreenSelected(index: number) {
    this.multiViewScreenSelectedIndex = index;

    this.imageViewerPageStore.multiViewScreenSelectedIndex$.next(index);
    const slideDescriptor = this.splitViewSlideDescriptors[index];
    if (slideDescriptor?.id) {
      this.selectSlideDescriptor(slideDescriptor);
    }
  }
}