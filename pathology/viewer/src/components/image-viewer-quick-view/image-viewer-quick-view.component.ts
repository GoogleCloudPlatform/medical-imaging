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

import { ChangeDetectorRef, Component, EventEmitter, Input, OnInit, Output, TemplateRef, ViewChild } from '@angular/core';
import { Router, RouterModule } from '@angular/router';
import { InspectPageComponent } from '../inspect-page/inspect-page.component';
import { type SlideDescriptor, SlideExtraMetadata} from '../../interfaces/slide_descriptor';
import { SlideInfo } from '../../interfaces/slide_descriptor';
import { type CohortInfo } from '../../services/cohort.service';
import { DialogService } from '../../services/dialog.service';
import { SlideApiService } from '../../services/slide-api.service';
import { ImageViewerPageStore } from '../../stores/image-viewer-page.store';
import { ImageViewerPageParams } from '../../app/app.routes';
import { Map } from 'ol';
import { OlTileViewerComponent } from '../ol-tile-viewer/ol-tile-viewer.component';
import { SlideDescriptorToViewerUrlParamsPipe } from '../../pipes/slide-descriptor-to-viewer-url-params.pipe';
import { QuickViewSlideDescriptorNamePipe } from '../../pipes/quick-view-slide-descriptor-name.pipe';
import { CommonModule } from '@angular/common';
const DEFAULT_VIEWER_URL = '/viewer';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import {combineLatest} from 'rxjs';
import {tap} from 'rxjs/operators';

/**
 * Quick View for Image viewer component with limited functionality
 */
@Component({
  selector: 'image-viewer-quick-view',
  standalone: true,
  imports: [OlTileViewerComponent, MatIconModule, SlideDescriptorToViewerUrlParamsPipe,
    QuickViewSlideDescriptorNamePipe, RouterModule, CommonModule,
    MatButtonModule, MatIconModule],
  templateUrl: './image-viewer-quick-view.component.html',
  styleUrl: './image-viewer-quick-view.component.scss'
})
export class ImageViewerQuickViewComponent implements OnInit {
  /** Reference to the delete launch update dialog template. */
  @ViewChild('quickviewImageDialogTemplate', { static: true })
  quickviewImageDialogTemplate!:
    TemplateRef<{ slideDescriptor: SlideDescriptor }>;

  @Input({ required: true }) slideDescriptor!: SlideDescriptor;
  @Input() enableExpandedView = true;
  @Input() showSlideData = false;
  @Input() cohortInfo?: CohortInfo;
  @Output() readonly olMapLoaded = new EventEmitter<Map>();

  isLoaded = false;
  slideInfo?: SlideInfo;
  selectedExtraMetaData?: SlideExtraMetadata = undefined;

  constructor(
    private readonly slideApiService: SlideApiService,
    private readonly dialogService: DialogService,
    private readonly imageViewerPageStore: ImageViewerPageStore,
    private readonly router: Router,
    private readonly cdRef: ChangeDetectorRef,
  ) { }

  ngOnInit() {
    this.setupMetadata();
    this.slideApiService.getSlideInfo(this.slideDescriptor.id as string)
      .subscribe((slideInfo) => {
        if (!slideInfo) return;
        this.slideInfo = slideInfo;
        this.cdRef.detectChanges();
      });
  }

  private setupMetadata() {
    combineLatest([
      this.imageViewerPageStore.selectedSplitViewSlideDescriptor$,
      this.imageViewerPageStore.slideMetaDataBySlideDescriptorId$
    ])
        .pipe(
            tap(([
                  selectedSplitViewSlideDescriptor,
                  slideMetaDataBySlideDescriptorId
                ]) => {
              if (!selectedSplitViewSlideDescriptor ||
                  !slideMetaDataBySlideDescriptorId) {
                return;
              }
              this.selectedExtraMetaData = slideMetaDataBySlideDescriptorId.get(
                  selectedSplitViewSlideDescriptor.id as string);
            }),
            )
        .subscribe();
  }

  openQuickviewImage(slideDescriptor: SlideDescriptor) {
    if (!slideDescriptor) {
      return;
    }
    const DIALOG_CONFIG = {
      autoFocus: false,
      disableClose: false,
    };
    this.dialogService
      .openComponentDialog(this.quickviewImageDialogTemplate, {
        ...DIALOG_CONFIG,
        ...{
          data: { slideDescriptor, slideInfo: this.slideInfo },
        }
      })
      .afterClosed()
      .subscribe(() => { });
  }

  openSlideData() {
    const DIALOG_CONFIG = {
      autoFocus: false,
      disableClose: false,
    };
    this.dialogService
      .openComponentDialog(InspectPageComponent, {
        ...DIALOG_CONFIG,
      })
      .afterClosed()
      .subscribe(() => { });
  }

  olMapLoadedHandler(olMap: Map) {
    this.isLoaded = true;
    this.cdRef.detectChanges();
    this.olMapLoaded.emit(olMap);
  }

  goToViewer(slideDescriptor: SlideDescriptor, cohortInfo?: CohortInfo) {
    const splitViewSlideDescriptors =
      [...this.imageViewerPageStore.splitViewSlideDescriptors$.value];

    let viewerParams = new SlideDescriptorToViewerUrlParamsPipe().transform(
      slideDescriptor, cohortInfo);

    const selectedSplitViewSlideDescriptor =
      this.imageViewerPageStore.selectedSplitViewSlideDescriptor$.value;
    const isViewerPage = this.router.url.startsWith(DEFAULT_VIEWER_URL);


    if (isViewerPage && selectedSplitViewSlideDescriptor) {
      const selectedSlideDescriptorIndex =
        this.imageViewerPageStore.multiViewScreenSelectedIndex$.value;

      if (selectedSlideDescriptorIndex !== -1) {
        splitViewSlideDescriptors.splice(
          selectedSlideDescriptorIndex, 1, slideDescriptor);

        const previousQueryParams =
          this.router.parseUrl(this.router.url).queryParams as
          ImageViewerPageParams;
        delete previousQueryParams.r;
        delete previousQueryParams.x;
        delete previousQueryParams.y;
        delete previousQueryParams.z;

        viewerParams = {
          ...previousQueryParams,
          series: splitViewSlideDescriptors
            .map((splitViewSlideDescriptor) => {
              return String(splitViewSlideDescriptor?.id ?? ' ');
            })
            .join(',')
            .replaceAll(' ', ''),
        };
      }
    }

    this.router.navigate(['/viewer'], {
      queryParams: viewerParams,
      replaceUrl: isViewerPage && !!selectedSplitViewSlideDescriptor,
    });
  }
}
