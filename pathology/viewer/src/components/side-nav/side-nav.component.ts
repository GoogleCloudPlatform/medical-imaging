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
import {Component, OnDestroy, OnInit, TemplateRef, ViewChild} from '@angular/core';
import {FormsModule} from '@angular/forms';
import {MatButtonModule} from '@angular/material/button';
import {MatIconModule} from '@angular/material/icon';
import {MatSelectModule} from '@angular/material/select';
import {MatTooltipModule} from '@angular/material/tooltip';
import {ActivatedRoute, NavigationEnd, Params, Router, RouterModule} from '@angular/router';
import {ReplaySubject} from 'rxjs';
import {distinctUntilChanged, filter, takeUntil, tap} from 'rxjs/operators';

import {DEFAULT_AUTH_URL, DEFAULT_COHORT_URL, DEFAULT_INSPECT_URL, DEFAULT_SEARCH_URL, DEFAULT_VIEWER_URL, ImageViewerPageParams} from '../../app/app.routes';
import {environment} from '../../environments/environment';
import {IccProfileType} from '../../interfaces/types';
import {IccProfileTypeToLabelPipe} from '../../pipes/icc-profile-type-to-label.pipe';
import {AuthService} from '../../services/auth.service';
import {DialogService} from '../../services/dialog.service';
import {ImageViewerPageStore} from '../../stores/image-viewer-page.store';

/**
 * Side navigation component.
 */
@Component({
  selector: 'side-nav',
  standalone: true,
  imports: [
    IccProfileTypeToLabelPipe,
    RouterModule,
    MatTooltipModule,
    MatSelectModule,
    MatIconModule,
    MatButtonModule,
    FormsModule,
    CommonModule,
  ],
  templateUrl: './side-nav.component.html',
  styleUrl: './side-nav.component.scss'
})
export class SideNavComponent implements OnInit, OnDestroy {
  @ViewChild('settingsDialogTemplate', {static: true})
  settingsDialogTemplate!: TemplateRef<{}>;
  private readonly destroyed$ = new ReplaySubject<boolean>(1);

  iccProfile: IccProfileType = IccProfileType.NONE;
  showXYZCoordinates = false;
  iccProfileType = [
    IccProfileType.NO, IccProfileType.ADOBERGB, IccProfileType.ROMMRGB,
    IccProfileType.SRGB
  ];

  isSlideSelected = false;
  lastCohortUrlQueryParams: {[key: string]: string} = {};
  lastSearchUrlQueryParams: {[key: string]: string} = {};
  lastViewerUrlQueryParams: ImageViewerPageParams = {};

  readonly defaultCohortUrl = DEFAULT_COHORT_URL;
  readonly defaultSearchUrl = DEFAULT_SEARCH_URL;
  readonly defaultViewerUrl = DEFAULT_VIEWER_URL;
  readonly defaultAuthUrl = DEFAULT_AUTH_URL;
  readonly defaultInspectUrl = DEFAULT_INSPECT_URL;
  activatedRouteParams: Params = {} as Params;
  readonly cohortsEnabled = environment.ENABLE_COHORTS;
  readonly annotationsEnabled = environment.ENABLE_ANNOTATIONS;

  constructor(
      private readonly authService: AuthService,
      readonly activatedRoute: ActivatedRoute,
      private readonly imageViewerPageStore: ImageViewerPageStore,
      private readonly dialogService: DialogService,
      readonly router: Router,
  ) {}

  ngOnDestroy() {
    this.destroyed$.next(true);
    this.destroyed$.complete();
  }

  ngOnInit() {
    this.setupRoutes();
    this.imageViewerPageStore.iccProfile$
        .pipe(
            takeUntil(this.destroyed$),
            tap((iccProfile) => {
              this.iccProfile = iccProfile;
            }),
            )
        .subscribe();
    this.imageViewerPageStore.showXYZCoordinates$
        .pipe(
            takeUntil(this.destroyed$),
            tap((showXYZCoordinates) => {
              this.showXYZCoordinates = showXYZCoordinates;
            }),
            )
        .subscribe();
    this.router.events
        .pipe(
            filter(
                (event): event is NavigationEnd =>
                    event instanceof NavigationEnd),
            tap((event: NavigationEnd) => {
              this.setupRoutes();
            }),
            )
        .subscribe();

    this.activatedRoute.queryParams
        .pipe(
            distinctUntilChanged(),
            tap((params) => {
              this.activatedRouteParams = params;
            }),
            )
        .subscribe();
  }

  private setupRoutes() {
    const url = this.router.url;
    if (url.startsWith(DEFAULT_SEARCH_URL)) {
      this.lastSearchUrlQueryParams = this.activatedRoute.snapshot.queryParams;
    }
    if (url.startsWith(DEFAULT_COHORT_URL)) {
      this.lastCohortUrlQueryParams = this.activatedRoute.snapshot.queryParams;
    }
    if (url.startsWith(DEFAULT_VIEWER_URL)) {
      this.lastViewerUrlQueryParams = this.activatedRoute.snapshot.queryParams;
    }
  }

  openSettingsDialog() {
    const DIALOG_CONFIG = {
      autoFocus: false,
      disableClose: false,
    };
    this.dialogService
        .openComponentDialog(this.settingsDialogTemplate, {
          ...DIALOG_CONFIG,
        })
        .afterClosed()
        .subscribe(() => {});
  }

  toggleDebugMode() {
    this.imageViewerPageStore.showXYZCoordinates$.next(
        !this.showXYZCoordinates);
  }

  selectIccProfile() {
    this.imageViewerPageStore.iccProfile$.next(this.iccProfile);
  }

  logout(): void {
    this.authService.logout();
  }
}
