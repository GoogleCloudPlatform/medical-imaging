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
import {Component, Inject, OnDestroy, OnInit, Optional} from '@angular/core';
import {MAT_DIALOG_DATA, MatDialogModule} from '@angular/material/dialog';
import {MatIconModule} from '@angular/material/icon';
import {MatProgressSpinnerModule} from '@angular/material/progress-spinner';
import {ReplaySubject} from 'rxjs';
import {map, takeUntil, tap} from 'rxjs/operators';

import {CohortInfo, CohortService} from '../../services/cohort.service';

/** Inputs that can be dynamically bound to the dialog component. */
export interface SelectCohortDialogData {
  readonly title?: string;
  readonly ownedCohorts?: boolean;
  readonly editableSharedCohorts?: boolean;
  readonly viewOnlySharedCohorts?: boolean;
  readonly requiredDeidValue?: boolean;
}

/**
 * The dialog for users to create a new cohort with.
 */
@Component({
  selector: 'dialog-cohort-select',
  standalone: true,
  imports: [
    MatProgressSpinnerModule,
    MatIconModule,
    MatDialogModule,
    CommonModule,
  ],
  templateUrl: './dialog-cohort-select.component.html',
  styleUrl: './dialog-cohort-select.component.scss'
})
export class DialogCohortSelectComponent implements OnDestroy, OnInit {
  title = 'Select cohort';
  ownedCohorts = true;
  adminCohorts = true;
  editableSharedCohorts = true;
  viewOnlySharedCohorts = false;
  requiredDeidValue?: boolean = undefined;

  private readonly allowAccessList = new Set([
    ...(this.ownedCohorts ? ['PATHOLOGY_USER_ACCESS_ROLE_OWNER'] : []),
    ...(this.adminCohorts?['PATHOLOGY_USER_ACCESS_ROLE_ADMIN']: []),
    ...(this.editableSharedCohorts?['PATHOLOGY_USER_ACCESS_ROLE_EDITOR']: []),
    ...(this.viewOnlySharedCohorts?['PATHOLOGY_USER_ACCESS_ROLE_VIEWER']: []),
  ]);

  cohorts: CohortInfo[] = [];
  loadingCohorts = false;
  private readonly destroy$ = new ReplaySubject();

  constructor(
      readonly cohortService: CohortService,
      @Optional() @Inject(MAT_DIALOG_DATA) public data?: SelectCohortDialogData,
  ) {
    if (data) {
      this.title = data.title ?? 'Select cohort';
      this.ownedCohorts = data.ownedCohorts ?? true;
      this.editableSharedCohorts = data.editableSharedCohorts ?? true;
      this.viewOnlySharedCohorts = data.viewOnlySharedCohorts ?? false;
      this.requiredDeidValue = data.requiredDeidValue ?? undefined;
    }
  }

  ngOnInit() {
    this.cohortService.loadAllCohorts();

    this.cohortService.cohorts$
        .pipe(
            takeUntil(this.destroy$), map((cohorts) => {
              const allowedCohorts: CohortInfo[] = cohorts.filter((cohort) => {
                return this.allowAccessList.has(cohort.access) &&
                    (this.requiredDeidValue === undefined ||
                     cohort.isDeid === this.requiredDeidValue);
              });

              return allowedCohorts;
            }),
            tap((cohorts: CohortInfo[]) => {
              this.cohorts = cohorts;
            }))
        .subscribe();

    this.cohortService.loadingCohortInfos$
        .pipe(
            takeUntil(this.destroy$),
            tap((loading: boolean) => {
              this.loadingCohorts = loading;
            }),
            )
        .subscribe();
  }

  ngOnDestroy() {
    this.destroy$.next('');
    this.destroy$.complete();
  }
}
