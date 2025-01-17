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
import {Component, OnDestroy} from '@angular/core';
import {FormControl, FormsModule, ReactiveFormsModule, Validators} from '@angular/forms';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatSnackBar, MatSnackBarConfig} from '@angular/material/snack-bar';
import {ReplaySubject} from 'rxjs';
import {takeUntil, tap} from 'rxjs/operators';

import {CohortService} from '../../services/cohort.service';
import {DialogService} from '../../services/dialog.service';

/**
 * The dialog for users to clone an existing cohort.
 */
@Component({
  selector: 'dialog-cohort-clone',
  standalone: true,
  imports: [FormsModule, ReactiveFormsModule, MatFormFieldModule, CommonModule],
  templateUrl: './dialog-cohort-clone.component.html',
  styleUrl: './dialog-cohort-clone.component.scss'
})
export class DialogCohortCloneComponent implements OnDestroy {
  name = new FormControl(
      `${this.cohortService.getSelectedCohortDisplayName()} - Clone`,
      Validators.required);
  description =
      new FormControl(this.cohortService.getSelectedCohortDescription());
  okDisabled = false;

  private readonly destroy$ = new ReplaySubject();

  constructor(
      private readonly cohortService: CohortService,
      private readonly dialogService: DialogService,
      private readonly snackBar: MatSnackBar,
  ) {}

  ngOnDestroy() {
    this.destroy$.next('');
    this.destroy$.complete();
  }

  cancel(): void {
    this.dialogService.close();
  }

  clone(): void {
    if (this.name.hasError('required')) {
      return;
    }
    this.okDisabled = true;

    const snackBarConfig = new MatSnackBarConfig();
    snackBarConfig.duration = 2000;
    this.snackBar.open('Cloning cohort...', '', snackBarConfig);

    this.cohortService
        .copyCohort(
            this.name.getRawValue()!,
            this.description.getRawValue() ?? undefined)
        .pipe(takeUntil(this.destroy$), tap((success) => {
                if (success) {
                  this.dialogService.close();
                } else {
                  this.okDisabled = false;
                  this.snackBar.dismiss();
                }
              }))
        .subscribe();
  }
}
