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
import {Component} from '@angular/core';
import {FormControl, FormsModule, ReactiveFormsModule, Validators} from '@angular/forms';
import {MatDialogModule} from '@angular/material/dialog';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatSnackBar, MatSnackBarConfig} from '@angular/material/snack-bar';
import {tap} from 'rxjs/operators';

import {CohortService} from '../../services/cohort.service';
import {DialogService} from '../../services/dialog.service';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';

/**
 * The dialog for users to de id an existing cohort to a different DICOM store.
 */
@Component({
  selector: 'dialog-cohort-de-id',
  standalone: true,
  imports: [
    FormsModule, MatDialogModule, ReactiveFormsModule, MatFormFieldModule,
    CommonModule, MatInputModule, MatButtonModule
  ],
  templateUrl: './dialog-cohort-de-id.component.html',
  styleUrl: './dialog-cohort-de-id.component.scss'
})
export class DialogCohortDeIdComponent {
  description =
      new FormControl(this.cohortService.getSelectedCohortDescription());
  name = new FormControl(
      `${this.cohortService.getSelectedCohortDisplayName()} - De-ID`,
      Validators.required);
  deIdDisabled = false;

  constructor(
      private readonly cohortService: CohortService,
      private readonly dialogService: DialogService,
      private readonly snackBar: MatSnackBar,
  ) {}

  cancel(): void {
    this.dialogService.close();
  }

  deId(): void {
    if (this.name.hasError('required')) {
      return;
    }

    this.deIdDisabled = true;
    this.snackBar.open('Requesting de-identified copy...');

    this.cohortService
        .deIdCohort(
            this.name.getRawValue()!,
            this.description.getRawValue() ?? undefined)
        .pipe(
            tap((success) => {
              if (success) {
                const snackBarConfig = new MatSnackBarConfig();
                snackBarConfig.duration = 2000;
                this.snackBar.open(
                    'Request for de-identififed copy received and being processed.',
                    '', snackBarConfig);
              } else {
                this.deIdDisabled = false;
              }
            }),
            )
        .subscribe();
    this.dialogService.close();
  }
}
