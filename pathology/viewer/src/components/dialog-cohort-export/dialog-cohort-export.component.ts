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
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatSnackBar, MatSnackBarConfig} from '@angular/material/snack-bar';

import {CohortService} from '../../services/cohort.service';
import {DialogService} from '../../services/dialog.service';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatDialogModule } from '@angular/material/dialog';

/**
 * The dialog for users to export an existing cohort to a user specified
 * GCS bucket.
 */
@Component({
  selector: 'dialog-cohort-export',
  standalone: true,
  imports: [CommonModule, MatDialogModule, MatFormFieldModule, FormsModule, ReactiveFormsModule, MatInputModule, MatButtonModule],
  templateUrl: './dialog-cohort-export.component.html',
  styleUrl: './dialog-cohort-export.component.scss'
})
export class DialogCohortExportComponent {
  gcsPath = new FormControl('', Validators.pattern(/^gs:\/\//));
  exportDisabled = false;
  showDeIdWarning = this.cohortService.selectedCohortInfo$.value?.isDeid;

  constructor(
      private readonly dialogService: DialogService,
      private readonly cohortService: CohortService,
      private readonly snackBar: MatSnackBar,
  ) {}

  cancel(): void {
    this.dialogService.close();
  }

  export(): void {
    if (this.gcsPath.hasError('pattern')) {
      return;
    }

    this.exportDisabled = true;
    this.snackBar.open('Requesting export...');

    this.cohortService.exportCohort(this.gcsPath.getRawValue()!)
        .subscribe((success) => {
          if (success) {
            this.dialogService.close();
            const snackBarConfig = new MatSnackBarConfig();
            snackBarConfig.duration = 2000;
            this.snackBar.open(
                'Export request received and being processed.', '',
                snackBarConfig);
          } else {
            this.exportDisabled = false;
            this.snackBar.dismiss();
          }
        });
  }
}
