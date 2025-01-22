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

import {Component} from '@angular/core';
import {Router} from '@angular/router';

import {CohortService} from '../../services/cohort.service';
import {DialogService} from '../../services/dialog.service';
import { MatDialogModule } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';

/**
 * The dialog for users to delete an existing cohort with.
 */
@Component({
  selector: 'dialog-cohort-delete',
  standalone: true,
  imports: [
    MatDialogModule,
    MatButtonModule,
    FormsModule,
    CommonModule,
  ],
  templateUrl: './dialog-cohort-delete.component.html',
  styleUrl: './dialog-cohort-delete.component.scss'
})
export class DialogCohortDeleteComponent {
  deleteDisabled = false;

  constructor(
      private readonly cohortService: CohortService,
      private readonly dialogService: DialogService,
      private readonly router: Router,
  ) {}

  getCohortDisplayName(): string {
    return this.cohortService.getSelectedCohortDisplayName();
  }

  delete(): void {
    this.deleteDisabled = true;

    this.cohortService.deleteSelectedCohort().subscribe((success: boolean) => {
      if (success) {
        this.dialogService.close();
        this.cohortService.loadAllCohorts();
        this.cohortService.unselectCohort();

        this.router.navigateByUrl('/cohorts');
      }

      this.deleteDisabled = false;
    });
  }

  cancel(): void {
    this.dialogService.close();
  }
}
