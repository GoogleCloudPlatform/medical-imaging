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
import {Component, Inject, Optional} from '@angular/core';
import {MatButtonModule} from '@angular/material/button';
import {MAT_DIALOG_DATA, MatDialogRef} from '@angular/material/dialog';
import {MatIconModule} from '@angular/material/icon';

/** Inputs that can be dynamically bound to the dialog component. */
export interface DialogConfirmationComponentData {
  readonly title?: string;
  readonly message?: string;
}

/**
 * A confirmation dialog that allows the user to confirm or cancel an action.
 */
@Component({
  selector: 'dialog-confirmation',
  standalone: true,
  imports: [CommonModule, MatButtonModule, MatIconModule],
  templateUrl: './dialog-confirmation.component.html',
  styleUrl: './dialog-confirmation.component.scss'
})
export class DialogConfirmationComponent {
  title = 'Confirmed';
  message = '';

  constructor(
      public dialogRef: MatDialogRef<DialogConfirmationComponent>,
      @Optional() @Inject(MAT_DIALOG_DATA) public data?:
          DialogConfirmationComponentData,
  ) {
    if (data) {
      this.title = data.title || 'Confirmed';
      this.message = data.message || '';
    }
  }

  onOk() {
    this.dialogRef.close(true);
  }

  onCancel() {
    this.dialogRef.close(false);
  }
}
