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

import {Component, Inject, Optional} from '@angular/core';
import {MatButtonModule} from '@angular/material/button';
import {MAT_DIALOG_DATA, MatDialogModule} from '@angular/material/dialog';
import {MatIconModule} from '@angular/material/icon';

/** Inputs that can be dynamically bound to the dialog component. */
export interface DialogErrorComponentData {
  readonly title?: string;
  readonly message?: string;
  readonly copyLogsToClipboard?: Function;
}

/**
 * A dialog component that displays an error message.
 */
@Component({
  selector: 'dialog-error',
  standalone: true,
  imports: [MatIconModule, MatButtonModule, MatDialogModule],
  templateUrl: './dialog-error.component.html',
  styleUrl: './dialog-error.component.scss'
})
export class DialogErrorComponent {
  title = 'Something went wrong';
  message?: string;
  copyLogsToClipboard?: Function;

  constructor(
      @Optional() @Inject(MAT_DIALOG_DATA) public data?:
          DialogErrorComponentData,
  ) {
    if (data) {
      this.title = data.title || 'Something went wrong';
      this.message = data.message;
      this.copyLogsToClipboard = data.copyLogsToClipboard;
    }
  }

  copyLogs() {
    if (this.copyLogsToClipboard) {
      this.copyLogsToClipboard();
    }
  }
}
