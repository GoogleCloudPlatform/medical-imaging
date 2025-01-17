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
import {Component, Inject, Input, Optional} from '@angular/core';
import {FormsModule} from '@angular/forms';
import {MatButton} from '@angular/material/button';
import {MAT_DIALOG_DATA, MatDialogRef} from '@angular/material/dialog';
import {MatFormField} from '@angular/material/form-field';

const ENTER_CODE = 'Enter';

/** Inputs that can be dynamically bound to the dialog component. */
export interface DialogStringQuestionsComponentData {
  readonly title?: string;
  readonly message?: string;
  readonly answer?: string;
}

/**
 * A dialog that asks a question and returns a string answer.
 */
@Component({
  selector: 'dialog-string-questions',
  standalone: true,
  imports: [CommonModule, MatFormField, FormsModule, MatButton],
  templateUrl: './dialog-string-questions.component.html',
  styleUrl: './dialog-string-questions.component.scss'
})
export class DialogStringQuestionsComponent {
  /** Title to display on the dialog. */
  @Input() title = 'Question?';
  /** Question to ask for answer to. */
  @Input() message = '';
  /** Stores the answer string to the question. */
  @Input() answer = '';

  constructor(
      public dialogRef:
          MatDialogRef<DialogStringQuestionsComponentData, string>,
      @Optional() @Inject(MAT_DIALOG_DATA) public data?:
          DialogStringQuestionsComponentData,
  ) {
    if (data) {
      this.title = data.title || '';
      this.message = data.message || '';
      this.answer = data.answer || '';
    }
  }

  keyPressHandler(event: KeyboardEvent) {
    if (event.code === ENTER_CODE) {
      this.onOk();
    }
  }

  onOk() {
    this.dialogRef.close(this.answer);
  }

  onCancel() {
    this.dialogRef.close();
  }
}
