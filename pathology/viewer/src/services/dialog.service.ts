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

import { Injectable, TemplateRef } from '@angular/core';
import {
  MatDialog,
  MatDialogConfig,
  MatDialogRef,
} from '@angular/material/dialog';
import { Observable } from 'rxjs';

import { ComponentType } from '@angular/cdk/portal';
import { MatSnackBar } from '@angular/material/snack-bar';
import { map } from 'rxjs/operators';
import { DialogConfirmationComponent } from '../components/dialog-confirmation/dialog-confirmation.component';
import { DialogStringQuestionsComponent } from '../components/dialog-string-questions/dialog-string-questions.component';
import { DialogErrorComponent } from '../components/dialog-error/dialog-error.component';

/**
 * Titles for various dialogs.
 */
export enum Titles {
  CONFIRM = 'Are you sure?',
  ERROR = 'Something went wrong...',
}

const SNACKBAR_OPEN_DURATION_IN_SECONDS = 10 * 1000;

/**
 * Handles showing dialogs for the application.
 */
@Injectable({
  providedIn: 'root',
})
export class DialogService {
  constructor(
    private readonly dialog: MatDialog,
    private readonly snackBar: MatSnackBar,
  ) { }

  confirm(message: string, title: Titles | string = Titles.CONFIRM):
    MatDialogRef<DialogConfirmationComponent> {
    const dialogRef: MatDialogRef<DialogConfirmationComponent> =
      this.dialog.open(DialogConfirmationComponent, {
        data: { title, message },
        autoFocus: false,
        disableClose: true,
        panelClass: 'mc-dialog',
        width: '25em',
      });

    return dialogRef;
  }

  openComponentDialog<C, D>(
    component: ComponentType<C> | TemplateRef<C>,
    configs: MatDialogConfig<D>): MatDialogRef<C> {
    const dialogRef: MatDialogRef<C> = this.dialog.open(component, configs);

    return dialogRef;
  }

  error(message: string): Observable<boolean> {
    return this.openErrorSnackbar(message);
  }

  openErrorSnackbar(message: string) {
    return this.snackBar
      .openFromComponent(DialogErrorComponent, {
        data: {
          title: Titles.ERROR,
          message,
        },
        duration: SNACKBAR_OPEN_DURATION_IN_SECONDS,
      })
      .afterDismissed()
      .pipe(map((a) => !!a));
  }

  prompt(title: string, message: string): Observable<string> {
    const dialogRef: MatDialogRef<DialogStringQuestionsComponent> =
      this.dialog.open(DialogStringQuestionsComponent, {
        autoFocus: false,
        disableClose: true,
        panelClass: 'mc-dialog',
      });

    dialogRef.componentInstance.title = title;
    dialogRef.componentInstance.message = message;

    return dialogRef.afterClosed();
  }

  close(): void {
    this.dialog.closeAll();
  }
}
