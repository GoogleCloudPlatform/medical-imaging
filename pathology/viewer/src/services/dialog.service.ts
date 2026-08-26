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
import { Observable, of } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import { ComponentType } from '@angular/cdk/portal';
import { MatSnackBar, MatSnackBarRef } from '@angular/material/snack-bar';
import { map } from 'rxjs/operators';
import { DialogConfirmationComponent } from '../components/dialog-confirmation/dialog-confirmation.component';
import { DialogStringQuestionsComponent } from '../components/dialog-string-questions/dialog-string-questions.component';
import { SnackBarErrorComponent } from '../components/snackbar-error/snackbar-error.component';
import { AuthStateService } from './auth-state.service';
import { isAuthHttpError } from '../utils/auth-helper.utils';

/**
 * Titles for various dialogs.
 */
export enum Titles {
  CONFIRM = 'Are you sure?',
  ERROR = 'Something went wrong...',
}

const SNACKBAR_OPEN_DURATION_IN_SECONDS = 4 * 1000;

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
    private readonly authState: AuthStateService,
  ) {}

  private activeErrorRef?: MatSnackBarRef<SnackBarErrorComponent>;

  confirm(message: string, title: Titles | string = Titles.CONFIRM): MatDialogRef<DialogConfirmationComponent> {
    const dialogRef: MatDialogRef<DialogConfirmationComponent> =
      this.dialog.open(DialogConfirmationComponent, {
        data: { title, message },
        autoFocus: false,
        disableClose: true,
        panelClass: 'mc-dialog',
        width: '25em',
        maxWidth: '100vw',
      });

    return dialogRef;
  }

  openComponentDialog<C, D>(
    component: ComponentType<C> | TemplateRef<C>,
    configs: MatDialogConfig<D>): MatDialogRef<C> {
    const dialogConfig = { maxWidth: '100vw', ...configs };
    const dialogRef: MatDialogRef<C> = this.dialog.open(component, dialogConfig);

    return dialogRef;
  }

  info(message: string, duration = 5000): void {
    this.snackBar.open(message, 'Close', {
      duration,
    });
  }

  error(message: string): Observable<boolean> {
    return this.openErrorSnackbar(message);
  }

  // Prefer this for HTTP errors so auth failures don’t show generic snackbars.
  errorFromHttp(err: unknown, fallbackMessage = 'Something went wrong.'): Observable<boolean> {
    if (isAuthHttpError(err) || this.authState.reauthInProgress) {
      return of(false);
    }

    const message =
      err instanceof HttpErrorResponse
        ? (err.error?.message || err.message || fallbackMessage)
        : (typeof err === 'string' ? err : fallbackMessage);

    return this.openErrorSnackbar(message);
  }

  openErrorSnackbar(message: string): Observable<boolean> {
    if (this.authState.reauthInProgress) return of(false);

    const normalized = (message ?? '').toString().replace(/\s+/g, ' ').trim();

    if (this.activeErrorRef) {
      this.activeErrorRef.dismiss();
      this.activeErrorRef = undefined;
    }

    const ref = this.snackBar.openFromComponent(SnackBarErrorComponent, {
      data: { title: Titles.ERROR, message: normalized },
      duration: SNACKBAR_OPEN_DURATION_IN_SECONDS,
      panelClass: ['dps-snackbar'],
    });

    this.activeErrorRef = ref;
    ref.afterDismissed().subscribe(() => {
      if (this.activeErrorRef === ref) this.activeErrorRef = undefined;
    });

    return of(true);
  }

  prompt(title: string, message: string): Observable<string> {
    const dialogRef: MatDialogRef<DialogStringQuestionsComponent> =
      this.dialog.open(DialogStringQuestionsComponent, {
        autoFocus: false,
        disableClose: true,
        panelClass: 'mc-dialog',
        maxWidth: '100vw',
      });

    dialogRef.componentInstance.title = title;
    dialogRef.componentInstance.message = message;

    return dialogRef.afterClosed();
  }

  close(): void {
    this.dialog.closeAll();
  }
}
