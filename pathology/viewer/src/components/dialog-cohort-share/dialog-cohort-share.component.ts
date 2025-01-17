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

import {Clipboard} from '@angular/cdk/clipboard';
import {COMMA, ENTER} from '@angular/cdk/keycodes';
import {CommonModule} from '@angular/common';
import {Component, OnDestroy, TemplateRef, ViewChild} from '@angular/core';
import {MatButtonModule} from '@angular/material/button';
import {MatChipInputEvent, MatChipsModule} from '@angular/material/chips';
import {MatOptionSelectionChange} from '@angular/material/core';
import {MatDialogModule, MatDialogRef} from '@angular/material/dialog';
import {MatDividerModule} from '@angular/material/divider';
import {MatIconModule} from '@angular/material/icon';
import {MatInputModule} from '@angular/material/input';
import {MAT_SELECT_CONFIG, MatSelectModule} from '@angular/material/select';
import {MatSnackBar, MatSnackBarConfig} from '@angular/material/snack-bar';
import {Router} from '@angular/router';
import {ReplaySubject} from 'rxjs';
import {takeUntil, tap} from 'rxjs/operators';

import {PathologyCohort, PathologyCohortAccess, SharePathologyCohortRequest} from '../../interfaces/cohorts';
import {PathologyUserAccess, PathologyUserAccessRole} from '../../interfaces/users';
import {CohortService} from '../../services/cohort.service';
import {DialogService} from '../../services/dialog.service';
import {LogService} from '../../services/log.service';
import {UserService} from '../../services/user.service';
import {WindowService} from '../../services/window.service';

const COHORT_ROLE_LABELS = {
  owner: 'PATHOLOGY_USER_ACCESS_ROLE_OWNER',
  admin: 'PATHOLOGY_USER_ACCESS_ROLE_ADMIN',
  editor: 'PATHOLOGY_USER_ACCESS_ROLE_EDITOR',
  viewer: 'PATHOLOGY_USER_ACCESS_ROLE_VIEWER',
};

const COHORT_ACCESS_LABELS = {
  unspecified: 'PATHOLOGY_COHORT_ACCESS_UNSPECIFIED' as PathologyCohortAccess,
  restricted: 'PATHOLOGY_COHORT_ACCESS_RESTRICTED' as PathologyCohortAccess,
  openEdit: 'PATHOLOGY_COHORT_ACCESS_OPEN_EDIT' as PathologyCohortAccess,
  openView: 'PATHOLOGY_COHORT_ACCESS_OPEN_VIEW_ONLY' as PathologyCohortAccess,
};

const EMAIL_VALIDATOR = /^[\w-\.]+@([\w-]+\.)+[\w-]{2,4}$/;

/**
 * The dialog for users to obtain a sharing link for a cohort.
 */
@Component({
  selector: 'dialog-cohort-share',
  standalone: true,
  imports: [
    CommonModule,
    MatSelectModule,
    MatDividerModule,
    MatIconModule,
    MatButtonModule,
    MatInputModule,
    MatChipsModule,
    MatDialogModule,
  ],
  templateUrl: './dialog-cohort-share.component.html',
  styleUrl: './dialog-cohort-share.component.scss',
  providers: [
    {provide: MAT_SELECT_CONFIG, useValue: {overlayPanelClass: 'mat-primary'}},
  ],

})
export class DialogCohortShareComponent implements OnDestroy {
  // Preview dialog.
  @ViewChild('discardConfirmationTemplate', {static: true})
  discardConfirmationTemplate!: TemplateRef<{}>;
  hasPendingChanges = false;
  isPublicAccess = false;
  modifiedUsers = new Set<string>();
  originalPathologyUserAccessRoleByEmail!: Map<string, PathologyUserAccessRole>;
  originalCohortAccess: PathologyCohortAccess =
      COHORT_ACCESS_LABELS.unspecified;
  shareAccessRole: PathologyUserAccessRole =
      COHORT_ROLE_LABELS.viewer as PathologyUserAccessRole;
  selectedCohort?: PathologyCohort;
  showDeIdWarning = this.cohortService.selectedCohortInfo$.value?.isDeid;

  readonly pathologyUserRoles = COHORT_ROLE_LABELS;
  readonly pathologyCohortAccess = COHORT_ACCESS_LABELS;
  readonly separatorKeysCodes = [ENTER, COMMA] as const;
  readonly pathologyUserRoleOptions: PathologyUserAccessRole[] = [
    COHORT_ROLE_LABELS.admin as PathologyUserAccessRole,
    COHORT_ROLE_LABELS.editor as PathologyUserAccessRole,
    COHORT_ROLE_LABELS.viewer as PathologyUserAccessRole,
  ];

  private cohortAccessLabel: PathologyCohortAccess =
      COHORT_ACCESS_LABELS.unspecified;
  private currentUser = '';
  private readonly destroy$ = new ReplaySubject();
  private emailList: string[] = [];
  private userAccessDisplayList: PathologyUserAccess[] = [];

  get pathologyUserAccess() {
    return this.userAccessDisplayList;
  }

  set pathologyUserAccess(userAccess: PathologyUserAccess[]) {
    userAccess =
        JSON.parse(JSON.stringify(userAccess)) as PathologyUserAccess[];
    // Parse owner out of userAccess, to place at top of list
    const ownerUserIndex = userAccess.findIndex((user) => {
      return user.accessRole === 'PATHOLOGY_USER_ACCESS_ROLE_OWNER';
    });
    const ownerUserAccess = userAccess.splice(ownerUserIndex, 1);

    // Sort users by user email
    userAccess.sort(
        (a, b) => (a.userEmail ?? '').localeCompare(b.userEmail ?? ''));

    userAccess = [...ownerUserAccess, ...userAccess];

    this.userAccessDisplayList = userAccess;
  }

  get emails() {
    return this.emailList;
  }

  set emails(emails: string[]) {
    const parseEmails = emails.filter((email: string) => {
      return EMAIL_VALIDATOR.test(email);
    });
    this.emailList = parseEmails;
    this.validatePendingChanges();
  }

  get cohortAccess() {
    return this.cohortAccessLabel;
  }

  set cohortAccess(cohortAccess: PathologyCohortAccess) {
    this.cohortAccessLabel = cohortAccess;
    this.validatePendingChanges();
  }

  constructor(
      private readonly clipboard: Clipboard,
      private readonly cohortService: CohortService,
      private readonly dialogService: DialogService,
      private readonly logService: LogService,
      private readonly snackBar: MatSnackBar,
      private readonly windowService: WindowService,
      private readonly router: Router,
      private readonly userService: UserService,
      public dialogRef: MatDialogRef<DialogCohortShareComponent>,
  ) {
    this.userService.getCurrentUser$()
        .pipe(takeUntil(this.destroy$))
        .subscribe((currentUser) => {
          this.currentUser = currentUser ?? '';
        });

    cohortService.selectedPathologyCohort$
        .pipe(takeUntil(this.destroy$), tap((selectedPathologyCohort) => {
                if (!selectedPathologyCohort) return;
                this.selectedCohortChanged(selectedPathologyCohort);
              }))
        .subscribe();
  }

  ngOnDestroy() {
    this.destroy$.next('');
    this.destroy$.complete();
  }

  accessRoleChanged(event: MatOptionSelectionChange, user: PathologyUserAccess):
      void {
    if (!event.isUserInput) return;
    if (!user.userEmail || !user.accessRole) return;
    const modifiedAccessRole: PathologyUserAccessRole = event.source.value;

    user.accessRole = modifiedAccessRole;

    // Check which users are modified and add to modifiedUsers
    const originalUserAccessRole: PathologyUserAccessRole|undefined =
        this.originalPathologyUserAccessRoleByEmail.get(user.userEmail);
    const isOriginalUser = user.accessRole === originalUserAccessRole;

    if (!originalUserAccessRole || !isOriginalUser) {
      this.modifiedUsers.add(user.userEmail);
    } else {
      this.modifiedUsers.delete(user.userEmail);
    }

    // Modify if pending changes based on modifiedUsers
    this.validatePendingChanges();
  }

  addEmail(event: MatChipInputEvent): void {
    const email: string = (event.value || '').trim().toLocaleLowerCase();

    // Add email
    if (email) {
      const emails = email.split(' ');
      this.emails = [...this.emails, ...emails];
    }

    // Clear input
    event.chipInput.clear();
  }

  backToCohortViewer(): void {
    this.emails = [];
  }

  closeDialog(): void {
    if (!this.hasPendingChanges) {
      this.dialogRef.close();
      return;
    }

    this.dialogService.confirm('Discard unsaved changes?')
        .afterClosed()
        .pipe(
            takeUntil(this.destroy$),
            tap((confirmClose: boolean) => {
              if (confirmClose) {
                this.dialogRef.close();
              }
            }),
            )
        .subscribe();
  }

  copyLink(): void {
    if (!this.selectedCohort || !this.selectedCohort.name) return;
    const cohortName = this.selectedCohort.name;

    const origin = this.windowService.getWindowOrigin();
    if (!origin) return;
    const url = `${origin}/cohorts?cohortName=${cohortName}`;

    const copySuccessful = this.clipboard.copy(url);

    const snackBarConfig = new MatSnackBarConfig();
    snackBarConfig.duration = 2000;

    if (!copySuccessful) {
      this.logService.error({
        name: 'Error copying share link',
        message: JSON.stringify({url}),
        stack: 'share_cohort_dialog',
      });
    }
    this.snackBar.open(
        copySuccessful ? 'URL copied.' : 'URL copy failed.', '',
        snackBarConfig);
  }

  generalAccessChanged(): void {
    if (!this.isPublicAccess) {
      this.cohortAccess = COHORT_ACCESS_LABELS.restricted;
    } else {
      this.cohortAccess = COHORT_ACCESS_LABELS.openView;
    }
  }

  pathologyUserAccessRoleToLabel(accessRole: PathologyUserAccessRole): string {
    return this.cohortService.pathologyUserAccessRoleToLabel(accessRole);
  }

  removeEmail(email: string): void {
    const index = this.emails.indexOf(email);
    if (index >= 0) {
      this.emails.splice(index, 1);
    }
  }

  removeUserAccess(event: MatOptionSelectionChange, user: PathologyUserAccess):
      void {
    if (!event.isUserInput || !user.userEmail) return;

    const foundUserIndex = this.pathologyUserAccess.findIndex(
        ({userEmail}) => userEmail === user.userEmail);

    if (foundUserIndex >= 0) {
      this.pathologyUserAccess.splice(foundUserIndex, 1);

      if (this.originalPathologyUserAccessRoleByEmail.has(user.userEmail)) {
        this.modifiedUsers.add(user.userEmail);
        this.validatePendingChanges();
      }
    }
  }

  savePendingChanges(): void {
    if (!this.selectedCohort) return;

    const request: SharePathologyCohortRequest = {
      name: this.selectedCohort.name,
      userAccess: this.pathologyUserAccess,
      cohortAccess: this.cohortAccess,
    };

    this.cohortService.shareCohort(request).subscribe((response) => {
      this.pathologyUserAccess = response.userAccess ?? [];

      this.validateCurretUserHasAccess(this.pathologyUserAccess);
    });
  }

  selectedCohortChanged(cohort: PathologyCohort): void {
    this.selectedCohort = cohort;
    this.pathologyUserAccess = this.selectedCohort.userAccess ?? [];

    // Setup cohort access based on selected cohort
    if (this.selectedCohort?.cohortMetadata?.cohortAccess) {
      this.originalCohortAccess =
          this.selectedCohort.cohortMetadata.cohortAccess;
      if (this.originalCohortAccess === COHORT_ACCESS_LABELS.unspecified) {
        this.cohortAccess = COHORT_ACCESS_LABELS.restricted;
      } else {
        this.cohortAccess = this.originalCohortAccess;
      }
      this.isPublicAccess =
          this.cohortAccess !== COHORT_ACCESS_LABELS.restricted;
    }

    // Setup for tracking modifications to user permissions, helper variables
    this.originalPathologyUserAccessRoleByEmail =
        new Map<string, PathologyUserAccessRole>(this.pathologyUserAccess.map(
            (user) => [user.userEmail!, user.accessRole!]));
    this.modifiedUsers.clear();

    this.validatePendingChanges();
  }

  shareEmails(emails: string[], shareAccessRole: PathologyUserAccessRole):
      void {
    if (!this.selectedCohort) return;

    const newUserAccess = emails.map((email) => {
      return {
        userEmail: email,
        accessRole: shareAccessRole,
      };
    });

    const shareCohortRequest: SharePathologyCohortRequest = {
      name: this.selectedCohort.name,
      userAccess: [...(this.selectedCohort.userAccess ?? []), ...newUserAccess],
    };

    this.cohortService.shareCohort(shareCohortRequest).subscribe((response) => {
      this.pathologyUserAccess = response.userAccess ?? [];
      this.backToCohortViewer();
    });
  }

  private routeToHome(): void {
    this.dialogRef.close();
    this.cohortService.unselectCohort();
    this.cohortService.reloadCohortInfos();

    this.router.navigateByUrl('/');
  }

  private validateCurretUserHasAccess(pathologyUserAccess:
                                          PathologyUserAccess[]): void {
    const foundUser = pathologyUserAccess.find(
        ({userEmail}) => userEmail === this.currentUser);

    if (!foundUser) {
      this.routeToHome();
      return;
    }
  }

  validatePendingChanges(): void {
    const modifiedCohortAccess =
        this.originalCohortAccess !== this.cohortAccess;
    const hasModifiedUsers = this.modifiedUsers.size > 0;
    const hasEmailsToAdd = this.emails.length > 0;

    this.hasPendingChanges =
        modifiedCohortAccess || hasModifiedUsers || hasEmailsToAdd;
  }
}