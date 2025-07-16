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
import {AbstractControl, FormControl, ValidationErrors, Validators} from '@angular/forms';
import {MAT_DIALOG_DATA, MatDialogModule} from '@angular/material/dialog';
import {MatSnackBar} from '@angular/material/snack-bar';
import {Observable, of, ReplaySubject} from 'rxjs';
import {filter, finalize, map, switchMap, takeUntil, tap} from 'rxjs/operators';

import {environment} from '../../environments/environment';
import {PathologyCohort} from '../../interfaces/cohorts';
import {RecordIdType} from '../../interfaces/hierarchy_descriptor';
import {RecordIdToSlideIds} from '../../interfaces/search';
import {CohortService} from '../../services/cohort.service';
import {DialogService} from '../../services/dialog.service';
import {SearchService} from '../../services/search.service';

// Delimits comma, semicolon, space, tabs, newlines
const DELIMITER_CHARS = environment.ID_DELIMITER;
const VALID_CHARS = `${environment.ID_VALIDATOR}${environment.ID_DELIMITER}`;

const DELIMITERS = new RegExp(`[${DELIMITER_CHARS}]+`, 'g');
const VALIDATOR = new RegExp(`^[${VALID_CHARS}]+$`, 'g');
const INVERSE_VALIDATOR = new RegExp(`[^${VALID_CHARS}]`, 'g');

/** Type of create cohort component. */
export enum EditModeType {
  ALL,
  SLIDES,
  NAME_DESCRIPTION,
}

/** Inputs that can be dynamically bound to the dialog component. */
export interface CreateCohortDialogData {
  description?: string;
  displayName?: string;
  editMode?: EditModeType;
}
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatIconModule} from '@angular/material/icon';
import {FormsModule, ReactiveFormsModule} from '@angular/forms';
import {MatRadioModule} from '@angular/material/radio';
import {CommonModule} from '@angular/common';
import {MatInputModule} from '@angular/material/input';
import {MatButtonModule} from '@angular/material/button';

/**
 * The dialog for users to create a new cohort with.
 */
@Component({
  selector: 'dialog-cohort-create',
  standalone: true,
  imports: [
    MatDialogModule, MatFormFieldModule, MatIconModule, FormsModule,
    ReactiveFormsModule, MatRadioModule, MatInputModule, MatButtonModule,
    CommonModule
],
  templateUrl: './dialog-cohort-create.component.html',
  styleUrl: './dialog-cohort-create.component.scss'
})
export class DialogCohortCreateComponent {
  caplockIds = true;
  displayName = new FormControl('', Validators.required);
  description = new FormControl('');
  idType: RecordIdType = 'slideId';
  commaSeparatedIds =
      new FormControl('', (control: AbstractControl): ValidationErrors|null => {
        const text = control.value ?? '';

        if (text.length && !VALIDATOR.test(text)) {
          const invalidUniqueChars: string = [
            ...new Set([...text.matchAll(INVERSE_VALIDATOR)].map(
                ((match) => match[0])))
          ].join(', ');
          return invalidUniqueChars ? {'invalidChars': invalidUniqueChars} :
                                      null;
        }

        return this.getIdsFromText(text).length === 0 ?
            {'required': this.editMode} :
            null;
      });

  badIds: string[] = [];
  editMode: EditModeType = EditModeType.ALL;
  editModeType: typeof EditModeType = EditModeType;
  modifyCohortLoading = false;
  hasDisplayNameError = false;
  hasIdsError = false;
  private readonly destroyed$ = new ReplaySubject<boolean>(1);

  constructor(
      private readonly cohortService: CohortService,
      private readonly dialogService: DialogService,
      private readonly searchService: SearchService,
      private readonly snackBar: MatSnackBar,
      @Optional() @Inject(MAT_DIALOG_DATA) public data?: CreateCohortDialogData,
  ) {
    this.editMode = data?.editMode ?? EditModeType.ALL;
    const {displayName, description} = this.data ?? {};
    if (displayName) {
      this.displayName.patchValue(displayName);
    }
    if (description) {
      this.description.patchValue(description);
    }
  }

  ngOnDestroy() {
    this.destroyed$.next(true);
    this.destroyed$.complete();
  }

  modifyCohort(): void {
    if ((this.editMode === EditModeType.ALL ||
         this.editMode === EditModeType.NAME_DESCRIPTION) &&
        this.displayName.hasError('required')) {
      this.displayName.updateValueAndValidity();
      this.displayName.markAsTouched();
      return;
    }

    if (this.editMode === EditModeType.NAME_DESCRIPTION) {
      this.modifyCohortDisplayNameAndDescription(
              this.displayName.value ?? '', this.description.value ?? '')
          .subscribe();
      return;
    }

    if (this.editMode === EditModeType.ALL ||
        this.editMode === EditModeType.SLIDES) {
      this.commaSeparatedIds.updateValueAndValidity();
      if (this.commaSeparatedIds.hasError('required') ||
          this.commaSeparatedIds.hasError('invalidChars')) {
        this.commaSeparatedIds.markAsTouched();
        return;
      }

      const commaSeparatedIds: string = this.commaSeparatedIds.value ?? '';
      this.modifyCohortLoading = true;
      this.commaSeparatedIds.setErrors(null);

      this.validateIds(this.getIdsFromText(commaSeparatedIds))
          .pipe(
              takeUntil(this.destroyed$),
              switchMap((recordSlides: RecordIdToSlideIds[]) => {
                const snackMessage =
                    this.editMode ? 'Appending ids...' : 'Creating cohort...';
                this.snackBar.open(snackMessage);

                return this.editMode ? this.appendIdsToCohort(recordSlides) :
                                       this.createCohort(recordSlides);
              }),
              finalize(() => {
                this.modifyCohortLoading = false;
              }),
              )
          .subscribe();
    }
  }

  modifyCohortDisplayNameAndDescription(
      displayName: string, description: string) {
    this.modifyCohortLoading = true;
    this.snackBar.open('Saving display name and description...');
    return this.cohortService
        .updateCohortDisplayNameAndDescription(displayName, description)
        .pipe(
            takeUntil(this.destroyed$), tap((success) => {
              if (success) {
                this.snackBar.open('Display name and description saved.');
                this.cohortService.reloadSelectedCohort();
                this.dialogService.close();
              } else {
                this.snackBar.dismiss();
              }
            }),
            finalize(() => {
              this.modifyCohortLoading = false;
            }));
  }

  createCohort(recordSlides: RecordIdToSlideIds[]) {
    return this.cohortService
        .createCohort(
            this.displayName.getRawValue()!,
            recordSlides.flatMap((recordSlide) => recordSlide.slideIds)
                .map((slide) => slide.slideId),
            this.description.getRawValue() ?? undefined)
        .pipe(
            takeUntil(this.destroyed$),
            tap((newCohort: PathologyCohort|void) => {
              this.snackBar.dismiss();
              if (newCohort) {
                this.dialogService.close();
                this.cohortService.loadAllCohorts();
                this.cohortService.routeToCohort(newCohort.name!);
              }
            }),
        );
  }

  appendIdsToCohort(recordSlides: RecordIdToSlideIds[]) {
    return this.cohortService
        .addSlidesToCohort(
            this.cohortService.getSelectedCohortName(),
            recordSlides.flatMap((recordSlide) => recordSlide.slideIds)
                .map((slide) => slide.slideId))
        .pipe(
            takeUntil(this.destroyed$),
            tap((success) => {
              if (success) {
                this.dialogService.close();
              }
            }),
        );
  }

  validateIds(ids: string[]): Observable<RecordIdToSlideIds[]> {
    if (!ids.length) {
      return of([]);
    }

    this.snackBar.open('Validating ids...');
    return this.searchService
        .getSlideDicomPathFromListOfRecordIds(ids, this.idType)
        .pipe(
            takeUntil(this.destroyed$),
            map((recordSlides: RecordIdToSlideIds[]) => {
              const badIds = this.computeBadIds(recordSlides);
              if (badIds.length) {
                this.commaSeparatedIds.setErrors({'badIds': badIds.join(', ')});
                this.modifyCohortLoading = false;
                return;
              }
              return recordSlides;
            }),
            filter(
                (recordSlides):
                    recordSlides is RecordIdToSlideIds[] => {
                      return recordSlides !== undefined &&
                          recordSlides?.length > 0;
                    }),
        );
  }

  toggleCaplockIds(): boolean {
    this.caplockIds = !this.caplockIds;
    this.forceUppercaseConditionally();

    return this.caplockIds;
  }

  forceUppercaseConditionally(): string {
    const commaSeparatedIds = this.commaSeparatedIds.getRawValue() ?? '';

    if (this.caplockIds) {
      this.commaSeparatedIds.patchValue(commaSeparatedIds.toUpperCase());
    } else {
      this.commaSeparatedIds.patchValue(commaSeparatedIds);
    }

    return this.commaSeparatedIds.getRawValue() ?? '';
  }

  cancel(): void {
    this.dialogService.close();
  }

  computeBadIds(recordSlides: RecordIdToSlideIds[]): string[] {
    const badIds: string[] =
        recordSlides.filter((slides) => slides.slideIds.length === 0)
            .map((slides) => slides.recordId);
    return badIds;
  }

  getIdsFromText(text: string|null): string[] {
    return text ? text.split(DELIMITERS).filter((id: string) => id !== '') : [];
  }
}