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

import {Injectable, OnDestroy} from '@angular/core';
import {Params, Router, UrlSerializer} from '@angular/router';
import {BehaviorSubject, combineLatest, Observable, of, ReplaySubject, Subscription} from 'rxjs';
import {catchError, distinctUntilChanged, filter, finalize, first, ignoreElements, map, mergeMap, switchMap, takeUntil, tap} from 'rxjs/operators';

import {environment} from '../environments/environment';
import {CopyPathologyCohortRequest, ExportPathologyCohortRequest, ListPathologyCohortsRequest, ListPathologyCohortsResponse, PathologyCohort, SavePathologyCohortRequest, SharePathologyCohortRequest, TransferDeIdPathologyCohortRequest, UnsavePathologyCohortRequest, UpdatePathologyCohortRequest} from '../interfaces/cohorts';
import {Case} from '../interfaces/hierarchy_descriptor';
import {PathologySlide} from '../interfaces/slide_descriptor';
import {PathologyUserAccessRole} from '../interfaces/users';
import {forkJoinWithProgress} from '../utils/common';

import {BusyService} from './busy.service';
import {DialogService} from './dialog.service';
import {computeDicomUrisByCaseId, DicomwebService, studyDicomModelToCase} from './dicomweb.service';
import {LogService} from './log.service';
import {OrchestratorService} from './orchestrator.service';
import {UserService} from './user.service';

const SHARE_COHORT_ERROR_MESSAGE_START_1 = 'The following user(s)';
const TRANSFER_DEID_COHORT_ERROR_TIMEOUT_MESSAGE =
    'The server encountered a temporary error and could not complete your request.<p>Please try again in 30 seconds.';


/**
 * Communicates cohort and user data with the cohort.
 */
@Injectable({providedIn: 'root'})
export class CohortService implements OnDestroy {
  readonly cohorts$ = new ReplaySubject<CohortInfo[]>(1);
  private currentUser = '';

  // Undefined means no cohort selected (yet).
  readonly selectedCohortInfo$ =
      new BehaviorSubject<CohortInfo|undefined>(undefined);
  readonly selectedPathologyCohort$ =
      new BehaviorSubject<PathologyCohort|undefined>(undefined);
  readonly selectedPathologyCohortCases$ = new BehaviorSubject<Case[]>([]);

  readonly loading$ = new BehaviorSubject<boolean>(false);
  readonly loadingCohortInfos$ = new BehaviorSubject<boolean>(false);
  readonly loadingSelectedPathologyCohort$ =
      new BehaviorSubject<boolean>(false);
  readonly loadingSelectedPathologyCohortCases$ =
      new BehaviorSubject<boolean>(false);
  readonly loadingProgressSelectedPathologyCohortCases$ =
      new BehaviorSubject<number>(0);
  readonly selectedCohortCases =
      new BehaviorSubject<Array<ReplaySubject<Case>>>([]);
  cohortsEnabled = false;
  private readonly destroyed$ = new ReplaySubject<boolean>(1);
  private fetchPathologyCohortsSubscription?: Subscription;
  private fetchPathologyCohortSubscription?: Subscription;
  readonly selectedCohortIsReadOnly$ = new BehaviorSubject<boolean>(true);
  constructor(
      private readonly orchestratorService: OrchestratorService,
      private readonly dicomwebService: DicomwebService,
      private readonly busyService: BusyService,
      private readonly logService: LogService,
      private readonly dialogService: DialogService,
      private readonly router: Router,
      private readonly userService: UserService,
      private readonly urlSerializer: UrlSerializer,
  ) {
    this.cohortsEnabled = environment.ENABLE_COHORTS;
    this.setupService();
  }

  private setupService() {
    if (!this.cohortsEnabled) return;
    this.userService.getCurrentUser$().subscribe((currentUser) => {
      this.currentUser = currentUser ?? '';
    });
    this.loading$.next(true);

    // Updates selectedCohortIsReadOnly based on current user and
    // selectedPathologyCohort
    combineLatest([
      this.userService.getCurrentUser$(),
      this.selectedPathologyCohort$,
    ])
        .pipe(
            takeUntil(this.destroyed$),
            tap(([currentUser, selectedPathologyCohort]) => {
              if (currentUser && selectedPathologyCohort) {
                const foundUser =
                    (selectedPathologyCohort.userAccess ??
                     []).find(({userEmail}) => userEmail === currentUser);

                const isUserReadOnly: boolean = foundUser?.accessRole ===
                    'PATHOLOGY_USER_ACCESS_ROLE_VIEWER';
                this.selectedCohortIsReadOnly$.next(isUserReadOnly);
              } else {
                this.selectedCohortIsReadOnly$.next(true);
              }
            }))
        .subscribe();

    // Combined loading state for page.
    combineLatest([
      this.loadingCohortInfos$,
      this.loadingSelectedPathologyCohort$,
      this.loadingSelectedPathologyCohortCases$,
    ])
        .pipe(
            takeUntil(this.destroyed$),
            tap((mergedLoading) => {
              const allLoading =
                  mergedLoading.some((loading) => loading === true);
              this.loading$.next(allLoading);
            }),
            )
        .subscribe();

    this.selectedPathologyCohort$
        .pipe(
            // Update selectedCohortInfo to match selectedPathologyCohort.
            tap((selectedPathologyCohort) => {
              const isSelectedCohortInfoSame: boolean =
                  (this.selectedCohortInfo$.getValue() &&
                   this.selectedCohortInfo$.getValue()?.name ===
                       selectedPathologyCohort?.name) ??
                  false;

              if (!isSelectedCohortInfoSame && selectedPathologyCohort) {
                this.selectedCohortInfo$.next(
                    this.pathologyCohortToCohortInfo(selectedPathologyCohort));
              }
            }),
            // Fetch cases
            switchMap((selectedPathologyCohort) => {
              if (selectedPathologyCohort) {
                this.selectedPathologyCohortCases$.next([]);
                return this.fetchPathologyCohortCases(selectedPathologyCohort);
              }
              return of(undefined);
            }),
            finalize(() => {
              this.loadingSelectedPathologyCohort$.next(false);
            }))
        .subscribe();
  }

  fetchPathologyCohortCases(pathologyCohort: PathologyCohort):
      Observable<Case[]> {
    const slidesByCaseUid: Map<string, PathologySlide[]> =
        pathologyCohort.slides ?
        computeDicomUrisByCaseId(pathologyCohort.slides) :
        new Map<string, PathologySlide[]>();

    const casesDetailsObs =
        [...slidesByCaseUid.keys()].map((caseUid: string) => {
          const slidesPerCase: PathologySlide[] =
              slidesByCaseUid.get(caseUid) ?? [];
          return this.fetchCase(caseUid, slidesPerCase, pathologyCohort);
        });

    this.loadingProgressSelectedPathologyCohortCases$.next(0);
    this.loadingSelectedPathologyCohortCases$.next(true);
    return forkJoinWithProgress(casesDetailsObs)
        .pipe(
            mergeMap(({result, progress}) => {
              progress
                  .pipe(
                      tap((value: number) => {
                        this.loadingProgressSelectedPathologyCohortCases$.next(
                            value);
                      }),
                      ignoreElements(),
                      )
                  .subscribe();
              return result;
            }),
            tap((finalResult) => {
              this.selectedPathologyCohortCases$.next(finalResult);
            }),
            finalize(() => {
              this.loadingSelectedPathologyCohortCases$.next(false);
              this.loadingProgressSelectedPathologyCohortCases$.next(0);
            }));
  }

  private fetchCase(
      caseUid: string,
      slidesPerCase: PathologySlide[],
      pathologyCohort: PathologyCohort,
      ): Observable<Case> {
    const errorCase: Case = {
      accessionNumber: 'failed to load',
      caseId: caseUid,
      date: '',
      failedToLoad: true,
      slides: slidesPerCase,
    };
    const isDeId = pathologyCohort?.cohortMetadata?.isDeid ?? false;
    return this.dicomwebService.getStudyMeta(caseUid).pipe(
        map(response => {
          if (!response || response.length === 0) {
            this.logService.error({
              name: 'Can not get study meta data',
              message: `No meta returned ${caseUid}, skipping`,
              stack: 'cohortService: cohortsToCases'
            });
            return errorCase;
          }

          const caseObj = studyDicomModelToCase(caseUid, response[0], isDeId);
          caseObj.slides = slidesPerCase;
          return caseObj;
        }),
        catchError((error) => {
          this.logService.error({
            name: 'Can not get study meta data',
            message: `No meta returned ${caseUid}, skipping. Error: ${error}`,
            stack: 'cohortService: cohortsToCases'
          });
          return of(errorCase);
        }),
    );
  }

  fetchPathologyCohort(cohortName: string) {
    this.loadingSelectedPathologyCohort$.next(true);
    this.fetchPathologyCohortSubscription?.unsubscribe();
    this.fetchPathologyCohortSubscription =
        this.orchestratorService.getPathologyCohort({name: cohortName})
            .pipe(
                takeUntil(this.destroyed$),
                tap((pathologyCohort: PathologyCohort) => {
                  // Don't allow users to view deleted cohorts
                  if (pathologyCohort?.cohortMetadata?.cohortStage ===
                      'PATHOLOGY_COHORT_LIFECYCLE_STAGE_SUSPENDED') {
                    throw new Error();
                  }
                  this.selectedPathologyCohort$.next(pathologyCohort);
                  this.loadingSelectedPathologyCohort$.next(false);
                }),
                catchError((error: Error) => {
                  this.loadingSelectedPathologyCohort$.next(false);
                  this.logService.error({
                    name: 'Error dialog',
                    message: JSON.stringify(error),
                    stack: 'cohort_service'
                  });
                  const cohortIdDelimiter = 'pathologyCohorts/';
                  const selectedCohortName = this.router.url;
                  const cohortId: string = selectedCohortName.slice(
                      selectedCohortName.lastIndexOf(cohortIdDelimiter) +
                      cohortIdDelimiter.length);

                  const errorMessage =
                      `User does not have permission to access cohort '${
                          cohortId}' or it does not exist.`;
                  this.dialogService.error(errorMessage)
                      .pipe(takeUntil(this.destroyed$))
                      .subscribe((confirmClose: boolean) => {
                        if (confirmClose) {
                          this.unselectCohort();
                          this.router.navigateByUrl('/');
                        }
                      });
                  return of(undefined);
                }),
                )
            .subscribe();
  }


  loadAllCohorts(): void {
    const cohortRequest: ListPathologyCohortsRequest = {
      view: 'PATHOLOGY_COHORT_VIEW_METADATA_ONLY',
    };
    this.loadingCohortInfos$.next(true);
    // Previous request is still running.
    if (this.fetchPathologyCohortsSubscription) return;

    this.fetchPathologyCohortsSubscription =
        this.orchestratorService.listPathologyCohorts(cohortRequest)
            .pipe(
                takeUntil(this.destroyed$),
                tap((listPathologyCohortsResponse:
                         ListPathologyCohortsResponse) => {
                  const activeCohortInfos: CohortInfo[] =
                      (listPathologyCohortsResponse.pathologyCohorts ?? [])
                          .filter((cohort) => {
                            return cohort.cohortMetadata?.cohortStage ===
                                'PATHOLOGY_COHORT_LIFECYCLE_STAGE_ACTIVE';
                          })
                          .map((cohort) => {
                            return this.pathologyCohortToCohortInfo(cohort);
                          });
                  activeCohortInfos.sort(
                      (a, b) => a.displayName.localeCompare(b.displayName));

                  this.cohorts$.next(activeCohortInfos);
                  this.loadingCohortInfos$.next(false);
                }),
                catchError((error) => {
                  this.logService.error({
                    name: 'Error loading cohorts',
                    message: JSON.stringify(error),
                    stack: 'cohort_service',
                  });
                  const errorMessage = 'Failed to load cohorts.';
                  this.dialogService.error(errorMessage);
                  this.loadingCohortInfos$.next(false);
                  throw new Error(errorMessage);
                }),
                )
            .subscribe(() => {
              this.fetchPathologyCohortsSubscription = undefined;
            });
  }

  parseCohorts(
      pathologyCohorts: PathologyCohort[],
      findUserEmail: string,
      findAccessRole?: PathologyUserAccessRole,
      ): PathologyCohort[] {
    if (!findAccessRole) {
    }
    return pathologyCohorts.filter(({userAccess}) => {
      const currentUser = (userAccess ?? []).find(({userEmail}) => {
        return userEmail === findUserEmail;
      });
      return currentUser?.accessRole === findAccessRole;
    });
  }

  shareCohort(cohortShareRequest: SharePathologyCohortRequest):
      Observable<PathologyCohort> {
    return this.orchestratorService.sharePathologyCohort(cohortShareRequest)
        .pipe(
            tap((cohort: PathologyCohort) => {
              this.selectedPathologyCohort$.next(cohort);
            }),
            catchError((error) => {
              const invalidUsersErrorIndex =
                  error.message.indexOf(SHARE_COHORT_ERROR_MESSAGE_START_1);

              let customMessage = 'Failed to save share cohort.';
              if (invalidUsersErrorIndex !== -1) {
                customMessage = error.message.substring(
                    invalidUsersErrorIndex,
                    Number(error.message.lastIndexOf('.')) + 1);
              }
              this.logService.error({
                name: 'Error dialog',
                message: JSON.stringify(error),
                stack: 'cohort_service',
              });
              this.dialogService.error(customMessage);
              return of();
            }),
        );
  }

  selectCohortInfo(selectedCohortInfo: CohortInfo) {
    this.selectedCohortInfo$.next(selectedCohortInfo);
  }

  unselectCohort() {
    this.selectedPathologyCohort$.next(undefined);
    this.selectedCohortInfo$.next(undefined);
  }

  isCohortSelected() {
    return this.selectedCohortInfo$.getValue() !== undefined;
  }

  getSelectedCohortName() {
    return this.selectedPathologyCohort$.getValue()?.name ?? '';
  }

  getSelectedCohortName$() {
    return this.selectedPathologyCohort$.pipe(map(cohort => cohort?.name));
  }

  getSelectedCohortDisplayName(): string {
    return this.selectedPathologyCohort$.getValue()
               ?.cohortMetadata?.displayName ??
        '';
  }

  getSelectedCohortDisplayName$() {
    return this.selectedPathologyCohort$.pipe(
        map(cohort => cohort?.cohortMetadata?.displayName));
  }

  getSelectedCohortDescription(): string {
    return this.selectedPathologyCohort$.getValue()
               ?.cohortMetadata?.description ??
        '';
  }

  getSelectedCohortDescription$(): Observable<string> {
    return this.selectedPathologyCohort$.pipe(
        map(cohort => cohort?.cohortMetadata?.description ?? ''));
  }

  getSelectedSlidesDicomUris$() {
    return this.selectedPathologyCohort$.pipe(
        distinctUntilChanged(),
        filter((selectedCohort) => selectedCohort !== undefined),
        map((cohort) => {
          return cohort?.slides?.map(({dicomUri}) => dicomUri!) ?? [];
        }));
  }

  // Get a pathology cohort. If cohort is already cached locally return the
  // cached version.
  getPathologyCohort(cohortName: string) {
    // Use the selected cohort iff it is the right cohort.
    if (this.selectedPathologyCohort$.getValue()?.name === cohortName) {
      return of(this.selectedPathologyCohort$.getValue());
    }
    return this.orchestratorService.getPathologyCohort({name: cohortName});
  }

  reloadCohortInfos() {
    this.loadAllCohorts();
  }

  reloadSelectedCohort() {
    if (this.isCohortSelected()) {
      this.fetchPathologyCohort(this.selectedCohortInfo$.getValue()!.name);
    }
  }

  async reloadSelectCohortIfNameMatching(cohortName: string) {
    if (this.selectedPathologyCohort$.getValue()?.name === cohortName) {
      this.fetchPathologyCohort(this.selectedPathologyCohort$.getValue()!.name!
      );
    }
  }

  pathologyUserAccessRoleToLabel(accessRole: PathologyUserAccessRole): string {
    switch (accessRole) {
      case 'PATHOLOGY_USER_ACCESS_ROLE_OWNER':
        return 'Owner';
      case 'PATHOLOGY_USER_ACCESS_ROLE_ADMIN':
        return 'Admin';
      case 'PATHOLOGY_USER_ACCESS_ROLE_EDITOR':
        return 'Editor';
      case 'PATHOLOGY_USER_ACCESS_ROLE_VIEWER':
        return 'Viewer';
      default:
        this.logService.error({
          name: 'Error converting pathology UserAccessRole to Label',
          message: JSON.stringify({accessRole}),
          stack: 'cohort_service',
        });
        return '';
    }
  }

  pathologyCohortToCohortInfo(
      cohort: PathologyCohort,
      ): CohortInfo {
    const currentUserAccess: PathologyUserAccessRole =
        (cohort.userAccess ?? [])
                .find(({userEmail}) => {
                  return userEmail === this.currentUser;
                })
                ?.accessRole as PathologyUserAccessRole ??
        'PATHOLOGY_USER_ACCESS_ROLE_UNSPECIFIED';
    return {
      displayName: cohort.cohortMetadata?.displayName ?? '',
      name: cohort.name ?? '',
      access: currentUserAccess,
      isDeid: cohort?.cohortMetadata?.isDeid ?? false,  // safer assumes has phi
      isShared: currentUserAccess !== 'PATHOLOGY_USER_ACCESS_ROLE_OWNER',
      isExported: cohort?.cohortMetadata?.cohortBehaviorConstraints ===
          'PATHOLOGY_COHORT_BEHAVIOR_CONSTRAINTS_CANNOT_BE_EXPORTED',
      pathologyCohort: cohort,
    };
  }

  private addSlidesToPathologyCohort(
      pathologyCohort: PathologyCohort, dicomUris: string[]) {
    pathologyCohort = {...pathologyCohort};
    // Filter out slides that already exist in the cohort.
    const existingUris = pathologyCohort?.slides?.map(slide => slide.dicomUri);
    const filteredUris = existingUris ?
        dicomUris.filter(uri => !existingUris.includes(uri)) :
        dicomUris;
    if (filteredUris.length === 0) {
      throw new AlreadyExistsError();
    }
    // Add the remaining slides to the cohort.
    const slidesToAdd =
        filteredUris.map(dicomUri => ({dicomUri} as PathologySlide));
    pathologyCohort.slides = [
      ...(pathologyCohort.slides ? pathologyCohort.slides : []), ...slidesToAdd
    ];

    return pathologyCohort;
  }

  addSlidesToCohort(cohortName: string, dicomUris: string[]):
      Observable<boolean> {
    this.loading$.next(true);
    return this.busyService.getIsBusyState$().pipe(
        // Wait until not busy.
        first(busy => !busy.isBusy), tap(() => {
          this.busyService.setIsBusy(
              `Adding slide${dicomUris.length === 1 ? '' : 's'} to cohort...`);
        }),
        switchMap(() => this.getPathologyCohort(cohortName)),
        map(pathologyCohort => {
          pathologyCohort =
              this.addSlidesToPathologyCohort(pathologyCohort!, dicomUris);
          // Prepare UpdatePathologyCohortRequest.
          return {
            pathologyCohort,
            updateMask: 'slides',
            view: 'PATHOLOGY_COHORT_VIEW_METADATA_ONLY',
          };
        }),
        // Perform update orchestrator call.
        switchMap(request => {
          return this.orchestratorService.updatePathologyCohort(
              request as UpdatePathologyCohortRequest,
              this.selectedPathologyCohort$.getValue()!,
          );
        }),
        tap(() => {
          this.busyService.setIsNotBusy();
          this.reloadSelectCohortIfNameMatching(cohortName);
          this.loading$.next(false);
        }),
        map(() => true),
        // Handle errors.
        catchError(e => {
          this.loading$.next(false);
          if (!(e instanceof AlreadyExistsError)) {
            this.logService.error({
              name: 'Error dialog',
              message: JSON.stringify(e),
              stack: 'cohort_service'
            });
            this.dialogService.error('Error while adding slide to cohort.');
          }
          this.busyService.setIsNotBusy();
          if (e instanceof AlreadyExistsError) {
            return of(true);
          }
          return of(false);
        }));
  }

  private removeSlideFromCohortSlideList(
      pathologyCohort: PathologyCohort, dicomUris: string[]) {
    pathologyCohort = {...pathologyCohort};
    const existingUris = pathologyCohort?.slides?.map(slide => slide.dicomUri);
    pathologyCohort.slides = existingUris ?
        pathologyCohort?.slides?.filter(
            slide => !slide.dicomUri || !dicomUris.includes(slide.dicomUri)) :
        [];
    return pathologyCohort;
  }

  // Remove slides from a cohort. Slide not existing in the cohort won't cause
  // this call to fail.
  removeSlidesFromCohort(cohortName: string, dicomUris: string[]):
      Observable<boolean> {
    this.loading$.next(true);
    return this.busyService.getIsBusyState$().pipe(
        // Wait until not busy.
        first(busy => !busy.isBusy), tap(() => {
          this.busyService.setIsBusy(`Removing slide${
              dicomUris.length === 1 ? '' : 's'} to cohort...`);
        }),
        // Fetch cohort.
        switchMap(() => this.getPathologyCohort(cohortName)), first(),
        map(pathologyCohort => {
          pathologyCohort =
              this.removeSlideFromCohortSlideList(pathologyCohort!, dicomUris);
          // Prepare UpdatePathologyCohortRequest.
          return {
            pathologyCohort,
            updateMask: 'slides',
            view: 'PATHOLOGY_COHORT_VIEW_METADATA_ONLY',
          };
        }),
        // Perform update orchestrator call.
        switchMap(
            request => this.orchestratorService.updatePathologyCohort(
                request as UpdatePathologyCohortRequest,
                this.selectedPathologyCohort$.getValue()!,
                )),
        tap(() => {
          this.busyService.setIsNotBusy();
          this.reloadSelectCohortIfNameMatching(cohortName);
          this.loading$.next(false);
        }),
        // Return true if succcessful.
        map(() => true),
        // Handle errors.
        catchError(e => {
          this.loading$.next(false);
          this.logService.error({
            name: 'Error dialog',
            message: JSON.stringify(e),
            stack: 'cohort_service'
          });
          this.dialogService.error('Error while removing slide from cohort.');
          return of(false);
        }));
  }

  removeCasesFromCohort(cohortName: string, caseUids: string[]):
      Observable<boolean> {
    const dicomUrisByCaseId = computeDicomUrisByCaseId(
        this.selectedPathologyCohort$.getValue()!.slides ?? []);
    const dicomUris = caseUids.reduce((uris, caseUid) => {
      const foundDicomUris =
          dicomUrisByCaseId.get(caseUid)!.map((slide) => slide.dicomUri)
              .filter((uri): uri is string => !!uri);
      uris = new Set([...uris, ...foundDicomUris]);
      return uris;
    }, new Set<string>());


    return this.removeSlidesFromCohort(cohortName, [...dicomUris]);
  }

  ngOnDestroy() {
    this.selectedCohortInfo$.unsubscribe();
    this.destroyed$.next(true);
    this.destroyed$.complete();
  }

  routeToCohort(name: string, token?: string) {
    const cohortPath = this.getCohortPath(name, token);

    this.router.navigateByUrl(cohortPath);
  }

  private getCohortPath(name: string, token?: string): string {
    const queryParams: Params = {[COHORT_ID_PARAM_KEY]: name};
    if (token) {
      queryParams[LINK_TOKEN_PARAM_KEY] = token;
    }

    return this.urlSerializer.serialize(
        this.router.createUrlTree(['cohorts'], {queryParams}));
  }

  routeToSelectedCohort() {
    if (this.isCohortSelected()) {
      this.routeToCohort(this.getSelectedCohortName());
    }
  }

  createCohort(
      displayName: string,
      dicomUris: string[],
      description?: string,
      ): Observable<PathologyCohort|void> {
    return this.orchestratorService
        .createPathologyCohort({
          pathologyCohort: {
            cohortMetadata: {
              displayName,
              description,
              cohortAccess: 'PATHOLOGY_COHORT_ACCESS_RESTRICTED'
            },
            slides: dicomUris.map((dicomUri: string) => ({dicomUri})),
          },
        })
        .pipe(
            map((newCohort: PathologyCohort):
                    PathologyCohort => {
                      if (!newCohort.name) {
                        throw new Error(
                            'Newly created cohort did not have a name.');
                      }
                      return newCohort;
                    }),
            catchError(error => {
              this.logService.error({
                name: 'Error dialog',
                message: JSON.stringify(error),
                stack: 'cohort_service',
              });
              this.dialogService.error('Failed to create cohort.');
              return of();
            }),
        );
  }

  updateCohortDisplayNameAndDescription(
      displayName: string,
      description: string,
      ): Observable<boolean> {
    const cohortName = this.getSelectedCohortName();

    return this.getPathologyCohort(cohortName)
        .pipe(
            switchMap((cohort: PathologyCohort|undefined) => {
              return this.orchestratorService.updatePathologyCohort(
                  {
                    pathologyCohort: {
                      name: this.getSelectedCohortName(),
                      cohortMetadata: {
                        ...cohort?.cohortMetadata,
                        ...{
                          displayName, description,
                        }
                      },
                    },
                    updateMask: ['displayName', 'description'].join(','),
                  },
                  this.selectedPathologyCohort$.getValue()!,
              );
            }),
            tap(() => {
              this.reloadCohortInfos();
              this.reloadSelectedCohort();
            }),
            map(() => true),
            catchError((error) => {
              this.logService.error({
                name: 'Error dialog',
                message: JSON.stringify(error),
                stack: 'cohort_service',
              });
              this.dialogService.error('Failed to save cohort display name.');
              return of(false);
            }),
        );
  }

  saveCohort(saveCohortRequest: SavePathologyCohortRequest):
      Observable<boolean> {
    return this.orchestratorService.savePathologyCohort(saveCohortRequest)
        .pipe(
            map(() => true),
            catchError((error) => {
              this.logService.error({
                name: 'Error dialog',
                message: JSON.stringify(error),
                stack: 'cohort_service',
              });
              this.dialogService.error('Failed to save shared cohort.');
              return of(false);
            }),
        );
  }

  unsaveCohort(unsaveRequest: UnsavePathologyCohortRequest):
      Observable<boolean> {
    return this.orchestratorService.unsavePathologyCohort(unsaveRequest)
        .pipe(
            map(() => true),
            catchError((error) => {
              this.logService.error({
                name: 'Error dialog',
                message: JSON.stringify(error),
                stack: 'cohort_service',
              });
              this.dialogService.error('Failed to unsave shared cohort.');
              return of(false);
            }),
        );
  }

  exportCohort(gcsDestPath: string): Observable<boolean> {
    const request: ExportPathologyCohortRequest = {
      name: this.getSelectedCohortName(),
      gcsDestPath,
    };

    return this.orchestratorService.exportPathologyCohort(request).pipe(
        map(() => true),
        catchError((error) => {
          this.logService.error({
            name: 'Error dialog',
            message: JSON.stringify(error),
            stack: 'cohort_service',
          });
          this.dialogService.error('Failed to export cohort.');
          return of(false);
        }),
    );
  }

  deIdCohort(displayName: string, description?: string): Observable<boolean> {
    // Find the dicom store parent.
    const segments =
        environment.IMAGE_DEID_DICOM_STORE_BASE_URL.split('/project/');
    if (segments.length === 1) {
      this.dialogService.error('Configured destination is not supported.');
      return of(false);
    }
    const request: TransferDeIdPathologyCohortRequest = {
      name: this.getSelectedCohortName(),
      displayNameTransferred: displayName,
      destDicomImages: `/project/${segments[1]}`,
    };
    if (description) {
      request.descriptionTransferred = description;
    }

    return this.orchestratorService.transferDeIdPathologyCohort(request).pipe(
        map(() => true),
        catchError((error) => {
          this.logService.error({
            name: 'Error dialog',
            message: JSON.stringify(error),
            stack: 'cohort_service',
          });

          if (error['originalStack'].includes(
                  TRANSFER_DEID_COHORT_ERROR_TIMEOUT_MESSAGE)) {
            return of(true);
          }

          this.dialogService.error(
              'Failed to begin de-identification process.');
          return of(false);
        }),
    );
  }

  copyCohort(displayName: string, description?: string): Observable<boolean> {
    const request: CopyPathologyCohortRequest = {
      name: this.getSelectedCohortName(),
      displayNameCopied: displayName,
    };
    if (description) {
      request.descriptionCopied = description;
    }

    return this.orchestratorService.copyPathologyCohort(request).pipe(
        tap((cohort) => {
          if (cohort?.name) {
            this.loadAllCohorts();
            this.fetchPathologyCohort(cohort.name);
          }
        }),
        map(() => true),
        catchError((error) => {
          this.logService.error({
            name: 'Error dialog',
            message: JSON.stringify(error),
            stack: 'cohort_service',
          });
          this.dialogService.error('Failed to copy cohort.');
          return of(false);
        }),
    );
  }

  deleteSelectedCohort(): Observable<boolean> {
    return this.orchestratorService
        .deletePathologyCohort({name: this.getSelectedCohortName()})
        .pipe(
            map(() => true),
            catchError((error) => {
              this.logService.error({
                name: 'Error dialog',
                message: JSON.stringify(error),
                stack: 'cohort_service'
              });
              this.dialogService.error('Failed to delete cohort.');
              return of(false);
            }),
        );
  }
}

// Client-facing data types
/**
 * Information that the UI requires about a given cohort.
 */
export interface CohortInfo {
  displayName: string;
  isExported: boolean;
  isDeid: boolean;
  isShared: boolean;
  name: string;
  access: PathologyUserAccessRole;
  pathologyCohort: PathologyCohort;
}

/**
 * The key for the /cohorts cohort ID query parameter - e.g. "cohortName" in
 * https://localhost:5432/cohorts?cohortName=pathologyUsers/123/pathologyCohorts/456
 * Parameter value should correspond to PathologyCohort.name.
 */
export const COHORT_ID_PARAM_KEY = 'cohortName';
/**
 * The token that is used when sharing a link with another user. This is only
 * needed for unsaved cohorts.
 */
export const LINK_TOKEN_PARAM_KEY = 'linkToken';

/**
 * Already exists exception.
 */
export class AlreadyExistsError extends Error {}
