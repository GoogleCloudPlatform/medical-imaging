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

// Request fields that are required by API to be a REST call url parameter.

import { CopyPathologyCohortRequest, CreatePathologyCohortRequest,  DeletePathologyCohortRequest, ExportPathologyCohortRequest, ExportPathologyCohortResponse, GetPathologyCohortRequest, ListPathologyCohortsRequest, ListPathologyCohortsResponse, PathologyCohort, PathologyCohortMetadata, SavePathologyCohortRequest, SavePathologyCohortResponse, SharePathologyCohortRequest, TransferDeIdPathologyCohortRequest, TransferDeIdPathologyCohortResponse, UndeletePathologyCohortRequest, UnsavePathologyCohortRequest, UnsavePathologyCohortResponse, UpdatePathologyCohortRequest } from "../interfaces/cohorts";
import { IdentifyCurrentUserRequest, PathologyUser, PathologyUserAccess } from "../interfaces/users";
import { Observable, catchError, defer, map, mergeMap, shareReplay, switchMap } from "rxjs";

import { AuthService } from "./auth.service";
import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { LogService } from "./log.service";
import { PathologySlide } from "../interfaces/slide_descriptor";
import { environment } from "../environments/environment";

const FIELDS_AS_URL_PARAM = ['linkToken', 'filter', 'view'];
/** Error message to retry update stale cohort. */
export const ERROR_MESSAGE_STALE_UPDATE_COHORT =
  'Request contains stale data for cohort';
const DEFAULT_RETRY = 3;

// NOTE: Stored cohort slides paths must start with project/....

// Remove the endpoint name from the slides paths. This is to allow to access those slide from other endpoints.
// Cohorts only support propert GCP DICOM store pathes starting with /projects.
function removeEndpointName(cohort?: PathologyCohort): PathologyCohort|undefined {
  if (!cohort) return cohort;
  if (cohort.slides) {
    cohort.slides = cohort.slides.map((slide: PathologySlide) => {
      if (slide.dicomUri && slide.dicomUri.includes('/projects/')) {
        slide.dicomUri = 'projects/' + (slide.dicomUri.split('/projects/')[1]);
      }
      return slide;
    });
  }
  return cohort;
}

// Add endpoint name to complete the full url.
function addEndpointName(cohort?: PathologyCohort): PathologyCohort|undefined {
  if (!cohort) return cohort;
  if (cohort.slides) {
    cohort.slides = cohort.slides.map((slide: PathologySlide) => {
      if (!slide.dicomUri) return slide;
      if (slide.dicomUri.includes(environment.IMAGE_DICOM_STORE_BASE_URL.split('/projects/')[1])) {
        slide.dicomUri = environment.IMAGE_DICOM_STORE_BASE_URL.split('projects/')[0] + slide.dicomUri;
      } else if (slide.dicomUri.includes(environment.IMAGE_DEID_DICOM_STORE_BASE_URL.split('/projects/')[1])) {
        slide.dicomUri = environment.IMAGE_DEID_DICOM_STORE_BASE_URL.split('projects/')[0] + slide.dicomUri;
      }
      return slide;
    });
  }
  return cohort;
}

/**
 * Communicates cohort and user data with the orchestrator.
 */
@Injectable({
  providedIn: 'root',
})
export class OrchestratorService {
  // Hot observable so one HTTP call can be reused.
  readonly userName$: Observable<string> = defer(()=>this.identifyCurrentUser().pipe(
    map(pathologyUser => pathologyUser.name!), shareReplay(1)));

  constructor(
    private readonly http: HttpClient,
    private readonly authService: AuthService,
    private readonly logService: LogService,
  ) { }

  private httpRequest(method: string, relativePath: string, body?: string):
    Observable<string> {

    const path = `${environment.ORCHESTRATOR_BASE_URL}${relativePath}`;
    return this.authService.getOAuthToken().pipe(switchMap((accessToken) => {
      if (method === 'GET') {
        body = undefined;
      }
      return this.http
        .request(method, path, {
          headers: {
            'Authorization': 'Bearer ' + accessToken,
            'content-type': 'application/json',
          },
          responseType: 'text',
          body,
        })
        .pipe(catchError(val => {
          // http.request appears to throw a Response object.
          // Stringify it so the error message is more than "[Object]".
          // See: https://developer.mozilla.org/en-US/docs/Web/API/Response
          val = JSON.stringify(val);
          this.logService.error(
            { name: `httpRequest: "${path}"`, message: val });
          throw new Error(`Error while fetching ${method} ${path}: ${val}`);
        }));
    }));
  }

  fetch<RESPONSE>(method: string, relativePath: string, request: unknown):
    Observable<RESPONSE> {
    const jsonRequest = JSON.stringify(request);
    if (request) {
      // Select fields FIELDS_AS_URL_PARAM in the request need to be passed as
      // url parameters.
      const requestTyped = request as Record<string, string>;
      const params: Record<string, string> = {};
      for (const token of FIELDS_AS_URL_PARAM) {
        if (token in requestTyped) params[token] = requestTyped[token];
      }
      if (Object.keys(params).length > 0) {
        // Request as URL params.
        relativePath =
          relativePath + '?' + (new URLSearchParams(params).toString());
      }
    }
    return this.httpRequest(method, relativePath, jsonRequest)
      .pipe(map(responseJson => {
        try {
          return JSON.parse(responseJson) as RESPONSE;
        } catch (error: unknown) {
          throw new Error(
            `Error parsing "${responseJson}" as ${relativePath}: ${error}`);
        }
      }));
  }

  // request example:
  //    pathologyCohort: {
  //      cohortMetadata: {
  //        displayName: 'test123',
  //        description: 'description123',
  //      }
  //    }
  createPathologyCohort(request: CreatePathologyCohortRequest):
    Observable<PathologyCohort> {
      request.pathologyCohort = removeEndpointName(request.pathologyCohort);
    return this.userName$.pipe(mergeMap(
      userName => this.fetch<PathologyCohort>(
        'POST', `/${userName}/pathologyCohorts`, request)));
  }
  // request example: {name: "testname"}
  deletePathologyCohort(request: DeletePathologyCohortRequest):
    Observable<PathologyCohort> {
    return this.fetch('DELETE', `/${request.name}`, request);
  }
  // request example: {name: "testname"}
  getPathologyCohort(request: GetPathologyCohortRequest):
    Observable<PathologyCohort> {
    return this.fetch<PathologyCohort>('GET', `/${request.name!}`, request).pipe(
      map(cohort => addEndpointName(cohort)!)
    );
  }
  listPathologyCohorts(request?: ListPathologyCohortsRequest):
    Observable<ListPathologyCohortsResponse> {
    return this.userName$.pipe(switchMap(
      userName => this.fetch<ListPathologyCohortsResponse>(
        'GET', `/${userName}/pathologyCohorts`, request))).pipe(
          map(response => {
            if (response.pathologyCohorts) {
              response.pathologyCohorts = response.pathologyCohorts!.map(cohort => addEndpointName(cohort)!);
            }
            return response;
          }));
  }
  // request.updateMask is a comma separated list of:
  // "displayName,slides,description"
  updatePathologyCohort(
    request: UpdatePathologyCohortRequest,
    cohort: PathologyCohort,
    retriesLeft: number = DEFAULT_RETRY,
  ): Observable<PathologyCohort> {
    request.pathologyCohort = removeEndpointName(request.pathologyCohort);
    return this
      .fetch<PathologyCohort>(
        'PATCH', `/${request.pathologyCohort!.name!}`, request)
      .pipe(
        catchError((error) => {
          const errorMessage: string = error['originalStack'] ?? '';
          if (retriesLeft &&
            errorMessage.includes(ERROR_MESSAGE_STALE_UPDATE_COHORT)) {
            retriesLeft--;

            return this.updatePathologyCohortRetryHandler(
              request, cohort, retriesLeft);
          }

          throw error;
        }),
      );
  }

  private updatePathologyCohortRetryHandler(
    request: UpdatePathologyCohortRequest,
    cohort: PathologyCohort,
    retriesLeft: number,
  ): Observable<PathologyCohort> {
    const getCohortRequest: GetPathologyCohortRequest = {
      name: request.pathologyCohort!.name,
      view: 'PATHOLOGY_COHORT_VIEW_FULL',
    };

    return this.getPathologyCohort(getCohortRequest)
      .pipe(
        map((latestCohort: PathologyCohort) => {
          const modifiedLatestCohort = this.consolidateStaleData(
            request,
            cohort,
            latestCohort,
          );

          return { newOriginalCohort: latestCohort, modifiedLatestCohort };
        }),
        switchMap((latestCohortsData: {
          newOriginalCohort: PathologyCohort;
          modifiedLatestCohort: PathologyCohort;
        }) => {
          request.pathologyCohort = latestCohortsData.modifiedLatestCohort;
          return this.updatePathologyCohort(
            request, latestCohortsData.newOriginalCohort, retriesLeft);
        }),
      );
  }

  private consolidateStaleData(
    request: UpdatePathologyCohortRequest,
    cohort: PathologyCohort,
    latestCohort: PathologyCohort,
  ): PathologyCohort {
    latestCohort = JSON.parse(JSON.stringify(latestCohort)) as PathologyCohort;
    if (!request.pathologyCohort || !latestCohort.cohortMetadata) {
      return latestCohort;
    }

    switch (request.updateMask) {
      case 'displayName':
        latestCohort.cohortMetadata.displayName =
          request.pathologyCohort?.cohortMetadata?.displayName;
        break;
      case 'description':
        latestCohort.cohortMetadata.description =
          request.pathologyCohort?.cohortMetadata?.description;
        break;
      case 'cohortMetadata':
        latestCohort.cohortMetadata = {
          ...request.pathologyCohort.cohortMetadata,
          ...{ updateTime: latestCohort?.cohortMetadata?.updateTime } as
          PathologyCohortMetadata,
        };
        break;
      case 'slides':
        let addedSlides: PathologySlide[] = [];
        let removedSlides: PathologySlide[] = [];
        const originalSlidesDicomUri = new Set<string>(
          (cohort?.slides ?? []).map((slide) => slide.dicomUri!));
        const updatedSlidesDicomUri =
          new Set<string>((request.pathologyCohort.slides ??
            []).map((slide) => slide.dicomUri!));

        removedSlides = (cohort?.slides ?? []).filter((slide) => {
          return slide.dicomUri ? !updatedSlidesDicomUri.has(slide.dicomUri) :
            false;
        });
        addedSlides = (request.pathologyCohort.slides ?? []).filter((slide) => {
          return slide.dicomUri ? !originalSlidesDicomUri.has(slide.dicomUri) :
            false;
        });

        if (addedSlides.length) {
          latestCohort.slides =
            [...(latestCohort.slides ?? []), ...addedSlides];
        }
        if (removedSlides.length) {
          const removedSlidesDicomUri =
            new Set(removedSlides.map((slide) => slide.dicomUri!));
          latestCohort.slides = (latestCohort.slides ?? []).filter((slide) => {
            return slide.dicomUri ? !removedSlidesDicomUri.has(slide.dicomUri) :
              false;
          });
        }
        break;
      case 'userAccess':
        const updatedUsersByEmail =
          new Map((request.pathologyCohort.userAccess ?? []).map((user) => {
            return [user.userEmail, user];
          }));
        const originalUsersByEmail =
          new Map((cohort?.userAccess ?? []).map((user) => {
            return [user.userEmail, user];
          }));

        let addedUsersByEmail = new Map<string, PathologyUserAccess>();
        let removedUsersByEmail = new Map<string, PathologyUserAccess>();
        addedUsersByEmail =
          new Map((request.pathologyCohort.userAccess ?? [])
            .filter((user) => {
              const originalUser =
                originalUsersByEmail.get(user.userEmail);
              if (!originalUser) return true;

              return !(
                originalUser.userEmail === user.userEmail &&
                originalUser.accessRole === user.accessRole);
            })
            .map((user) => {
              return [user.userEmail ?? '', user];
            }));

        removedUsersByEmail =
          new Map((cohort?.userAccess ?? [])
            .filter((user) => {
              const updatedUser =
                updatedUsersByEmail.get(user.userEmail);
              if (!updatedUser) return true;

              return !(
                updatedUser.userEmail === user.userEmail &&
                updatedUser.accessRole === user.accessRole);
            })
            .map((user) => {
              return [user.userEmail ?? '', user];
            }));

        if (addedUsersByEmail.size) {
          if (latestCohort.userAccess) {
            for (let index = 0; index < (latestCohort.userAccess ?? []).length;
              index++) {
              const user = latestCohort.userAccess[index];
              if (user.userEmail && addedUsersByEmail.has(user.userEmail)) {
                latestCohort.userAccess[index] =
                  addedUsersByEmail.get(user.userEmail)!;
                addedUsersByEmail.delete(user.userEmail);
              }
            }
          }
          latestCohort.userAccess = [
            ...(latestCohort.userAccess ?? []),
            ...addedUsersByEmail.values(),
          ];
        }
        if (removedUsersByEmail.size) {
          latestCohort.userAccess = (latestCohort.userAccess ??
            []).filter((user) => {
              return user.userEmail ? removedUsersByEmail.has(user.userEmail) :
                false;
            });
        }
        break;
      default:
    }

    return latestCohort;
  }

  sharePathologyCohort(request: SharePathologyCohortRequest):
    Observable<PathologyCohort> {
    return this.fetch('POST', `/${request.name!}:share`, request);
  }
  savePathologyCohort(request: SavePathologyCohortRequest):
    Observable<SavePathologyCohortResponse> {
    return this.fetch('POST', `/${request.name!}:save`, request);
  }
  unsavePathologyCohort(request: UnsavePathologyCohortRequest):
    Observable<UnsavePathologyCohortResponse> {
    return this.fetch('POST', `/${request.name!}:unsave`, request);
  }
  // request example: {name: "testname"}
  undeletePathologyCohort(request: UndeletePathologyCohortRequest):
    Observable<PathologyCohort> {
    return this.fetch('POST', `/${request.name!}:undelete`, request);
  }
  transferDeIdPathologyCohort(request:
    TransferDeIdPathologyCohortRequest):
    Observable<TransferDeIdPathologyCohortResponse> {
    return this.fetch('POST', `/${request.name!}:transfer`, request);
  }
  copyPathologyCohort(request: CopyPathologyCohortRequest):
    Observable<PathologyCohort> {
    return this.fetch('POST', `/${request.name!}:copy`, request);
  }
  exportPathologyCohort(request: ExportPathologyCohortRequest
  ):
    Observable<ExportPathologyCohortResponse> {
    return this.fetch('POST', `/${request.name!}:export`, request);
  }
  identifyCurrentUser(request: IdentifyCurrentUserRequest = {}):
    Observable<PathologyUser> {
    return this.fetch('POST', ':identifyCurrentUser', request);
  }
}