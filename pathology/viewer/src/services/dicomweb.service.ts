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

import { Case, Patient, RECORD_ID_TYPE_META, RecordIdType, Slide } from '../interfaces/hierarchy_descriptor';
import { DicomModality, DicomModel, DicomTag, PersonName, formatDate, formatName, isPatients } from '../interfaces/dicom_descriptor';
import { Observable, defer, merge, of, throwError } from 'rxjs';
import { catchError, filter, first, map, mergeAll, switchMap, toArray } from 'rxjs/operators';

import { AuthService } from './auth.service';
import { HttpClient } from '@angular/common/http';
import { ImageTile } from '../interfaces/image_tile';
import { Injectable } from '@angular/core';
import { LogService } from './log.service';
import { PathologySlide } from '../interfaces/slide_descriptor';
import { SlideInfo } from '../interfaces/slide_descriptor';
import { encodeByteArray } from '../utils/crypt';
import { environment } from '../environments/environment';
import { DicomWebUrlConfig, constructDicomWebUrl, parseDICOMwebUrl } from '../interfaces/dicomweb';
import {CompressionType} from '../interfaces/types';

const DICOM_URI_VALIDATOR = new RegExp('^.*studies/[0-9.]*');

/**
 * Icc profile settings.
 */
export enum IccProfileType {
  NONE = '',  // if none option needed, assuming yes for default
  NO = 'no',
  YES = 'yes',
  ADOBERGB = 'adobergb',  // Adobe RGB
  ROMMRGB = 'rommrgb',
  SRGB = 'srgb',
}

/**
 * Icc profile setting to readable labels
 */
export const iccProfileTypeToLabel = new Map<IccProfileType, string>([
  [IccProfileType.NONE, 'None'],
  [IccProfileType.NO, 'No'],
  [IccProfileType.YES, 'Yes'],
  [IccProfileType.ADOBERGB, 'Adobe RGB'],
  [IccProfileType.ROMMRGB, 'Reference Output Medium Metric (ROMM RGB)'],
  [IccProfileType.SRGB, 'Standard RGB'],
]);

const UNKNOWN_CASE_ID = 'Unknown Case ID';
const UNKNOWN_DEID_CASE_ID = 'Unknown Case ID';
/**
 * Retrieves Dicom metadata and images from the Healthcare API.
 */
@Injectable({
  providedIn: 'root',
})
export class DicomwebService {
  // Slide label is assumed to be continuous alphanumber segments separated by a
  // non-alpha numeric symbol, for example: TS-01-1-A1-3
  // The following regex captures all segments including any prefix separator.
  private readonly slideLabelTextSegment = new RegExp('\\W?\\w+', 'g');

  constructor(
    private readonly http: HttpClient,
    private readonly authService: AuthService,
    private readonly logService: LogService,
  ) { }

  getImageTile(
    path: string, slideInfo: SlideInfo, tile: ImageTile,
    iccProfile = IccProfileType.NONE): Observable<string> {
    const zoomLevel = slideInfo.levelMap[tile.scale];
    const { tileSize } = zoomLevel;

    const tilesPerWidth = Math.ceil(Number(zoomLevel.width) / tileSize);

    // Flooring because in cases that the whole slide fits the tile, the tile
    // size can be smaller than a normal tile. Fraction should only happen in
    // such a scenario. For example: Normal tile size is 512. corner.x is 512.
    // tileSize is 363. We want locationWidth to be 1.
    const locationWidth = Math.floor(tile.corner.x / tileSize);
    const locationHeight = Math.floor(tile.corner.y / tileSize);

    const frameNum =
      locationHeight * tilesPerWidth + locationWidth % tilesPerWidth;

    let offset = 0;
    let frame;
    let instUID = '';
    let downSampleMultiplier = 0;
    // Frames may be stored across multiple DICOMs. Find the right ones.
    for (let i = 0; i < zoomLevel.properties.length; i++) {
      if (frameNum >= offset + zoomLevel.properties[i].frames) {
        offset = offset + zoomLevel.properties[i].frames;
        continue;
      }
      frame = frameNum - offset + 1;
      instUID = zoomLevel.properties[i].instanceUid;
      downSampleMultiplier = zoomLevel.downSampleMultiplier ?? 0;
      break;
    }

    if (typeof frame === 'undefined' || !instUID) {
      // The tile is missing!
      return of('');
    }

    if (slideInfo.isFlatImage) {
      return this.getFlatImage(path, instUID);
    }

    const encoded = this.getEncodedImageTile(
      path, instUID, frame, downSampleMultiplier,
      iccProfile);

    return encoded;
  }

  // disableServerCaching - is only valid when using the tile proxy.
  getEncodedImageTile(
    path: string, instUID: string, frame: number,
    downSampleMultiplier = 0, iccProfile = IccProfileType.NONE,
    disableServerCaching = false) {
    const url = this.generateEncodedImageTileUrl(
      path, instUID, frame, downSampleMultiplier, iccProfile,
      disableServerCaching);

    return this.httpGetStringEncodedImage(url);
  }

  generateEncodedImageTileUrl(
    path: string, instUID: string, frame: number,
    downSampleMultiplier = 0, iccProfile = IccProfileType.NONE,
    disableServerCaching = false): string {
    let url = `${path}/instances/${instUID}/frames/${frame}/rendered`;
    let isFirst = true;
    if (downSampleMultiplier) {
      url += `?downsample=${downSampleMultiplier}`;
      isFirst = false;
    }
    if (iccProfile) {
      url += isFirst ? '?' : '&';
      url += `iccprofile=${iccProfile}`;
      isFirst = false;
    }
    if (disableServerCaching) {
      url += isFirst ? '?' : '&';
      url += 'disable_caching=' + disableServerCaching.toString();
    }
    return url;
  }

  // Fetch multiple tiles at once. Implementation is not complete since it does
  // not parse the result. Currently use to measure fetch speed.
  getEncodedImageTiles(
    path: string, instUID: string, frames: number[], dicomStore: string,
    downSampleMultiplier = 0, iccProfile = IccProfileType.NONE,
    disableServerCaching = false) {
    const frameCSV = frames.join(',');
    let url =
      `${dicomStore}${path}/instances/${instUID}/frames/${frameCSV}`;
    let isFirst = true;
    if (downSampleMultiplier) {
      url += `?downsample=${downSampleMultiplier}`;
      isFirst = false;
    }
    if (iccProfile) {
      url += isFirst ? '?' : '&';
      url += `iccprofile=${iccProfile}`;
      isFirst = false;
    }
    if (disableServerCaching) {
      url += isFirst ? '?' : '&';
      url += 'disable_caching=' + disableServerCaching.toString();
    }
    return this.httpGetStringEncodedImage(url);
  }

  private getFlatImage(
    path: string, instanceUID: string): Observable<string> {
    const url =
      `${path}/instances/${instanceUID}/rendered`;

    return this.httpGetStringEncodedImage(url);
  }

  private httpGetStringEncodedImage(
    url: string, compressionType: CompressionType = CompressionType.DEFAULT): Observable<string> {
    return this.authService.getOAuthToken().pipe(switchMap((accessToken) => {
      return this.http
        .get(url, {
          headers: {
            'Accept': compressionType + ',multipart/related',
            'Authorization': 'Bearer ' + (accessToken ?? ''),
          },
          responseType: 'arraybuffer',
          withCredentials: false
        })
        .pipe(
          map(buffer => {
            return String(encodeByteArray(new Uint8Array(buffer)));
          }),
          catchError(val => {
            this.logService.error(
              { name: `httpGetImg: "${url}"`, message: val });
            return throwError('Network error');
          }));
    }));
  }


  private httpGetText(url: string):
    Observable<string> {
    return this.authService.getOAuthToken().pipe(switchMap((accessToken) => {
      return this.http
        .get(url, {
          headers: {
            'Authorization': 'Bearer ' + accessToken,
          },
          responseType: 'text',
        })
        .pipe(catchError(val => {
          this.logService.error({
            name: `httpGetText: "${url}"`,
            message: val
          });
          return throwError(() => new Error('Network error'));
        }));
    }));
  }

  // Get metadata of instances (actual Dicom files). In pathology this
  // refers to tissue zoom layers and other associated images.
  // limit if set limits the number of results.
  getInstancesMetadata(seriesPath: string, tags: string[], limit?: number):
    Observable<DicomModel[]> {
    const searchParams = new URLSearchParams();
    tags.length && searchParams.append('includefield', tags.join(','));
    limit && searchParams.append('limit', limit.toString());
    const params = searchParams.toString();
    const url = `${seriesPath}/instances${params ? '?' : ''}${params}`;
    return this.httpGetText(url)
      .pipe(map(response => JSON.parse(response) as DicomModel[]));
  }
  
  /**
   * Get extra metadata for a slide.
   * go/dicomspec/part18.html#sect_10.4.1.1.2
   */
  getExtraMetadata(path: string): Observable<DicomModel[]> {
    return this.httpGetText(`${path}/metadata`)
      .pipe(first(), map(response => JSON.parse(response) as DicomModel[]));
  }

  getImageSecondaryCapture(
    path: string, instanceUid: string,
    compressionType: CompressionType = CompressionType.DEFAULT,
    iccProfile = IccProfileType.NONE): Observable<string> {
    let url = this.generateImageSecondaryCaptureUrl(
      path, instanceUid, compressionType);
    if (iccProfile) {
      url += `?iccprofile=${iccProfile}`;
    }

    return this.httpGetStringEncodedImage(url, compressionType);
  }

  generateImageSecondaryCaptureUrl(
    path: string, instanceUid: string,
    compressionType: CompressionType = CompressionType.DEFAULT): string {
    const url =
      `${path}/instances/${instanceUid}/rendered`;
    return url;
  }

  /**
   * Search for a slide label can not be done directly because the current DICOM
   * store GCP implementation does not support to search on the container name
   * tag.
   * https://docs.google.com/document/d/1rXqN6I39-150Hj67s6SBD2jMiJsuYS5sTmXnyAil0wQ
   *
   * There we need to search for the case first which is typically a prefix of
   * the slide id. We don't know how many segements belong to the case. Here we
   * heuristacally assume the case id is shorter by 2 or 3 segements than the
   * slide id.
   *
   * For example: The slide id GO-1644426098813-2-A-0 has 5 segements. Its case
   * id is GO-1644426098813 which has only 2 segments.
   *
   * If no match is found, function returns an empty array. []
   */
  private searchSlideLabel(searchText: string) {
    const caseIdSegments = searchText.match(this.slideLabelTextSegment);
    if (!caseIdSegments) {
      throw new Error(`Slide ID "${searchText}" does not confirm to provide case id template`);
    }
    // Search for case id with 1,2 or 3 segments less than the provided slide
    // id.
    const searches = [1, 2, 3]
      .map(n => caseIdSegments.length - n)
      .filter(n => n > 0)
      .map(
        n => this.searchSeriesById(
          caseIdSegments.slice(0, n).join(''), 'caseId'),
        DicomModality.SLIDE_MICROSCOPY);
    const CONCURRENCY = 5;
    return merge(...searches)
      .pipe(
        // Array of dicom models to sequence of dicom model emits.
        switchMap(dicomModel => dicomModel || []),
        // Limit concurrency using defer and mergeAll(concurrency). A
        // cohort could contains hundreds of cases. If we won't limit the
        // concurrency the frontend will make hundrends of requests in the
        // same time increasing the chance that the backend will be
        // overwhelmed and timeout for some of the request. See example
        // here:
        // https://stackblitz.com/edit/typescript-xscm2p?file=index.ts&devtoolsheight=100
        map(dicomModel => defer(
          () => this.getInstancesMetadata(
            studyDicomModelToSeriesPath(
              environment.IMAGE_DICOM_STORE_BASE_URL as string, dicomModel),
            [
              DicomTag.CONTAINER_IDENTIFIER,
              DicomTag.STUDY_INSTANCE_UID,
              DicomTag.SERIES_INSTANCE_UID
            ],
            1 /* = limit*/))),
        mergeAll(CONCURRENCY),
        // Dereference array since we limited to 1 results.
        map(dicomModel => dicomModel[0]),
        // Filter out slides that don't match the searched slide label.
        filter(dicomModel => {
          const instanceValue =
            dicomModel[DicomTag.CONTAINER_IDENTIFIER]?.Value;
          if (instanceValue?.constructor === Array &&
            instanceValue[0].constructor === String) {
            return instanceValue[0] === searchText;
          }
          return false;
        }),
        toArray(),
      );
  }

  // Search is only supported in the IMAGE dicomstore over PHI.
  searchSeriesById(
    searchText: string, searchType: RecordIdType,
    modality: DicomModality = DicomModality.SLIDE_MICROSCOPY):
    Observable<DicomModel[]> {
    const dicomWebUrlConfig :  DicomWebUrlConfig = {
      baseUrl: environment.IMAGE_DICOM_STORE_BASE_URL,
      resource: 'series',
      queryParams: {
        modality: modality.toString()
      },
      path: {}
    };

    let recordIdType = undefined;
    if (searchText === '*') {
      searchText = '';
      // Limit to avoid long latency on large dicomstores.
      dicomWebUrlConfig.queryParams!.limit = 20;
    }
    switch (searchType) {
      case 'caseId':
        recordIdType = RECORD_ID_TYPE_META.caseId;
        break;
      case 'slideId':
        recordIdType = RECORD_ID_TYPE_META.slideId;
        break;
      case 'patientId':
        recordIdType = RECORD_ID_TYPE_META.patientId;
        break;
      default:
        throw new Error(`Invalid search type ${searchType}`);
    }
    if (recordIdType.dicomWebSearchToken) {
      dicomWebUrlConfig.queryParams![recordIdType.dicomWebSearchToken!]=searchText;
    }

    if (searchType === 'slideId') {
      return this.searchSlideLabel(searchText);
    }
    if (recordIdType.dicomWebSearchToken === undefined) {
      throw new Error(`dicomWebSearchToken not defined for ${searchType}`);
    }

    return this.httpGetText(constructDicomWebUrl(dicomWebUrlConfig))
        .pipe(
            first(),
            map((response: string) =>
                    (response && JSON.parse(response) as DicomModel[]) || []));
  }

  // From case name (series path) get case id and patient information, without
  // fetching the list of slides.
  getStudyMeta(pathToStudy: string) {
    const dicomWebUrl = parseDICOMwebUrl(pathToStudy);
    dicomWebUrl.resource = 'series';
    dicomWebUrl.queryParams = {
      includefield: [
        DicomTag.ACCESSION_NUMBER,
        DicomTag.STUDY_DATE,
      ].join(','),
      limit: 1,
    };

    return this
      .httpGetText(
        constructDicomWebUrl(dicomWebUrl))
      .pipe(first(), map(response => JSON.parse(response) as DicomModel[]));
  }

  getInstancesByStudy(
    pathToStudy: string, dicomModality = DicomModality.SLIDE_MICROSCOPY):
    Observable<DicomModel[]> {
    return this
      .httpGetText(
        `${pathToStudy}/instances?modality=${dicomModality}&includefield=${DicomTag.CONTAINER_IDENTIFIER}`)
      .pipe(first(), map(response => JSON.parse(response) as DicomModel[]));
  }
}

function studyDicomModelToSeriesPath(
  dicomStoreParent: string, dicomModel: DicomModel) {
  const studyInstanceValue = dicomModel[DicomTag.STUDY_INSTANCE_UID]?.Value;
  let studyInstanceUID = '';
  if (studyInstanceValue?.constructor === Array &&
    studyInstanceValue[0].constructor === String) {
    studyInstanceUID = studyInstanceValue[0];
  }
  const seriesInstanceValue =
    dicomModel[DicomTag.SERIES_INSTANCE_UID]?.Value;
  let seriesUID = '';
  if (seriesInstanceValue?.constructor === Array &&
    seriesInstanceValue[0].constructor === String) {
    seriesUID = seriesInstanceValue[0];
  }

  return `${dicomStoreParent}/studies/${studyInstanceUID}/series/${seriesUID}`;
}

/**
 * This function takes a list of pathology slides and returns a map of case IDs
 * to lists of slides.
 *
 * @param slides The list of pathology slides.
 *
 * @return The map of case IDs to lists of slides.
 */
export function computeDicomUrisByCaseId(slides: PathologySlide[]):
  Map<string, PathologySlide[]> {
  const cases = slides.reduce((map, slide) => {
    if (slide.dicomUri) {
      const match = slide.dicomUri.match(DICOM_URI_VALIDATOR);
      if (match && match[0]) {
        const key = match[0];
        map.set(key, [...(map.get(key) ?? []), slide]);
      } else {
        const errorMessage = `Malformated dicom uid: ${slide.dicomUri}`;
        throw new Error(errorMessage);
      }
    }
    return map;
  }, new Map<string, PathologySlide[]>());

  return cases;
}

/**
 * Populate provided patient using data of the provided series DicomModel.
 */
export function updatePatient(dicomModel: DicomModel, patient: Patient) {
  const patientNameInstanceValue = dicomModel[DicomTag.PATIENT_NAME]?.Value;
  const personName: PersonName | undefined =
    patientNameInstanceValue?.constructor !== Array &&
      isPatients(patientNameInstanceValue) ?
      patientNameInstanceValue[0] :
      undefined;

  patient.name ??= formatName(personName);

  const patientIdInstanceValue = dicomModel[DicomTag.PATIENT_ID]?.Value;
  let patientId = '';
  if (patientIdInstanceValue?.constructor === Array &&
    patientIdInstanceValue[0].constructor === String) {
    patientId = patientIdInstanceValue[0];
  }
  patient.patientId ??= patientId;

  const accessionNumberInstanceValue =
    dicomModel[DicomTag.ACCESSION_NUMBER]?.Value;
  let accessionNumber = 'Unknown Case ID';
  if (accessionNumberInstanceValue?.constructor === Array &&
    accessionNumberInstanceValue[0].constructor === String) {
    accessionNumber = accessionNumberInstanceValue[0] ?? 'Unknown Case ID';
  }

  const studyDateInstanceValue = dicomModel[DicomTag.STUDY_DATE]?.Value;
  let studyDate = '';
  if (studyDateInstanceValue?.constructor === Array &&
    studyDateInstanceValue[0].constructor === String) {
    studyDate = studyDateInstanceValue[0];
  }

  // Don't replace an undefined date with a default until after we're done
  // using them to find the most recent case.
  if (!patient.latestCaseDate || studyDate > patient.latestCaseDate) {
    patient.latestCaseDate = studyDate;
    patient.latestCaseAccessionNumber = accessionNumber;
  }
}

/**
 * Populate provided map from case UID to Case object using data of the provided
 * series DicomModel.
 */
export function createOrUpdateCase(
  dicomStoreParent: string, dicomModel: DicomModel,
  cases: Map<string, Case>) {
  const studyInstanceUIDInstanceValue =
    dicomModel[DicomTag.STUDY_INSTANCE_UID]?.Value;
  let studyInstanceUID = '';
  if (studyInstanceUIDInstanceValue?.constructor === Array &&
    studyInstanceUIDInstanceValue[0].constructor === String) {
    studyInstanceUID = studyInstanceUIDInstanceValue[0];
  }

  const seriesUIDInstanceValue =
    dicomModel[DicomTag.SERIES_INSTANCE_UID]?.Value;
  let seriesUID = '';
  if (seriesUIDInstanceValue?.constructor === Array &&
    seriesUIDInstanceValue[0].constructor === String) {
    seriesUID = seriesUIDInstanceValue[0];
  }

  if (!studyInstanceUID || !seriesUID) {
    return;
  }
  let slideCase: Case = cases.get(studyInstanceUID) as Case;
  if (!slideCase) {
    slideCase = studyDicomModelToCase(
      `${dicomStoreParent}/studies/${studyInstanceUID}`,
      dicomModel,
      false /* isDeId */,
    );
  }
  slideCase.slides = [
    ...(slideCase.slides ?? []),
    dicomModelToPathologySlide(dicomModel),
  ];
  cases.set(studyInstanceUID, slideCase);
}

function dicomModelToPathologySlide(dicomModel: DicomModel): PathologySlide {
  const studyInstanceUIDInstanceValue =
    dicomModel[DicomTag.STUDY_INSTANCE_UID]?.Value;
  let studyInstanceUID = '';
  if (studyInstanceUIDInstanceValue?.constructor === Array &&
    studyInstanceUIDInstanceValue[0].constructor === String) {
    studyInstanceUID = studyInstanceUIDInstanceValue[0];
  }

  const seriesUIDInstanceValue =
    dicomModel[DicomTag.SERIES_INSTANCE_UID]?.Value;
  let seriesUID = '';
  if (seriesUIDInstanceValue?.constructor === Array &&
    seriesUIDInstanceValue[0].constructor === String) {
    seriesUID = seriesUIDInstanceValue[0];
  }

  const scanUniqueId: string =
    studyInstanceUID.slice(studyInstanceUID.lastIndexOf('.') + 1);

  return {
    scanUniqueId,
    name: `pathologySlides/${scanUniqueId}`,
    dicomUri: `${environment.IMAGE_DICOM_STORE_BASE_URL}/studies/${studyInstanceUID}/series/${seriesUID}`,

  };
}

/**
 * Create a Case object from a DicomModel. This will not count number of slides.
 */
export function studyDicomModelToCase(
  studyUid: string,
  dicomModel: DicomModel,
  isDeId = false,
): Case {
  const accessionNumberInstanceValue =
    dicomModel[DicomTag.ACCESSION_NUMBER]?.Value;
  let accessionNumber = UNKNOWN_CASE_ID;
  if (accessionNumberInstanceValue?.constructor === Array &&
    accessionNumberInstanceValue[0].constructor === String) {
    accessionNumber = accessionNumberInstanceValue[0] ?? UNKNOWN_CASE_ID;
  }

  if (isDeId) {
    accessionNumber = UNKNOWN_DEID_CASE_ID;
  }

  const studyDateInstanceValue = dicomModel[DicomTag.STUDY_DATE]?.Value;
  let studyDate = '';
  if (studyDateInstanceValue?.constructor === Array &&
    studyDateInstanceValue[0].constructor === String) {
    studyDate = studyDateInstanceValue[0];
  }

  return {
    accessionNumber,
    date: formatDate(studyDate),
    caseId: studyUid,
    slides: [],
  } as Case;
}

/**
 * Create a Slide object from a DicomModel.
 */
export function studyDicomModelToSlide(
  dicomStoreParent: string, dicomModel: DicomModel): Slide {
  const slideRecordIdInstanceValue =
    dicomModel[DicomTag.CONTAINER_IDENTIFIER]?.Value;
  let slideRecordId = '';
  if (slideRecordIdInstanceValue?.constructor === Array &&
    slideRecordIdInstanceValue[0].constructor === String) {
    slideRecordId = slideRecordIdInstanceValue[0];
  }

  return {
    slideId: studyDicomModelToSeriesPath(dicomStoreParent, dicomModel),
    ...slideRecordId &&
    {
      slideRecordId
    }
  };
}
