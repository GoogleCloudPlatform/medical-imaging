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

import { HttpClient } from '@angular/common/http';
import { Injectable, OnDestroy, Optional } from '@angular/core';
import { Attribute, AttributeValue, dicomAttr, DicomModality, DicomModel, DicomTag, getSequenceValue, getValue, SopClassUid, TransferSyntaxUID } from '../interfaces/dicom_descriptor';
import { AnnotationGroup, AnnotationPropertyCategoryCode, DicomAnnotation, DicomAnnotationInstance, ReferencedImage, ReferencedSeries } from '../interfaces/annotation_descriptor';
import { constructDicomWebPath, constructDicomWebUrl, DicomWebPath, DicomWebUrlConfig, parseDICOMwebUrl } from '../interfaces/dicomweb';
import { Level, PixelSize, SlideInfo } from '../interfaces/slide_descriptor';
import { LogService } from './log.service';
import { UserService } from './user.service';
import { UuidGenerator } from './uuid-generator.service';
import { inlineBinaryToNumberArray, numberArrayToInlineBinary, vrToType } from '../utils/inline-binary';
import { BehaviorSubject, EMPTY, forkJoin, Observable, of, ReplaySubject } from 'rxjs';
import { catchError, finalize, map, mergeMap, switchMap, takeUntil, tap, toArray, withLatestFrom } from 'rxjs/operators';
import { AuthService } from './auth.service';
import { environment } from '../environments/environment';
/**
 * Error returned by DICOM Proxy if annotation uid triple already exists.
 */
export const UID_TRIPLE_ALREADY_EXISTS_ERROR =
  'Cannot upload DICOM Annotation. Uid triple';
const DEFAULT_RETRY = 3;
const MM_TO_METERS = 1e-3; /* mm to meters */
const FLAT_IMAGE_METER_TO_PIXEL_RATIO = 1;

function pixelSizeInMeters(pixelSize: number | undefined): number {
  return (pixelSize ?? 0) * MM_TO_METERS || FLAT_IMAGE_METER_TO_PIXEL_RATIO;
}

/**
 * Convert screen coordinates to pixel coordinates.
 */
export function metersToPixelsWithYFlip(
  coordinates: number[][], pixelSize: PixelSize): number[] {
  const px = pixelSizeInMeters(pixelSize.width);
  const py = pixelSizeInMeters(pixelSize.height);
  const meterCoordinates = coordinates.map(([x, y]) => {
    return [x / px, -(y / py)];
  });
  return meterCoordinates.flat();
}

function findSlideLevel(instanceUID: string, slideInfo: SlideInfo): Level |
  undefined {
  return slideInfo.levelMap.find(
    level => level.properties.some(prop => prop.instanceUid === instanceUID));
}

// Shoelace Theorem: This function determines if a polygon, defined by a list of
// [x, y] coordinates, is oriented clockwise by summing edge cross products.
function isPolygonClockwise(coords: number[]): boolean {
  let sum = 0;
  const n = coords.length;
  for (let i = 0; i < n; i += 2) {
    const j = (i + 2) % n;  // Wrap index
    sum += (coords[j] - coords[i]) *
      (coords[i + 1] + (j + 1 < n ? coords[j + 1] : coords[(j + 1) % n]));
  }
  return sum > 0;
}

// This function flips the order of the points in a polygon.
function flipPointOrder(points: number[]) {
  // Handle empty input
  if (points.length === 0) {
    return [];
  }
  const reversedPoints: number[] = [];
  for (let i = points.length - 2; i >= 0; i -= 2) {
    reversedPoints.push(points[i], points[i + 1]);
  }
  return reversedPoints;
}

function arrayBufferToBase64(buffer: ArrayBuffer): string {
  let binary = '';
  const bytes = new Uint8Array(buffer);
  const len = bytes.byteLength;
  for (let i = 0; i < len; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);  // Encode the binary string to Base64
}

function getReferencedSeries(
  dicomModel: DicomModel, slideInfo: SlideInfo): ReferencedSeries {
  // Access the referenced image, copy it if it exists otherwise use defaults
  // from largest magnificaiton image layer.
  const refSeriesModel =
    getSequenceValue(dicomModel, DicomTag.REFERENCED_SERIES_SEQUENCE)[0];
  const referencedSopClassUid = slideInfo.sopClassUid;
  const referencedSopInstanceUid =
    getValue(
      refSeriesModel, DicomTag.REFERENCED_SERIES_SEQUENCE,
      DicomTag.REFERENCED_SOP_INSTANCE_UID) ||
    slideInfo.levelMap[0]
      .properties[0]
      .instanceUid;  // Use the highest magnification level as
  // the default instance reference.

  const level = findSlideLevel(referencedSopInstanceUid, slideInfo)!;
  const pixelSize = {
    width: level?.pixelWidth ?? 0,
    height: level?.pixelHeight ?? 0
  };
  const referencedImage: ReferencedImage = {
    sopClassUid: referencedSopClassUid,
    sopInstanceUid: referencedSopInstanceUid,
    pixelSize,
    dicomModel: level.dicomModel,
  };
  const seriesInstanceUid =
    getValue(refSeriesModel, DicomTag.SERIES_INSTANCE_UID) ||
    // Use the series of the annotation DICOM instance as the default series
    // reference.
    getValue(dicomModel, DicomTag.SERIES_INSTANCE_UID);
  return {
    referencedImage,
    seriesInstanceUid,
  };
}

function uidToTruncedInt(seriesUID: string): number {
  // Remove periods and convert the remaining large number string to a BigInt.
  return Number(seriesUID.replace(/[^0-9]/g, '').slice(-9));
}

/**
 * Service to interact with Dicom Annotations store
 */
@Injectable({ providedIn: 'root' })
export class DicomAnnotationsService implements OnDestroy {

  currentUser = '';
  private readonly destroyed$ = new ReplaySubject<boolean>(1);
  readonly dicomAnnotationInstances$ =
    new BehaviorSubject<DicomAnnotationInstance[]>([]);
  readonly loadingDicomAnnotations$ = new BehaviorSubject<boolean>(true);
  readonly uuidGenerator?: UuidGenerator;

  constructor(
    private readonly authGuard: AuthService,
    private readonly http: HttpClient,
    private readonly logService: LogService,
    private readonly userService: UserService,
    @Optional() readonly uuidGen?: UuidGenerator,
  ) {
    if (uuidGen) {
      this.uuidGenerator = uuidGen;
    } else if (environment.DICOM_GUID_PREFIX) {
      this.uuidGenerator =
        new UuidGenerator(environment.DICOM_GUID_PREFIX, this.logService);
    }

    (environment.ANNOTATION_HASH_STORED_USER_EMAIL ?
      this.userService.getCurrentUserHash$() :
      this.userService.getCurrentUser$()).subscribe((currentUser) => {
      this.currentUser = currentUser ?? '';
    });
  }

  ngOnDestroy() {
    this.destroyed$.next(true);
    this.destroyed$.complete();
  }

  annotationsToDicomModel(dicomAnnotation: DicomAnnotation): DicomModel {
    if (!dicomAnnotation || !dicomAnnotation.annotationGroupSequence) {
      return {};
    }
    const refImageDicomModel =
      dicomAnnotation.instance.referencedSeries.referencedImage.dicomModel;

    const date = new Date();
    // DICOM VR type DA format: YYYYMMDD.
    const formattedDate = date.toJSON().slice(0, 10).split('-').join('');
    const currentDAAttribute = dicomAttr('DA', formattedDate);

    // DICOM VR type TM format: HHMMSS.FFFFFF.
    const currentTMAttribute =
      dicomAttr('TM', [`${String(date.getHours()).padStart(2, '0')}${String(date.getMinutes()).padStart(2, '0')}${String(date.getSeconds()).padStart(2, '0')}.${String(date.getMilliseconds()).padEnd(6, '0')}`]);

    const clonedDicomAnnotationModel: DicomModel = {
      // DICOM conformant tags.
      '00480301' /* PixelOriginInterpretation */: dicomAttr('CS', 'VOLUME'),
      '00200010' /* StudyID */: refImageDicomModel['00200010'],
      '00400560' /* Specimen Description Sequence */:
        refImageDicomModel['00400560'],
      '00020012' /* Implementation Class UID */:
        dicomAttr('UI', '1.3.6.1.4.1.11129.5.4.1' /* google */),
      '00200013' /* Instance Number */: dicomAttr(
        'IS', [uidToTruncedInt(dicomAnnotation.instance.path.instanceUID!)]),
      '00020013' /* ImplementationVersionName */: dicomAttr('SH', 'Google'),
      '00400513' /* IssuerOfTheContainerIdentifierSequence */: dicomAttr('CS'),
      '00400518' /* ContainerTypeCodeSequence */: dicomAttr('SQ'),
      '00400555' /* Container Identifier Sequence */:
        refImageDicomModel['00400555'],
      '00080005' /* SpecificCharacterSet */: dicomAttr('CS', 'ISO_IR 192'),
      '00080020' /* StudyDate */: refImageDicomModel['00080020'],
      '00080023' /* ContentDate */: currentDAAttribute,
      '00080030' /* StudyTime */: refImageDicomModel['00080030'],
      '00080033' /* ContentTime */: currentTMAttribute,
      '00080050' /* AccessionNumber */: refImageDicomModel['00080050'],
      '00080070' /* Manufacturer */: dicomAttr('LO', 'GOOGLE'),
      '00080080' /* InstitutionName */: dicomAttr('LO'),
      '00080090' /* ReferringPhysicianName */: dicomAttr('PN'),
      '00081090' /* ManufacturerModelName */: dicomAttr('LO', 'Madcap'),
      '00100010' /* PatientName */: refImageDicomModel['00100010'],
      '00100020' /* PatientID */: refImageDicomModel['00100020'],
      '00100030' /* PatientBirthDate */: refImageDicomModel['00100030'],
      '00100040' /* PatientSex */: refImageDicomModel['00100040'],
      '00181000' /* DeviceSerialNumber */: dicomAttr('LO', '1.0'),
      '00181020' /* SoftwareVersions */: dicomAttr('LO', '1.0'),
      '00200011' /* SeriesNumber */: dicomAttr(
        'IS', [uidToTruncedInt(dicomAnnotation.instance.path.seriesUID!)]),
      '00700081' /* ContentDescription */:
        dicomAttr('LO', 'microscopy annotations'),
      '00700080' /* ContentLabel */: dicomAttr('CS', 'SM_ANN'),
      [DicomTag.CONTAINER_IDENTIFIER]:
        refImageDicomModel[DicomTag.CONTAINER_IDENTIFIER],

      // DICOM annotations
      [DicomTag.TOTAL_PIXEL_MATRIX_ORIGIN_SEQUENCE]:
        refImageDicomModel[DicomTag.TOTAL_PIXEL_MATRIX_ORIGIN_SEQUENCE],
      [DicomTag.MODALITY]: dicomAttr('CS', DicomModality.ANNOTATION),
      [DicomTag.TRANSFER_SYNTAX_UID]:
        dicomAttr('UI', TransferSyntaxUID.IMPLICIT_VR_LITTLE_ENDIAN),
      [DicomTag.SOP_CLASS_UID]: dicomAttr(
        'UI', SopClassUid.MICROSCOPY_BULK_SIMPLE_ANNOTATIONS_STORAGE),
      [DicomTag.STUDY_INSTANCE_UID]:
        dicomAttr('UI', dicomAnnotation.instance.path.studyUID!),
      [DicomTag.SERIES_INSTANCE_UID]:
        dicomAttr('UI', dicomAnnotation.instance.path.seriesUID!),
      [DicomTag.SOP_INSTANCE_UID]:
        dicomAttr('UI', dicomAnnotation.instance.path.instanceUID!),
      [DicomTag.ANNOTATION_COORDINATE_TYPE]:
        dicomAttr('CS', dicomAnnotation.annotationCoordinateType),
      [DicomTag.INSTANCE_CREATION_TIME]: currentTMAttribute,
      [DicomTag.INSTANCE_CREATION_DATE]: currentDAAttribute,
    };

    // Helper function to add attribute to cloned model.
    const addAttribute = (tag: DicomTag, value: AttributeValue, vr: string) => {
      clonedDicomAnnotationModel[tag] = dicomAttr(vr, value);
    };

    if (dicomAnnotation.instance.referencedSeries) {
      const referencedSeriesModel = this.buildReferencedSeriesModel(
        dicomAnnotation.instance.referencedSeries);
      addAttribute(
        DicomTag.REFERENCED_SERIES_SEQUENCE, [referencedSeriesModel],
        'SQ');
      addAttribute(
        DicomTag.REFERENCED_IMAGE_SEQUENCE,
        getSequenceValue(
          referencedSeriesModel, DicomTag.REFERENCED_INSTANCE_SEQUENCE),
        'SQ');
    }
    // Build AnnotationGroupSequence.
    const annotationGroups = dicomAnnotation.annotationGroupSequence.map(
      group => this.buildAnnotationGroupModel(
        group,
        dicomAnnotation.instance.referencedSeries.referencedImage.pixelSize!
      ));

    if (annotationGroups.length > 0) {
      addAttribute(
        DicomTag.ANNOTATION_GROUP_SEQUENCE, annotationGroups, 'SQ');
    }

    // PersonIdentificationCode
    const personIdentificationCodeSequence: Attribute = dicomAttr('SQ', [
      {
      [DicomTag.LONG_CODE_VALUE]:
        dicomAttr('UC', dicomAnnotation.instance.annotatorId ?? 'unknown'),
      [DicomTag.CODE_MEANING]: dicomAttr('LO', 'author'),
      ['00080102' /* CodingSchemeDesignator */]: dicomAttr('LO', '99AUTH'),
    }]);

    clonedDicomAnnotationModel[DicomTag.OPERATOR_IDENTIFICATION_SEQUENCE] =
      dicomAttr('SQ', [{
        [DicomTag.PERSON_IDENTIFICATION_CODE_SEQUENCE]: personIdentificationCodeSequence}]);

    return clonedDicomAnnotationModel;
  }

  bulkDataURItoInlineBinary(attr: Attribute, accessToken: string):
    Observable<Attribute> {
    if (attr.BulkDataURI) {
      const headers = {
        ...(accessToken && { 'authorization': 'Bearer ' + accessToken }),
        'Accept': 'application/octet-stream; transfer-syntax=*',
      };
      return this.http
        .get(attr.BulkDataURI, { headers, responseType: 'arraybuffer' })
        .pipe(
          map(arrayBuffer => arrayBufferToBase64(arrayBuffer)), map(b64 => {
            attr.InlineBinary = b64;
            return attr;
          }));
    }
    return of(attr);
  }

  createOrModifyDicomAnnotation(
    dicomAnnotation: DicomAnnotation,
    retriesLeft: number = DEFAULT_RETRY,
  ): Observable<DicomModel | {}> {
    if (!environment.ANNOTATIONS_DICOM_STORE_BASE_URL) return EMPTY;
    // Generate new uuid.
    dicomAnnotation.instance.path.instanceUID = this.generateUUID();

    const previousAnnotationInstances =
      [...this.dicomAnnotationInstances$.getValue()].find(
        (annotationInstance) =>
          annotationInstance.annotatorId === this.currentUser);

    // Determines series ID.
    // 1. Maintaining the same series ID if it exists to this user.
    // 2. Otherwise, if it exists, use a series ID used of an annotation by
    // another user.
    // 3. Otherwise, generate a new series UID.
    if (previousAnnotationInstances) {
      dicomAnnotation.instance.path.seriesUID =
        previousAnnotationInstances.path.seriesUID;
    } else if (this.dicomAnnotationInstances$.getValue().length > 0) {
      dicomAnnotation.instance.path.seriesUID =
        this.dicomAnnotationInstances$.getValue()[0].path.seriesUID;
    } else {
      dicomAnnotation.instance.path.seriesUID = this.generateUUID(true);
    }

    // DICOM Proxy adds the annotatorId.
    // Merge json model with new annotations and add new uid.
    const newDicomModel = this.annotationsToDicomModel(dicomAnnotation);

    if (!previousAnnotationInstances) {
      return this.writeDicomAnnotations(newDicomModel, retriesLeft)
        .pipe(
          takeUntil(this.destroyed$),
          tap((response) => {
            const newAnnotationInstance = dicomAnnotation.instance;

            const existingAnnotationInstances =
              this.dicomAnnotationInstances$.getValue();
            const newAnnotationInstances: DicomAnnotationInstance[] =
              [newAnnotationInstance];
            if (existingAnnotationInstances.length) {
              newAnnotationInstances.push(...existingAnnotationInstances);
            }

            this.dicomAnnotationInstances$.next(newAnnotationInstances);
          }),
          map(() => {
            return newDicomModel;
          }),
          finalize(() => {
            this.loadingDicomAnnotations$.next(false);
          }),
        );
    }

    this.loadingDicomAnnotations$.next(true);
    return this.writeDicomAnnotations(newDicomModel, retriesLeft)
      .pipe(
        takeUntil(this.destroyed$),
        map(() => {
          const newAnnotationInstance = dicomAnnotation.instance;

          let existingAnnotationInstances =
            this.dicomAnnotationInstances$.getValue();
          const previousAnnotationInstances =
            existingAnnotationInstances.filter(
              ({ annotatorId }) => annotatorId === this.currentUser);
          existingAnnotationInstances = existingAnnotationInstances.filter(
            ({ annotatorId }) => annotatorId !== this.currentUser);

          existingAnnotationInstances.push(newAnnotationInstance);

          this.dicomAnnotationInstances$.next(existingAnnotationInstances);

          return previousAnnotationInstances;
        }),
        switchMap(
          (annotationInstancesToDelete: DicomAnnotationInstance[]) => {
            const forkJoinObservables = annotationInstancesToDelete.map(
              (annotationInstance) => this.deleteDicomAnnotationsPath(
                annotationInstance.path));
            return forkJoin(forkJoinObservables);
          }),
        map(() => {
          return newDicomModel;
        }),
        finalize(() => {
          this.loadingDicomAnnotations$.next(false);
        }),
      );
  }

  deleteDicomAnnotations(annotationInstancePath: string): Observable<{}> {
    this.loadingDicomAnnotations$.next(true);
    const deleteUrl =
      environment.ANNOTATIONS_DICOM_STORE_BASE_URL + environment.ANNOTATIONS_DICOM_STORE_PARENT + annotationInstancePath;
    this.loadingDicomAnnotations$.next(true);

    return this.authGuard.getOAuthToken().pipe(
      takeUntil(this.destroyed$),
      switchMap((accessToken) => {
        const headers: {
          authorization?: string,
        } = {
          ...(accessToken && { 'authorization': 'Bearer ' + accessToken }),
        };
        return this.http.delete(deleteUrl, { headers });
      }),
      catchError((error) => {
        error = JSON.stringify(error);
        this.logService.error(
          { name: `httpRequest: "${deleteUrl}"`, message: error });
        const errorMessage = 'Error while deleting Dicom annotations.';

        throw new Error(errorMessage);
      }),
      finalize(() => {
        this.loadingDicomAnnotations$.next(false);
      }),
    );
  }

  deleteDicomAnnotationsPath(annotationInstancePath: DicomWebPath) {
    return this.deleteDicomAnnotations(
      constructDicomWebPath(annotationInstancePath));
  }

  // Converts an annotation DicomModel to a structure containing the SOP
  // instance ID, annotator ID, and creation date and time.
  dicomModelToAnnotationInstance(
    dicomModel: DicomModel, slideInfo: SlideInfo,
    index = 0): DicomAnnotationInstance {
    const instanceUID = getValue(dicomModel, DicomTag.SOP_INSTANCE_UID);
    const seriesUID = getValue(dicomModel, DicomTag.SERIES_INSTANCE_UID);
    const studyUID = getValue(dicomModel, DicomTag.STUDY_INSTANCE_UID);

    const annotatorId =
      this.getAnnotatorIdByDicomAnnotationInstance(dicomModel) ||
      `Unknown user ${index + 1}`;
    const creationDateAndTime =
      this.getCreationDateAndTimeByDicomModel(dicomModel);

    return {
      path: {
        studyUID,
        seriesUID,
        instanceUID,
      },
      annotatorId,
      referencedSeries: getReferencedSeries(dicomModel, slideInfo),
      ...(creationDateAndTime && { creationDateAndTime }),
    };
  }

  dicomModelToAnnotations(dicomModel: DicomModel, slideInfo: SlideInfo):
    DicomAnnotation {
    const referencedSeries = getReferencedSeries(dicomModel, slideInfo);
    const pixelSize = referencedSeries.referencedImage.pixelSize!;
    const annotationCoordinateType =
      getValue(dicomModel, DicomTag.ANNOTATION_COORDINATE_TYPE);

    const dicomAnnotation: DicomAnnotation = {
      instance: this.dicomModelToAnnotationInstance(dicomModel, slideInfo),
      annotationCoordinateType,
      annotationGroupSequence:
        getSequenceValue(dicomModel, DicomTag.ANNOTATION_GROUP_SEQUENCE)
          .map((sequence) => {
            return this.dicomModelToAnnotationGroup(
              sequence, pixelSize, annotationCoordinateType);
          })
          .flat(),
    };
    return dicomAnnotation;
  }

  // Retrieve DicomAnnotationInstances for the specified slide path.
  fetchDicomAnnotationInstances(slidePath: string, slideInfo: SlideInfo):
    Observable<DicomAnnotationInstance[]> {
    if (!environment.ENABLE_ANNOTATIONS) {
      return of([]);
    }
    const slideDicomWebUrlConfig: DicomWebUrlConfig =
      parseDICOMwebUrl(slidePath);

    // Construct query to find all instances in the study with an annotation
    // model.
    const annotationDicomWebUrlConfig: DicomWebUrlConfig = {
      baseUrl: environment.ANNOTATIONS_DICOM_STORE_BASE_URL + environment.ANNOTATIONS_DICOM_STORE_PARENT,
      path: {
        studyUID: slideDicomWebUrlConfig.path.studyUID,
      },
      resource: 'instances',
      queryParams: {
        includefield: [
          DicomTag.STUDY_INSTANCE_UID,
          DicomTag.OPERATOR_IDENTIFICATION_SEQUENCE,
          DicomTag.INSTANCE_CREATION_DATE,
          DicomTag.INSTANCE_CREATION_TIME,
          DicomTag.REFERENCED_INSTANCE_SEQUENCE,
          DicomTag.REFERENCED_IMAGE_SEQUENCE,
          DicomTag.REFERENCED_SERIES_SEQUENCE,
          DicomTag.ANNOTATION_COORDINATE_TYPE
        ].join(','),
        modality: 'ANN',
      },
    };

    const fetchUrl = constructDicomWebUrl(annotationDicomWebUrlConfig);

    return this.authGuard.getOAuthToken().pipe(
      takeUntil(this.destroyed$),
      switchMap((accessToken) => {
        const headers = {
          ...(accessToken && { 'authorization': 'Bearer ' + accessToken }),
          'content-type': 'application/dicom+json',
          'Accept': 'application/dicom+json'
        };

        if (!this.loadingDicomAnnotations$.getValue()) {
          this.loadingDicomAnnotations$.next(true);
        }
        return this.http.get<DicomModel[]>(fetchUrl, { headers });
      }),
      // Filter empty results.
      map((x) => {
        if (!!x && x.length > 0 && !!(x[0])) {
          return x;
        }
        return [];
      }),
      // Keep only DicomModels with reference to the provided slide path.
      map((dicomAnnotatorsModels: DicomModel[]) => {
        return DicomAnnotationsService.filterDicomModelsByReferencedSeriesUID(
          dicomAnnotatorsModels, slideDicomWebUrlConfig.path.seriesUID!);
      }),
      map((dicomAnnotatorsModels: DicomModel[]) => {
        if (!dicomAnnotatorsModels) {
          this.dicomAnnotationInstances$.next([]);
          return [];
        }
        const annotationInstances =
          dicomAnnotatorsModels
            // Filter out annotations not containing coordinate type,
            // likely not bulk microscopy annotations.
            .filter(
              dicomAnnotatorsModels => dicomAnnotatorsModels
              [DicomTag.ANNOTATION_COORDINATE_TYPE])
            .map((dicomAnnotatorsModel, index) => {
              return this.dicomModelToAnnotationInstance(
                dicomAnnotatorsModel, slideInfo, index);
            });

        this.dicomAnnotationInstances$.next(annotationInstances);
        return annotationInstances;
      }),
      catchError((error) => {
        this.dicomAnnotationInstances$.next([]);
        this.logService.error({
          name: `httpRequest: "${fetchUrl}"`,
          message: JSON.stringify(error)
        });

        throw error;
      }),
      finalize(() => {
        this.loadingDicomAnnotations$.next(false);
      }),
    );
  }

  // Retrieves DICOM annotations for a given annotation instance path.
  fetchDicomAnnotationModels(annInstancePath: string):
    Observable<DicomModel[]> {
    this.loadingDicomAnnotations$.next(true);
    if (!environment.ENABLE_ANNOTATIONS) {
      return of([]);
    }

    const tokenObservable = this.authGuard.getOAuthToken();

    return tokenObservable.pipe(
      takeUntil(this.destroyed$),
      switchMap((accessToken) => {
        const headers = {
          ...(accessToken && { 'authorization': 'Bearer ' + accessToken }),
          'content-type': 'application/dicom+json',
          'Accept': 'application/dicom+json',
        };

        if (!this.loadingDicomAnnotations$.getValue()) {
          this.loadingDicomAnnotations$.next(true);
        }

        const instanceDicomWebUrlConfig = parseDICOMwebUrl(annInstancePath);
        instanceDicomWebUrlConfig.baseUrl = environment.ANNOTATIONS_DICOM_STORE_BASE_URL + environment.ANNOTATIONS_DICOM_STORE_PARENT;
        instanceDicomWebUrlConfig.resource = 'metadata';

        const fetchUrl = constructDicomWebUrl(instanceDicomWebUrlConfig);

        return this.http.get<DicomModel[]>(fetchUrl, { headers });
      }),
      mergeMap(models => models),
      withLatestFrom(tokenObservable),
      mergeMap(([model, token]) => {
        const annotationGroupSequence =
          getSequenceValue(model, DicomTag.ANNOTATION_GROUP_SEQUENCE);
        // If bulkdataURI attribute is found, fetch it and store it as
        // InlineBinary.
        const bulkdataURIObservables =

          [
            DicomTag.POINT_COORDINATES_DATA,
            DicomTag.DOUBLE_POINT_COORDINATES_DATA,
            DicomTag.LONG_PRIMITIVE_POINT_INDEX_LIST
          ]
            .map(
              tag => annotationGroupSequence
                .map(
                  (grpModel, index) =>
                    ({ attr: grpModel[tag], index }))
                .filter(({ attr }) => attr?.BulkDataURI)
                .map(
                  ({ attr, index }) => {
                    return this
                      .bulkDataURItoInlineBinary(
                        attr, token)
                      .pipe(
                        tap((attr) => {
                          annotationGroupSequence[index][tag] =
                            attr;
                        }),
                      );
                  }))
            .flat();

        if (bulkdataURIObservables.length === 0) {
          return of(model);
        }
        return forkJoin(bulkdataURIObservables).pipe(map(() => model));
      }),
      toArray(),
      takeUntil(this.destroyed$),
      catchError((error) => {
        error = JSON.stringify(error);
        this.logService.error({ name: `httpRequest: `, message: error });
        const errorMessage = 'Error while fetching Dicom annotations.';

        throw new Error(errorMessage);
      }),
      finalize(() => {
        this.loadingDicomAnnotations$.next(false);
      }),
    );
  }

  // Retrieve Annotator ID from DICOM Annotation Model: Navigate nested tags,
  // handle missing tags/values, and extract ID from LONG_CODE_VALUE in
  // PERSON_IDENTIFICATION_CODE_SEQUENCE.
  getAnnotatorIdByDicomAnnotationInstance(dicomAnnotationModel: DicomModel):
    string {
    if (!dicomAnnotationModel[DicomTag.OPERATOR_IDENTIFICATION_SEQUENCE]) {
      return '';
    }
    let annotatorId = '';
    let operator =
      (dicomAnnotationModel[DicomTag.OPERATOR_IDENTIFICATION_SEQUENCE]
        ?.Value ??
        [''])[0] as (DicomModel | string);
    if (!(operator instanceof String)) {
      operator = operator as DicomModel;
      let personIdentificationCode =
        (operator[DicomTag.PERSON_IDENTIFICATION_CODE_SEQUENCE]?.Value ??
          [''])[0] as (DicomModel | string);
      if (!(personIdentificationCode instanceof String)) {
        personIdentificationCode = personIdentificationCode as DicomModel;
        let longCodeValue =
          (personIdentificationCode[DicomTag.LONG_CODE_VALUE]?.Value ??
            [''])[0] as (DicomModel | string);
        if (!(longCodeValue instanceof String)) {
          longCodeValue = longCodeValue as DicomModel;
          annotatorId =
            (personIdentificationCode[DicomTag.LONG_CODE_VALUE]?.Value ??
              [''])[0] as (string);
        }
      }
    }

    return annotatorId;
  }

  getUniqueAnnotationInstances(annotationInstances: DicomAnnotationInstance[]) {
    annotationInstances = [...annotationInstances];
    const uniqueAnnotators = new Map<string, DicomAnnotationInstance>();

    annotationInstances.forEach((data) => {
      if (uniqueAnnotators.has(data.annotatorId)) {
        const uniqueData = uniqueAnnotators.get(data.annotatorId);
        if ((uniqueData?.creationDateAndTime ?? 0) <
          (data?.creationDateAndTime ?? 0)) {
          uniqueAnnotators.set(data.annotatorId, data);
        }
      } else {
        uniqueAnnotators.set(data.annotatorId, data);
      }
    });

    annotationInstances =
      [...uniqueAnnotators.values()] as DicomAnnotationInstance[];

    return annotationInstances;
  }

  private buildAnnotationGroupModel(
    annotationGroup: AnnotationGroup, pixelSize: PixelSize): DicomModel {
    const annotationGroupModel: DicomModel = {
      [DicomTag.ANNOTATION_GROUP_NUMBER]:
        dicomAttr('US', [annotationGroup.idNumber]),
      [DicomTag.ANNOTATION_GROUP_UID]: dicomAttr(
        'UI', annotationGroup.annotationGroupUid || this.generateUUID()),
      [DicomTag.ANNOTATION_GROUP_LABEL]:
        dicomAttr('LO', annotationGroup.annotationGroupLabel),
      [DicomTag.ANNOTATION_GROUP_DESCRIPTION]:
        dicomAttr('UT', annotationGroup.annotationGroupDescription),
      [DicomTag.ANNOTATION_GROUP_GENERATION_TYPE]:
        dicomAttr('CS', annotationGroup.annotationGroupGenerationType),
    } as DicomModel;

    if (annotationGroup.annotationPropertyCategoryCodeSequence?.length) {
      annotationGroupModel[DicomTag
        .ANNOTATION_PROPERTY_CATEGORY_CODE_SEQUENCE] =
        dicomAttr(
          'SQ',
          [this.buildAnnotationPropertyCategoryCodeModel(
            annotationGroup.annotationPropertyCategoryCodeSequence[0])]);
    } else {
      // Use default category
      // dicom.nema.org/medical/dicom/current/output/chtml/part03/chapter_8.html#sect_8.1

      const defaultCategory = {
        '00080100': dicomAttr('SH', '91723000'),  // Anatomical structure
        '00080102': dicomAttr('SH', 'SCT'),       // SNOMED CT code
        '00080104': dicomAttr('LO', 'Anatomical structure'),
      };
      annotationGroupModel[DicomTag
        .ANNOTATION_PROPERTY_CATEGORY_CODE_SEQUENCE] =
        dicomAttr('SQ', [defaultCategory]);
    }

    // Hardcode a generic label until we give such option in to the user.
    // The comment provided by the user represents the text meaning.
    const propertyTypeCodeSequence = {
      '00080100':
        dicomAttr('SH', '395538009'),    // Microscopic specimen observation
      '00080102': dicomAttr('SH', 'SCT'),  // SNOMED CT code
      '00080104': dicomAttr('LO', annotationGroup.annotationGroupDescription),
    };
    annotationGroupModel[DicomTag.ANNOTATION_PROPERTY_TYPE_CODE_SEQUENCE] =
      dicomAttr('SQ', [propertyTypeCodeSequence]);

    annotationGroupModel[DicomTag.GRAPHIC_TYPE] =
      dicomAttr('CS', annotationGroup.graphicType);

    const pixelCoordinates = metersToPixelsWithYFlip(
      annotationGroup.pointCoordinatesData, pixelSize);
    const base64Coordinates = numberArrayToInlineBinary(
      Float32Array,
      isPolygonClockwise(pixelCoordinates) ?
        pixelCoordinates :
        flipPointOrder(pixelCoordinates));
    annotationGroupModel[DicomTag.POINT_COORDINATES_DATA] = {
      'InlineBinary': base64Coordinates,
      'vr': 'OF'
    } as Attribute;

    const longPrimitivePointIndexList =
      annotationGroup.longPrimitivePointIndexList;
    const base64LongPrimitive =
      numberArrayToInlineBinary(Int32Array, longPrimitivePointIndexList);
    annotationGroupModel[DicomTag.LONG_PRIMITIVE_POINT_INDEX_LIST] = {
      'InlineBinary': base64LongPrimitive,
      'vr': 'OL'
    } as Attribute;

    annotationGroupModel[DicomTag.NUMBER_OF_ANNOTATIONS] =
      dicomAttr('UL', [longPrimitivePointIndexList.length]);

    annotationGroupModel[DicomTag.ANNOTATION_APPLIES_TO_ALL_OPTICAL_PATHS] =
      dicomAttr('CS', 'YES');

    return annotationGroupModel;
  }

  private buildAnnotationPropertyCategoryCodeModel(
    annotationPropertyCategoryCode: AnnotationPropertyCategoryCode):
    DicomModel {
    return {
      [DicomTag.CODE_VALUE]:
        dicomAttr('SH', [annotationPropertyCategoryCode.codeValue]),
      [DicomTag.CODING_SCHEME_DESIGNATOR]: dicomAttr(
        'SH',
        [String(annotationPropertyCategoryCode.codingSchemeDesignator)]),
      [DicomTag.CODING_SCHEME_VERSION]:
        dicomAttr('SH', [annotationPropertyCategoryCode.codingSchemeVersion]),
      [DicomTag.CONTEXT_IDENTIFIER]: dicomAttr(
        'CS',
        annotationPropertyCategoryCode.contextIdentifier ?
          [annotationPropertyCategoryCode.contextIdentifier] :
          undefined),
    };
  }

  private buildReferencedSeriesModel(refSeries: ReferencedSeries): DicomModel {
    const refSeriesModel: DicomModel = {
      [DicomTag.SERIES_INSTANCE_UID]:
        dicomAttr('UI', [refSeries.seriesInstanceUid]),
    } as DicomModel;

    if (refSeries.referencedImage) {
      // Create Referenced Image Sequence
      const refImageSequence: Attribute = dicomAttr(
        'SQ', [{
          // Referenced SOP Class UID
          [DicomTag.REFERENCED_SOP_CLASS_UID]:
            dicomAttr('UI', [refSeries.referencedImage.sopClassUid]),
          // Referenced SOP Instance UID
          [DicomTag.REFERENCED_SOP_INSTANCE_UID]:
            dicomAttr('UI', [refSeries.referencedImage.sopInstanceUid]),
        }]);

      // Add Referenced Image
      refSeriesModel[DicomTag.REFERENCED_INSTANCE_SEQUENCE] =
        refImageSequence;
    }
    return refSeriesModel;
  }

  private dicomModelToAnnotationGroup(
    annotationGroupModel: DicomModel, pixelSize: PixelSize,
    annotationCoordinateType: string): AnnotationGroup[] {
    const annotationGroup: AnnotationGroup = {} as AnnotationGroup;
    if (annotationCoordinateType !== '2D') {
      annotationGroup.pointCoordinatesData = [];
      annotationGroup.error =
        `Unsupported coordinate type: ${annotationCoordinateType}`;
      return [annotationGroup];
    }

    annotationGroup.idNumber = Number(
      annotationGroupModel[DicomTag.ANNOTATION_GROUP_NUMBER].Value);
    annotationGroup.annotationGroupUid =
      (annotationGroupModel[DicomTag.ANNOTATION_GROUP_UID].Value as
        string[])[0];
    if (annotationGroupModel[DicomTag.ANNOTATION_GROUP_LABEL]?.Value) {
      annotationGroup.annotationGroupLabel =
        (annotationGroupModel[DicomTag.ANNOTATION_GROUP_LABEL].Value as
          string[])[0];
    }
    if (annotationGroupModel[DicomTag.ANNOTATION_GROUP_DESCRIPTION]
      ?.Value) {
      annotationGroup.annotationGroupDescription =
        (annotationGroupModel[DicomTag.ANNOTATION_GROUP_DESCRIPTION]
          .Value as string[])[0];
    }
    annotationGroup.annotationGroupGenerationType =
      (annotationGroupModel[DicomTag.ANNOTATION_GROUP_GENERATION_TYPE]
        .Value as ['MANUAL'] |
        ['SEMIAUTOMATIC'] | ['AUTOMATIC'])[0];
    // Parse out AnnotationPropertyCategoryCodeSequence.
    // Sequence will always only have 1 element.
    const annotationPropertyCategoryCodeSeq = getSequenceValue(
      annotationGroupModel,
      DicomTag.ANNOTATION_PROPERTY_CATEGORY_CODE_SEQUENCE);
    if (annotationPropertyCategoryCodeSeq?.length) {
      const annotationPropertyCategoryCodeModel =
        annotationPropertyCategoryCodeSeq[0];
      const annotationPropertyCategoryCode: AnnotationPropertyCategoryCode = {
        codeValue: getValue(
          annotationPropertyCategoryCodeModel, DicomTag.CODE_VALUE),
        codingSchemeDesignator: Number(getValue(
          annotationPropertyCategoryCodeModel,
          DicomTag.CODING_SCHEME_DESIGNATOR)),
        codingSchemeVersion: getValue(
          annotationPropertyCategoryCodeModel,
          DicomTag.CODING_SCHEME_VERSION),
        contextIdentifier: getValue(
          annotationPropertyCategoryCodeModel,
          DicomTag.CONTEXT_IDENTIFIER),
      };
      annotationGroup.annotationPropertyCategoryCodeSequence =
        [annotationPropertyCategoryCode];
    }
    annotationGroup.graphicType =
      (annotationGroupModel[DicomTag.GRAPHIC_TYPE].Value as ['POINT'] |
      ['POLYGON'] | ['RECTANGLE'])[0];

    const binaryPointIndexList =
      annotationGroupModel[DicomTag.LONG_PRIMITIVE_POINT_INDEX_LIST]
        ?.InlineBinary ??
      '';
    const binaryPointIndexType =
      annotationGroupModel[DicomTag.LONG_PRIMITIVE_POINT_INDEX_LIST]
        ?.vr ??
      'OL';
    const inlineBinaryNumber = inlineBinaryToNumberArray(
        vrToType(binaryPointIndexType as string), binaryPointIndexList);
    if (!inlineBinaryNumber.length) {
      inlineBinaryNumber.push(1);  // default, assume single shape
    }

    const indecesArr =
        (inlineBinaryNumber).map((v) => v - 1);  // change to 0 based indeces.

    // Parse out Point Coordinates Data.
    const coordinatesTag =
      annotationGroupModel[DicomTag.DOUBLE_POINT_COORDINATES_DATA]
        ?.InlineBinary ?
        DicomTag.DOUBLE_POINT_COORDINATES_DATA :
        DicomTag.POINT_COORDINATES_DATA;
    const binaryCoordinates =
      annotationGroupModel[coordinatesTag]?.InlineBinary ?? '';
    const binaryCoordinatesType =
      annotationGroupModel[coordinatesTag]?.vr ?? '';

    const coordsArr = inlineBinaryToNumberArray(
      vrToType(binaryCoordinatesType as string), binaryCoordinates);

    // Check if the indeces array ends with the end of the coord array index
    if (indecesArr.slice(-1)[0] !== coordsArr.length) {
      indecesArr.push(coordsArr.length);
    }

    const annotationGroups: AnnotationGroup[] = [];

    if (annotationGroup.graphicType === 'POINT') {
      const newAnnotationGroup = { ...annotationGroup };
      newAnnotationGroup.pointCoordinatesData =
        this.pixelsToMetersWithYFlip(coordsArr, pixelSize);
      annotationGroups.push(newAnnotationGroup);
    } else {
      for (let i = 0; i < indecesArr.length - 1; i++) {
        const newAnnotationGroup = { ...annotationGroup };
        newAnnotationGroup.pointCoordinatesData = this.pixelsToMetersWithYFlip(
          coordsArr.slice(indecesArr[i], indecesArr[i + 1]), pixelSize);
        annotationGroups.push(newAnnotationGroup);
      }
    }

    return annotationGroups;
  }

  private generateUUID(updateRandomFrac = false): string {
    if (!this.uuidGenerator) return '';

    if (updateRandomFrac) {
      this.uuidGenerator.updateRandomFraction();
    }
    this.uuidGenerator.incrementCounter();
    return this.uuidGenerator.generateUid();
  }

  private getCreationDateAndTimeByDicomModel(dicomModel: DicomModel): Date
    | undefined {
    if (!dicomModel[DicomTag.INSTANCE_CREATION_DATE]?.Value) {
      return;
    }

    const dateString = (dicomModel[DicomTag.INSTANCE_CREATION_DATE].Value ??
      [''])[0] as string;
    if (!dateString) return;

    const year = Number(dateString.substring(0, 4));
    const month = Number(dateString.substring(4, 6));
    const day = Number(dateString.substring(6, 8));
    const date = new Date(year, month - 1, day);

    const timeString = (dicomModel[DicomTag.INSTANCE_CREATION_TIME].Value ??
      [''])[0] as string;
    const hour = Number(timeString.slice(0, 2));
    const minutes = Number(timeString.slice(2, 4));
    const seconds = Number(timeString.slice(4, 6));
    const milliseconds = Number(timeString.slice(7));

    date.setHours(hour, minutes, seconds, milliseconds);

    return date;
  }

  private pixelsToMetersWithYFlip(coordsArr: number[], pixelSize: PixelSize):
    number[][] {
    const px = pixelSizeInMeters(pixelSize.width);
    const py = pixelSizeInMeters(pixelSize.height);

    const coords: number[][] = [];
    // flip y axis since openlayers y-axis points up.
    // Openlayers uses global projection with the Y axis pointing down, while
    // image coordinates the Y axis is pointing up.
    for (let i = 0; i < coordsArr.length; i += 2) {
      coords.push([coordsArr[i] * px, -(coordsArr[i + 1] * py)]);
    }
    return coords;
  }

  private static filterDicomModelsByReferencedSeriesUID(
    dicomModels: DicomModel[], referencedSeriesUID: string): DicomModel[] {
    return dicomModels.filter((dicomModel) => {
      const refImageModel = getSequenceValue(
        dicomModel, DicomTag.REFERENCED_SERIES_SEQUENCE)[0];
      const refSeriesUID =
        getValue(refImageModel, DicomTag.SERIES_INSTANCE_UID);
      const seriesUID = getValue(dicomModel, DicomTag.SERIES_INSTANCE_UID);
      // Retain this instance only if it belongs to the same series as the
      // referenced slide. If a specific slide is referenced (refSer), check
      // against its seriesUID. Otherwise, compare with the current
      // instance's seriesUID.
      return refSeriesUID ? refSeriesUID === referencedSeriesUID :
        seriesUID === referencedSeriesUID;
    });
  }

  private writeDicomAnnotations(
    newDicomModel: DicomModel,
    retriesLeft: number = DEFAULT_RETRY,
  ): Observable<DicomModel | {}> {
    this.loadingDicomAnnotations$.next(true);
    const createUrl = `${environment.ANNOTATIONS_DICOM_STORE_BASE_URL + environment.ANNOTATIONS_DICOM_STORE_PARENT}/studies`;

    return this.authGuard.getOAuthToken().pipe(
      takeUntil(this.destroyed$),
      switchMap((accessToken) => {
        if (!this.loadingDicomAnnotations$.getValue()) {
          this.loadingDicomAnnotations$.next(true);
        }

        const boundary = 'DICOMwebBoundary';
        const multipartRelatedContent = `--${boundary}\r\nContent-Type: application/dicom+json; transfer-syntax=1.2.840.10008.1.2.1\r\n\r\n${JSON.stringify([newDicomModel])}\r\n--${boundary}--`;

        const headers = {
          'Content-Type':
            'multipart/related; type="application/dicom+json";transfer-syntax=1.2.840.10008.1.2.1;boundary='+boundary,
          ...(accessToken && { 'authorization': 'Bearer ' + accessToken }),
          'Accept': 'application/dicom+json',
        };

        return this.http.post<DicomModel>(
          createUrl,
          multipartRelatedContent,
          { headers },
        );
      }),
      catchError((error) => {
        const stringifiedError = JSON.stringify(error);
        if (retriesLeft &&
          stringifiedError.includes(UID_TRIPLE_ALREADY_EXISTS_ERROR)) {
          retriesLeft--;
          const newUid = this.generateUUID(true);
          newDicomModel[DicomTag.SOP_INSTANCE_UID].Value = newUid;
          return this.writeDicomAnnotations(newDicomModel, retriesLeft);
        }

        this.logService.error(
          { name: `httpRequest: "${createUrl}"`, message: stringifiedError });

        throw error;
      }),

    );

  }
}
