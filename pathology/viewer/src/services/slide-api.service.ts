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

import { AssociatedImage, Level, LevelMap, SlideDescriptor, SlideExtraMetadata } from '../interfaces/slide_descriptor';
import { BehaviorSubject, EMPTY, Observable, throwError } from 'rxjs';
import { DicomModel, DicomTag, PersonName, SopClassUid, formatName } from '../interfaces/dicom_descriptor';
import { catchError, map } from 'rxjs/operators';

import { DicomwebService } from './dicomweb.service';
import { IccProfileType } from './dicomweb.service';
import { ImageTile } from '../interfaces/image_tile';
import { Injectable } from '@angular/core';
import { NUM_COLOR_CHANNELS_WITHOUT_TRANSPARENCY } from '../interfaces/slide_model';
import { SlideInfo } from '../interfaces/slide_descriptor';
import { environment } from '../environments/environment';
import { isSlideDeId } from '../interfaces/dicom_store_descriptor';

// The value is based on the best understanding of the Dicom standard and should
// be consistent with the ingest pipeline.
// See: go/dpas-ingest-pipeline-label-value
const IMAGE_TYPE_SLIDE_LABEL_VALUE = 'LABEL';

// The image type used for the tissue pyramid.
const IMAGE_TYPE_SLIDE_PYRAMID_VALUE = 'VOLUME';

const MIN_ZOOM_LEVEL_SIZE = 1024;  // something that can fit the screen.

/**
 * Retrieves slide info and tile data from the Healthcare API.
 */
@Injectable({
  providedIn: 'root',
})
export class SlideApiService {
  ENABLE_SERVER_INTERPOLATION: boolean = environment.ENABLE_SERVER_INTERPOLATION ?? false;

  readonly slideDescriptors$ = new BehaviorSubject<SlideDescriptor[]>([]);

  readonly selectedSlideInfo$ =
    new BehaviorSubject<SlideInfo | undefined>(undefined);
  readonly selectedExtraMetaData$ =
    new BehaviorSubject<SlideExtraMetadata | undefined>(undefined);

  constructor(
    private readonly dicomwebService: DicomwebService,
  ) { }

  private computeLevel(dicomModel: DicomModel, isFlatImage: boolean): Level {
    let frames =
      dicomModel[DicomTag.NUMBER_OF_FRAMES]?.Value?.[0] as number;
    let height = Number(
      dicomModel?.[DicomTag.TOTAL_PIXEL_MATRIX_ROWS]?.Value ?? NaN);
    let width = Number(
      dicomModel?.[DicomTag.TOTAL_PIXEL_MATRIX_COLUMNS]?.Value ?? NaN);


    const tileSize = Math.max(
      Number(dicomModel?.[DicomTag.ROWS]?.Value?.[0] ?? undefined),
      Number(dicomModel?.[DicomTag.COLS]?.Value?.[0] ?? undefined));
    const instanceUid =
      dicomModel[DicomTag.SOP_INSTANCE_UID]?.Value?.[0] as string;
    const offset = (dicomModel[DicomTag.OFFSET]?.Value?.[0] ?? 0) as number;
    const physicalWidthMillimeters =
      Number(dicomModel[DicomTag.IMAGE_VOLUME_WIDTH]?.Value ?? NaN);
    const physicalHeightMillimeters =
      Number(dicomModel[DicomTag.IMAGE_VOLUME_HEIGHT]?.Value ?? NaN);
    let pixelWidth: number | undefined = physicalWidthMillimeters / width;
    let pixelHeight: number | undefined = physicalHeightMillimeters / height;
    if ((!Number.isFinite(pixelWidth) || !Number.isFinite(pixelHeight)) &&
      dicomModel[DicomTag.PIXEL_SPACING]?.Value?.length === 2) {
      const ps = dicomModel[DicomTag.PIXEL_SPACING].Value;
      pixelWidth = Number(ps[0]);
      pixelHeight = Number(ps[1]);
    }
    pixelWidth =
      !pixelWidth || !Number.isFinite(pixelWidth) ? undefined : pixelWidth;
    pixelHeight =
      !pixelHeight || !Number.isFinite(pixelHeight) ? undefined : pixelHeight;

    if (isFlatImage) {
      frames = 1;
      height = Number(dicomModel?.[DicomTag.ROWS]?.Value?.[0] ?? undefined);
      width = Number(dicomModel?.[DicomTag.COLS]?.Value?.[0] ?? undefined);
    }

    // ⌈NumberOfFrames * tile width * tile height * Samples Per Pixel *
    // ⌈Allocated Bits Stored / 8⌉ / LossImageCompressionRatio⌉
    const storedBytes = Math.ceil(
      Number(dicomModel?.[DicomTag.ROWS]?.Value?.[0] ?? NaN) *
      Number(dicomModel?.[DicomTag.COLS]?.Value?.[0] ?? NaN) * frames *
      Number(dicomModel?.[DicomTag.SAMPLES_PER_PIXEL]?.Value?.[0] ?? NaN) *
      Math.ceil(
        Number(dicomModel?.[DicomTag.BITS_ALLOCATED]?.Value?.[0] ?? NaN) /
        8) /
      Number(
        dicomModel?.[DicomTag.LOSSY_IMAGE_COMPRESSION_RATIO]?.Value?.[0] ??
        1 /* uncompressed */));

    return {
      properties: [{
        offset,
        frames,
        instanceUid,
      }],
      width,
      height,
      pixelWidth,
      pixelHeight,
      tileSize,
      zoom: 0,  // unknown at this point
      storedBytes,
      dicomModel,
    };
  }

  private fillServerDownsampledLevels(levelMap: LevelMap): LevelMap {
    const downSampleMultiplier = 2;

    levelMap = JSON.parse(JSON.stringify(levelMap)) as LevelMap;

    const maxLevelMap = levelMap.reduce(
      (prev, current) => (prev.width > current.width) ? prev : current);

    /**
     * Levels that were skipped because they could not be generated through
     * downsampling.
     *
     * Downsampling only allowed from a original level (non
     * downSampleMultiplier)
     */
    const skippedLevels = new Set();

    for (let i = 1; i < levelMap.length; i++) {
      const currentLevel = levelMap[i];
      const expectedLevel = Math.round(
        Math.log2(currentLevel.pixelWidth ?? currentLevel.width) -
        Math.log2(maxLevelMap.pixelWidth ?? maxLevelMap.width));

      // Validate and add missing level.
      if (i + skippedLevels.size !== expectedLevel) {
        const prevLevel: Level = JSON.parse(JSON.stringify(levelMap[i - 1])) as Level;
        // Skip adding missing level when previous level is downsampled.
        if (prevLevel.downSampleMultiplier) {
          skippedLevels.add(i);
          continue;
        }

        // Generate downsampled level for the current index based on prev level
        const downScaledLevel =
          this.downScaleLevel(prevLevel, downSampleMultiplier);
        downScaledLevel.downSampleMultiplier = downSampleMultiplier;

        // Insert downScaledLevel middle of prevLevel and currentLevel
        // Example-post-splice:
        // levelMap = [..., prevLevel, downScaledLevel, currentLevel, ...]
        levelMap.splice(i, 0, downScaledLevel);
      }
    }

    const lastLevel = levelMap[levelMap.length - 1];
    if ((lastLevel.width > MIN_ZOOM_LEVEL_SIZE ||
      lastLevel.height > MIN_ZOOM_LEVEL_SIZE) &&
      !lastLevel.downSampleMultiplier) {
      // Generate downsampled level for the current index based on prev level
      const downScaledLevel =
        this.downScaleLevel(lastLevel, downSampleMultiplier);
      downScaledLevel.downSampleMultiplier = downSampleMultiplier;
      levelMap.push(downScaledLevel);
    }

    return levelMap;
  }

  private downScaleLevel(level: Level, scaleRatio: number, isFlatImage = false):
    Level {
    level = JSON.parse(JSON.stringify(level)) as Level;

    level.width = Math.floor(level.width / scaleRatio);
    level.height = Math.floor(level.height / scaleRatio);

    level.pixelWidth =
      level.pixelWidth && level.pixelWidth * scaleRatio;
    level.pixelHeight =
      level.pixelHeight && level.pixelHeight * scaleRatio;
    if (isFlatImage) {
      level.tileSize = Math.floor(level.tileSize / scaleRatio);
    }
    if (level.storedBytes) {
      level.storedBytes =
        Math.ceil(level.storedBytes / scaleRatio / scaleRatio);
    }
    return level;
  }

  private fillZoom(sortedLevelMap: LevelMap): LevelMap {
    // The scale change of the each level relative to the largest. Use pixel
    // spacing if available since image width is an integer which introduces
    // numerical errors.
    const maxLevelMap = sortedLevelMap[0];
    return sortedLevelMap.map((level) => {
      level.zoom = maxLevelMap.pixelWidth && level.pixelWidth ?
        maxLevelMap.pixelWidth / level.pixelWidth :
        level.width / maxLevelMap.width;
      // Reduce numerical errors.
      level.zoom = Number.parseFloat(level.zoom.toPrecision(6));
      return level;
    });
  }

  private computeSlideInfo(
    id: string, levelMap: LevelMap, associatedImages: AssociatedImage[],
    slideName: string, isFlatImage: boolean,
    sopClassUid: SopClassUid): SlideInfo {
    const maxLevelMap = levelMap.reduce(
      (prev, current) => (prev.width > current.width) ? prev : current);
    const minLevelMap = levelMap.reduce(
      (prev, current) => (prev.width < current.width) ? prev : current);

    const minZoom = minLevelMap.zoom;

    return {
      channelCount: NUM_COLOR_CHANNELS_WITHOUT_TRANSPARENCY,
      size: {
        x: maxLevelMap.width,
        y: maxLevelMap.height,
      },
      numZoomLevels: levelMap.length,
      minZoom,
      levelMap,
      associatedImages,
      slideName,
      isFlatImage,
      sopClassUid,
    };
  }

  getSlideInfo(id: string): Observable<SlideInfo> {
    // All tags to query. (sorted alphabetically)
    const TAGS = [
      DicomTag.ACCESSION_NUMBER,
      DicomTag.BITS_ALLOCATED,
      DicomTag.COLS,
      DicomTag.CONTAINER_IDENTIFIER,
      DicomTag.IMAGE_TYPE,
      DicomTag.IMAGE_VOLUME_HEIGHT,
      DicomTag.IMAGE_VOLUME_WIDTH,
      DicomTag.INSTANCE_NUMBER,
      DicomTag.LOSSY_IMAGE_COMPRESSION_RATIO,
      DicomTag.NUMBER_OF_FRAMES,
      DicomTag.OFFSET,
      DicomTag.PATIENT_BIRTH_DATE,
      DicomTag.PATIENT_ID,
      DicomTag.PATIENT_NAME,
      DicomTag.PATIENT_SEX,
      DicomTag.PIXEL_SPACING,
      DicomTag.ROWS,
      DicomTag.SAMPLES_PER_PIXEL,
      DicomTag.SOP_CLASS_UID,
      DicomTag.SOP_INSTANCE_UID,
      DicomTag.SPECIMEN_DESCRIPTION_SEQUENCE,
      DicomTag.STUDY_DATE,
      DicomTag.STUDY_ID,
      DicomTag.STUDY_TIME,
      DicomTag.TOTAL_PIXEL_MATRIX_COLUMNS,
      DicomTag.TOTAL_PIXEL_MATRIX_ROWS,
    ];

    return this.dicomwebService.getInstancesMetadata(id, TAGS).pipe(
      map((instances: DicomModel[]) => {

        if (!instances) throw throwError(id + ' not found');
        let levelMap: LevelMap = [];
        const associatedImages: AssociatedImage[] = [];
        let isFlatImage = false;
        const slideName =
          instances[0]?.[DicomTag.CONTAINER_IDENTIFIER]?.Value?.[0] as
          string;

        for (const instance of instances) {
          const classUidStr =
            instance[DicomTag.SOP_CLASS_UID]?.Value?.[0];
          const classUid = classUidStr as SopClassUid;
          const instanceUid =
            instance[DicomTag.SOP_INSTANCE_UID]?.Value?.[0] as string;

          // Image type semantic meaning is the 3rd value in the array
          // according to Dicom standard.
          const imageType =
            instance?.[DicomTag.IMAGE_TYPE]?.Value?.[2] as string ??
            'unknown';
          const isSecondaryCapture =
            classUid === SopClassUid.TILED_SECONDARY_CAPTURE;
          if (isSecondaryCapture ||
            (classUid === SopClassUid.TILED_MICROSCOPE &&
              imageType !== IMAGE_TYPE_SLIDE_PYRAMID_VALUE)) {
            // Associated images, which are not part of the pyramid. Those
            // include thumbnails, macro images and label images. Other ML
            // results can fall in this category.
            const height = Number(
              instance?.[DicomTag.ROWS]?.Value?.[0] ?? undefined);
            const width = Number(
              instance?.[DicomTag.COLS]?.Value?.[0] ?? undefined);

            associatedImages.push({
              width,
              height,
              instanceUid,
              type: imageType,
              isSecondaryCapture,
            });
            continue;
          }

          isFlatImage =
            classUid === SopClassUid.VL_MICROSCOPIC_IMAGE_STORAGE ||
            classUid ===
            SopClassUid
              .VL_SLIDE_COORDINATES_MICROSCOPIC_IMAGE_STORAGE;
          if (isFlatImage) {
            let flatImageLevelMap: LevelMap = [
              this.computeLevel(instance, isFlatImage),
            ];
            flatImageLevelMap = this.fillZoom(flatImageLevelMap);

            return this.computeSlideInfo(
              id, flatImageLevelMap, associatedImages, slideName,
              isFlatImage, classUid);
          }
          if (classUid !== SopClassUid.TILED_MICROSCOPE) {
            // Unsupported type.
            continue;
          }

          const levelInstance = this.computeLevel(instance, isFlatImage);
          // Sort properties
          levelInstance.properties.sort((a, b) => {
            return a.offset - b.offset;
          });

          levelMap.push(levelInstance);
        }

        // Sort and remove empty enteries.
        levelMap.sort((a, b) => {
          const apixel = (a.pixelWidth ?? 0);
          const bpixel = (b.pixelWidth ?? 0);
          return apixel - bpixel;
        });
        levelMap.splice(levelMap.filter(e => e).length);
        // Find and remove duplicate levels.
        for (let i = 0; i < levelMap.length - 1; ++i) {
          if (levelMap[i].width === levelMap[i + 1].width) {
            levelMap.splice(i + 1, 1);
            ++i;
          }
        }

        if (this.ENABLE_SERVER_INTERPOLATION) {
          levelMap = this.fillServerDownsampledLevels(levelMap);
        }

        levelMap = this.fillZoom(levelMap);
        levelMap.sort((a, b) => {
          const apixel = (a.pixelWidth ?? 0);
          const bpixel = (b.pixelWidth ?? 0);
          return apixel - bpixel;
        });

        return this.computeSlideInfo(
          id, levelMap, associatedImages, slideName, isFlatImage,
          SopClassUid.TILED_MICROSCOPE);
      }),
      catchError(
        err => throwError(
          typeof err === 'string' ?
            err + ' when handling slide metadata' :
            err)));
  }

  getSlideExtraMetadata(id: string): Observable<SlideExtraMetadata> {
    const deided = isSlideDeId(id);
    return this.dicomwebService.getExtraMetadata(id).pipe(
      map((rawValues: DicomModel[]) => {
        const rawValue = rawValues[0];
        return {
          patientName: formatName(
            rawValue[DicomTag.PATIENT_NAME]?.Value?.[0] as
            PersonName) ||
            'Unknown Patient Name',
          patientId: rawValue[DicomTag.PATIENT_ID]?.Value?.[0] as string,
          caseId: rawValue[DicomTag.ACCESSION_NUMBER]?.Value?.[0] as
            string ??
            'Unknown Case ID',
          ...deided && { deided },
          rawValue,
        };
      }),
      catchError(
        err => throwError(
          typeof err === 'string' ?
            err + ' when handling slide extra metadata' :
            err)));
  }

  getSlidesForCase(caseId: string): Observable<SlideDescriptor[]> {
    return this.dicomwebService.getInstancesByStudy(caseId).pipe(
      map(instances => {
        if (!instances) {
          return [];
        }

        const slides = new Map<string, SlideDescriptor>();
        for (const instance of instances) {
          const seriesInstanceUID =
            instance[DicomTag.SERIES_INSTANCE_UID]?.Value?.[0] as
            string;
          if (!slides.has(seriesInstanceUID)) {
            const slide: SlideDescriptor = {
              id: `${caseId}/series/${seriesInstanceUID}`,
              name: instance[DicomTag.CONTAINER_IDENTIFIER]?.Value?.[0] as
                string,
            };
            slides.set(seriesInstanceUID, slide);
          }
        }
        const slideDescriptors = Array.from(slides.values()).sort((a, b) => {
          return (a.name ?? '').localeCompare(b.name ?? '');
        });
        this.slideDescriptors$.next(slideDescriptors);
        return slideDescriptors;
      }));
  }

  getImageTile(volumeName: string, slideInfo: SlideInfo, imageTile: ImageTile):
    Observable<string | null> {
    return this.dicomwebService.getImageTile(
      volumeName, slideInfo, imageTile,
      this.ENABLE_SERVER_INTERPOLATION ? IccProfileType.SRGB :
        IccProfileType.NONE);
  }

  getImageTileBinary(volumeName: string, imageTile: ImageTile, channel: number):
    Observable<Uint32Array | null> {
    return EMPTY;
  }

  getImageSlideLabel(volumeName: string, slideInfo: SlideInfo):
    Observable<string | null> {
    // Fetch image depending on how it was stored.
    const associatedImage = slideInfo.associatedImages.find(
      ({ type }) => type.toUpperCase() === IMAGE_TYPE_SLIDE_LABEL_VALUE);
    if (associatedImage) {
      return (associatedImage.isSecondaryCapture ?
        this.dicomwebService.getImageSecondaryCapture(
          volumeName, associatedImage.instanceUid) :
        this.dicomwebService.getEncodedImageTile(
          volumeName, associatedImage.instanceUid,
          1 /* frame num */))
        .pipe(
          map(data => data ? `data:image/jpeg;base64,${data}` : data),
          catchError(err => ''));
    }
    return EMPTY;
  }
}