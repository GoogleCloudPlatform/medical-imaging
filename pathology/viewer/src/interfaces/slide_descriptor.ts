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

// Ensure that all serialized data interfaces use "declare interface" to
// preserve the property names during closure compilation.

import { DicomModel, SopClassUid } from './dicom_descriptor';

import { ChannelConfig } from './channel_config';

/**
 * This is the main artifact we compute from a slide's dicom data.
 * Describes the size of each zoom level.
 */
export declare interface LevelMap extends Array<Level> { } { }

/**
 * Describes the size of a pixel.
 */
export declare interface PixelSize {
    width: number;   // mm
    height: number;  // mm
}

/**
 * Describes the properties in a level.
 */
export declare interface Level {
    properties: LevelProperty[];
    width: number;         // pixels
    height: number;        // pixels
    pixelWidth?: number;   // mm
    pixelHeight?: number;  // mm
    tileSize: number;      // pixels
    zoom: number;          // scale from original
    downSampleMultiplier?: number;
    storedBytes?: number;
    dicomModel: DicomModel;
}

/**
 * Describes a level property within a dicom data store.
 */
export declare interface LevelProperty {
    offset: number;
    frames: number;
    instanceUid: string;
}

/**
 * Describes a associated image. An image that is not part of the tissue
 * pyramid but related to the tissue, such as label image, ML or macro.
 */
export declare interface AssociatedImage {
    width?: number;
    height?: number;
    type: string;
    instanceUid: string;
    isSecondaryCapture: boolean;
}

/** Info about the slide returned from dicom store. */
export declare interface SlideInfo {
    channelCount: number;
    isFlatImage?: boolean;
    sopClassUid: SopClassUid;
    size: { x: number; y: number };
    numZoomLevels: number;
    minZoom: number;
    // Contains information exclusive to the DICOM API.
    levelMap: LevelMap;
    associatedImages: AssociatedImage[];
    slideName?: string;
}

/** Info about the slide */
export interface SlideMetadata {
    volumeName: string | null;
    biopsyType: string | null;
    fixationType: string | null;
    nativeMagnification: string | null;
    stain: string | null;
    tissueType: string | null;
    slideRelations?: { sameScanAs: string[] | null } | null;
    laserWavelengths?:
    { laser: number | null; emissionWavelengths: number[] | null; } | null;
}

/** Extra metadata about the slide */
export interface SlideExtraMetadata {
    patientName: string | null;
    patientId: string | null;
    caseId: string | null;
    rawValue: DicomModel;
    deided?: boolean;
}

/**
 * SlideDescriptor represents the serializable form of the SlideModel,
 * which is used to persist state between sessions in session storage or
 * serialize to the url.
 */
export declare interface SlideDescriptor {
    // URL to series.
    id: string;
    name?: string;  // An optional override name for the slide to display instead
    // of id
    slideInfo?: SlideInfo;
    // Used to configure channel weights.
    channelConfig?: ChannelConfig;
    // GCP bucket and folder with tiled images that the viewer will display.
    // Contains folders [0, 1, ...] where 0 is native zoom level.
    // Each of these folders contains tiles `${x}_${y}`
    customImageSource?: string;
}

/**
 * The URL-serializable version of the serialized slide model doesn't contain
 * all fields on the slide model - the most notable absence being SlideInfo,
 * which is fetched from the API. All other properties are optional.
 */
export declare interface UrlSerializedSlide {
    id?: string;
    name?: string;  // An optional override name for the slide to display instead
    // of id
}

/**
 * Represents a specific AI result for a slide.
 **/
export declare interface AISlideResult {
    captureDate: string;
    modelManufacturer: string;
    modelName: string;
    modelVersion: string;
    score: string;
    instanceUid: string;
}

/**
 * Represents a slide in the pathology API.
 */
export interface PathologySlide {
    name?: string;
    scanUniqueId?: string;
    dicomUri?: string;
}