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

import {BehaviorSubject} from 'rxjs';
import {filter, map} from 'rxjs/operators';
import { Visibility } from './visibility';
import { ChannelConfig, ChannelConfigEntry, getDefaultChannelConfig } from './channel_config';
import { ImageRectangle,ImageVector } from './image_overlay_geometry';
import { ImageOverlay } from './image_overlay';
import {SlideDescriptor, SlideExtraMetadata, SlideInfo, SlideMetadata} from './slide_descriptor';

const defaultColorMixes =
    [[200, 0, 0], [0, 200, 0], [0, 0, 200], [100, 100, 0], [0, 100, 100]];

/**
 * Exported here for use throughout the application.
 */
export const TILE_SIZE = 256;

/**
 * If the number of channels differs,
 * channel config behavior is triggered.
 */
export const NUM_COLOR_CHANNELS_WITHOUT_TRANSPARENCY = 3;

/** Class representing a slide of pathology image data. */
export class SlideModel {
  slideInfo!: SlideInfo;
  initialized$ = new BehaviorSubject(false);
  slideMetadata!: SlideMetadata;
  slideExtraMetadata?: SlideExtraMetadata;
  imageOverlays?: ImageOverlay[];
  channelConfig!: ChannelConfig;

  private readonly forceUpdateSubject = new BehaviorSubject(void 0);

  private forceUpdateTimeout?: NodeJS.Timeout;

  loadImage?:
      (rectangle: ImageRectangle, zoomLevel: number) => Promise<HTMLImageElement>;

  slideLabel = new BehaviorSubject("");

  forceUpdate() {
    // Defer + debounce the channel config render.
    // This makes the input UI more responsive.
    if (typeof this.forceUpdateTimeout !== 'undefined') {
      clearTimeout(this.forceUpdateTimeout);
    }
    this.forceUpdateTimeout = setTimeout(() => {
      clearTimeout(this.forceUpdateTimeout);
      this.forceUpdateTimeout = undefined;
      this.forceUpdateSubject.next(void 0);
    });
  }
  readonly forceUpdate$ = this.forceUpdateSubject.asObservable();

  addChannelConfigEntry() {
    const [red, green, blue] =
        defaultColorMixes[this.channelConfig.entries.length];

    const {channelCount} = this;

    const entry: ChannelConfigEntry = {
      volumeName: this.id,
      channel: Math.floor(channelCount / 2),
      channelCount,
      red,
      green,
      blue,
      weight: 25,
      min: 0,
      max: 1000,
    };
    this.channelConfig.entries.push(this.buildReactiveEntry(entry, () => {
      this.forceUpdate();
    }));
    this.forceUpdate();
    return entry;
  }

  deleteChannelConfigEntry(indexToDelete: number) {
    this.channelConfig.entries = this.channelConfig.entries.filter(
        (entry, index) => index !== indexToDelete);
    this.forceUpdate();
  }

  private size = new ImageVector(0, 0);
  private numZoomLevels = 1;

  id!: string;
  name?: string;
  channelCount!: number;
  volumeName!: string;
  visibility = new Visibility();

  initializeFromDescriptor(slideDescriptor: SlideDescriptor) {
    const {id, name, slideInfo, channelConfig, customImageSource} =
        slideDescriptor;
    // Convert id to string, even if URL service parsed param as number.
    this.id = String(id);
    if (slideInfo) {
      this.setSlideInfo(slideInfo);
    }
    if (name) {
      this.name = name;
    }
    this.channelConfig = {
      entries: channelConfig?.entries?.map(
                   entry => this.buildReactiveEntry(
                       entry,
                       () => {
                         this.forceUpdate();
                       })) ??
          [],
    };
    if (channelConfig?.frozen) {
      this.channelConfig.frozen = true;
    }

    if (customImageSource) {
      const fallbackImage = new Image(TILE_SIZE, TILE_SIZE);
      this.loadImage = (rectangle: ImageRectangle, zoomLevel: number) => {
        const level = this.slideInfo.levelMap[zoomLevel];
        const image = new Image(level.tileSize, level.tileSize);

        // Only allow integer values. Scale to appropriate value.
        const corner = rectangle.corner.scale(level.zoom).round();

        const sourcePath = `${zoomLevel}/${corner.x}_${corner.y}`;
        if (this.sourceToTile.has(sourcePath)) {
          return this.sourceToTile.get(sourcePath)!;
        }

        const sourceBase = 'https://storage.cloud.google.com';

        // customImageSource is of format {cloudBucket}/{folderName}.
        const source = `${sourceBase}/${customImageSource}/${sourcePath}.png`;
        const promise = new Promise<HTMLImageElement>(resolve => {
          image.addEventListener('load', () => {
            this.sourceToTile.set(source, Promise.resolve(image));
            resolve(image);
          });
          image.addEventListener('error', () => {
            this.sourceToTile.set(source, Promise.resolve(fallbackImage));
            resolve(fallbackImage);
          });
        });

        image.src = source;
        return promise;
      };
    }
  }

  private readonly sourceToTile = new Map<string, Promise<HTMLImageElement>>();

  buildReactiveEntry(entry: ChannelConfigEntry, forceUpdate: Function):
      ChannelConfigEntry {
    const reactiveEntry: ChannelConfigEntry = {
      get volumeName() {
        return entry.volumeName;
      },
      set volumeName(name: string) {
        entry.volumeName = name;
        forceUpdate();
      },
      get channel() {
        return entry.channel;
      },
      set channel(channel: number) {
        entry.channel = channel;
        forceUpdate();
      },
      get channelCount() {
        return entry.channelCount;
      },
      set channelCount(channelCount: number) {
        entry.channelCount = channelCount;
        forceUpdate();
      },
      get red() {
        return entry.red ?? 0;
      },
      set red(red: number) {
        entry.red = red;
      },
      get green() {
        return entry.green ?? 0;
      },
      set green(green: number) {
        entry.green = green;
      },
      get blue() {
        return entry.blue ?? 0;
      },
      set blue(blue: number) {
        entry.blue = blue;
        // In practice, we'll always update all three color attributes
        // when we update one (thanks to the HTML color input).
        // Only need to call forceUpdate once blue is set.
        // This lets us avoid two renders that won't be seen.
        forceUpdate();
      },
      get weight() {
        return entry.weight ?? 1;
      },
      set weight(weight: number) {
        entry.weight = weight;
        forceUpdate();
      },
      get min() {
        return entry.min ?? 0;
      },
      set min(min: number) {
        entry.min = min;
        forceUpdate();
      },
      get max() {
        return entry.max ?? 1000;
      },
      set max(max: number) {
        entry.max = max;
        forceUpdate();
      },
      get clipMax() {
        return entry.clipMax;
      },
      set clipMax(clipMax: boolean|undefined) {
        // To keep the config from taking up *even more* URL space,
        // only include clipMax if it's been set to true.
        if (!clipMax) {
          // toggle off
          delete entry.clipMax;
        } else {
          // toggle on
          entry.clipMax = true;
        }
        // Manually force update:
        forceUpdate();
      }
    };

    return reactiveEntry;
  }

  getName() {
    return this.name ? this.name : "";
  }

  getSlideInfo() {
    return this.slideInfo;
  }

  setSlideInfo(slideInfo: SlideInfo) {
    this.slideInfo = slideInfo;
    this.size = new ImageVector(slideInfo.size.x, slideInfo.size.y);
    this.numZoomLevels = slideInfo.numZoomLevels;
    this.channelCount = slideInfo.channelCount;
    this.name = slideInfo.slideName;
    if (this.channelCount !== 3 && !this.channelConfig) {
      this.channelConfig = getDefaultChannelConfig();
      // Add one config to start off.
      this.addChannelConfigEntry();
    }
    this.initialized$.next(true);
  }

  setSlideMetadata(slideMetadata: SlideMetadata) {
    this.slideMetadata = slideMetadata;
    this.volumeName = slideMetadata.volumeName || '';
  }

  getSize() {
    return this.size;
  }

  getNumZoomLevels() {
    return this.numZoomLevels;
  }

  waitInitialized$() {
    return this.initialized$.pipe(filter(isInitialized => isInitialized),
        map(() => this));
  }

  // Get the ratio between the lowest size image to the full size image.
  getMinZoom() {
    return this.slideInfo.minZoom;
  }

  getClosestSmallerZoomLevel(zoom: number) {
    const consideredLevel =
        this.slideInfo.levelMap?.filter(level => level.zoom < zoom) ?? [];
    return consideredLevel.length > 0 ?
        consideredLevel
            .reduce(
                (prev, curr) =>
                    Math.abs(curr.zoom - zoom) < Math.abs(prev.zoom - zoom) ?
                    curr :
                    prev)
            .zoom :
        // Minzoom as default value to avoid empty array edge case.
        this.slideInfo.minZoom;
  }

  getChannelCount() {
    return this.channelCount;
  }

  serialize(): SlideDescriptor {
    const base: SlideDescriptor = {
      id: this.id,
      name: this.name,
      slideInfo: this.getSlideInfo(),
    };

    if (this.channelConfig) {
      base.channelConfig = this.channelConfig;
    }

    return base;
  }
}

export {type SlideDescriptor, type SlideInfo, type SlideMetadata};