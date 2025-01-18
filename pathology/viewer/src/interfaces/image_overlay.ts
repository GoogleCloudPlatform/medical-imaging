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

import { ImageOverlayDescriptor } from './image_overlay_descriptor';
import {BehaviorSubject} from 'rxjs';
import { parseNumber } from '../utils/common';
import { Visibility } from './visibility';
import { ImageVector } from './image_overlay_geometry';

const ALLOWED_OVERLAY_HOST_NAME = [
];

/**
 * Model class representing an image overlay on a viewport.
 */
export class ImageOverlay {
  visibility: Visibility;
  position = new ImageVector(0, 0);
  size = new ImageVector(0, 0);
  imgElement!: HTMLImageElement;
  modelId?: string;
  modelCategory?: string;

  private readonly isLoadedSubject = new BehaviorSubject(false);
  isLoaded$ = this.isLoadedSubject.asObservable();

  constructor(public imageUrl = '') {
    const visible = true;
    const opacity = 50;
    this.visibility = new Visibility(visible, opacity);
  }

  initializeFromDescriptor(imageOverlay: string|ImageOverlayDescriptor) {
    if (typeof imageOverlay === 'string') {
      this.checkAllowedHost(imageOverlay);
      this.imageUrl = imageOverlay;
    } else {
      const {visibility, position, size, url, modelId, modelCategory} =
          imageOverlay;

      if (visibility) {
        // Only explicitly turn off visibility when set to false.
        const visible = visibility.visible === false ? false : true;
        this.visibility =
            new Visibility(visible, parseNumber(visibility.opacity));
      }

      if (position) {
        this.position =
            new ImageVector(parseNumber(position.x), parseNumber(position.y));
      }

      if (size) {
        this.size = new ImageVector(parseNumber(size.w), parseNumber(size.h));
      }

      if (url) {
        this.checkAllowedHost(url);
        this.imageUrl = url;
      }

      if (modelId) {
        this.modelId = modelId;
      }

      if (modelCategory) {
        this.modelCategory = modelCategory;
      }
    }

    this.imgElement = new Image();
    this.imgElement.onload = () => {
      this.isLoadedSubject.next(true);
    };
    this.imgElement.src = this.imageUrl;
  }

  checkAllowedHost(url: string) {
    const {
      hostname,
    } = new URL(url);
    if (!ALLOWED_OVERLAY_HOST_NAME.includes(hostname) &&
        !url.startsWith('data:image/png;base64')) {
      throw {error: {message: 'Disallowed image overlay hostname', url}};
    }
  }

  setPosition(position: ImageVector) {
    this.position = position;
  }

  setSize(size: ImageVector) {
    this.size = size;
  }

  serialize(): ImageOverlayDescriptor {
    return {
      url: this.imageUrl,
      visibility: this.visibility.serialize(),
      position: {
        x: this.position.x,
        y: this.position.y,
      },
      size: {
        w: this.size.x,
        h: this.size.y,
      },
      modelId: this.modelId,
      modelCategory: this.modelCategory,
    };
  }
}

export {type ImageOverlayDescriptor};