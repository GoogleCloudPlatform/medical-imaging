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

import {Control} from 'ol/control';

/** Rotation angle for primary (left-click) action: 90 degrees */
const COARSE_ROTATION_DEG = 90;

/**
 * Normalizes rotation to 0-360 degree range to prevent drift.
 */
function normalizeRotation(radians: number): number {
  const twoPi = 2 * Math.PI;
  let normalized = radians % twoPi;
  if (normalized < 0) {
    normalized += twoPi;
  }
  return normalized;
}

/**
 * Converts degrees to radians.
 */
function degreesToRadians(degrees: number): number {
  return (degrees * Math.PI) / 180;
}

/**
 * Custom OpenLayers control for image rotation with coarse (90°) and fine (15°) increments.
 *
 * Usage:
 * - Left-click: Rotate by ±90°
 * - Shift+Left-Click Mouse+Drag: Continuous fine rotation (via DragRotateAndZoom interaction)
 */
export class RotationControl extends Control {
  private readonly coarseRotationRad = degreesToRadians(COARSE_ROTATION_DEG);

  constructor() {
    const element = document.createElement('div');
    element.className = 'ol-rotate-buttons ol-unselectable ol-control';

    const rotateLeftBtn = document.createElement('button');
    rotateLeftBtn.innerHTML = '<span class="material-icons">rotate_left</span>';
    rotateLeftBtn.setAttribute('type', 'button');

    const rotateRightBtn = document.createElement('button');
    rotateRightBtn.innerHTML = '<span class="material-icons">rotate_right</span>';
    rotateRightBtn.setAttribute('type', 'button');

     const tooltipText =
`Click: Rotate 90°
 Shift+Left-Click Mouse +Drag: Fine rotation`;

    rotateLeftBtn.title = tooltipText;
    rotateRightBtn.title = tooltipText;

    element.appendChild(rotateLeftBtn);
    element.appendChild(rotateRightBtn);

    super({element});

    // Left-click handlers (coarse rotation: 90°)
    rotateLeftBtn.addEventListener('click', (e) => {
      e.preventDefault();
      this.rotate(-this.coarseRotationRad);
    });

    rotateRightBtn.addEventListener('click', (e) => {
      e.preventDefault();
      this.rotate(this.coarseRotationRad);
    });
  }

  /**
   * Rotates the map view by the specified angle in radians.
   * Rotation accumulates correctly and is normalized to prevent drift.
   */
  private rotate(angleRadians: number): void {
    const map = this.getMap();
    if (!map) return;

    const view = map.getView();
    const currentRotation = view.getRotation();
    const newRotation = normalizeRotation(currentRotation + angleRadians);

    // Animate the rotation for smooth UX
    view.animate({
      rotation: newRotation,
      duration: 200,
    });
  }
}