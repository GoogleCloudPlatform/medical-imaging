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

/**
 * Most compound microscopes come with interchangeable lenses known as objective
 * lenses. Objective lenses come in various magnification powers, with the most
 * common being 4x, 10x, 40x, and 100x. The following magnifications levels are
 * used to simulate what the pathologists are used to.
 * For example the actual scanning could have been done in 20X but with a high
 * DPI, resulting with a digital zoom that looks like 40X.
 */

const magnificationValue: {[key: string]: number;} = {
    'M_100X': 100,
    'M_40X': 40,
    'M_20X': 20,
    'M_10X': 10,
    'M_5X': 5,
    'M_5X_DIV_2': 5 / 2,
    'M_5X_DIV_4': 5 / 4,
    'M_5X_DIV_8': 5 / 8,
    'M_5X_DIV_16': 5 / 16,
    'M_5X_DIV_32': 5 / 32,
    'M_5X_DIV_64': 5 / 64,
    'M_5X_DIV_128': 5 / 128,
    'M_UNKNOWN': 0,
  };
  
  /**
   * Utility method for converting from magnification string to actual zoom
   * number.
   */
  export function zoomNameToVal(zoomName: string): number {
    if (!magnificationValue.hasOwnProperty(zoomName)) {
      return magnificationValue['M_UNKNOWN'] ?? 0;
    }
    return magnificationValue[zoomName];
  }
  
  /**
   * Utility method for converting from zoom number to magnification string.
   */
  export function zoomValToName(zoomVal: number): string {
    return Object.keys(magnificationValue)
               .find(key => magnificationValue[key] === zoomVal) ||
        'M_UNKNOWN';
  }
  
  
  const standardMagnificationsValues: number[] = [
    100, 80, 40, 20, 10, 5, 5 / 2, 5 / 4, 1, 5 / 8, 5 / 16, 5 / 32, 5 / 64,
    5 / 128, 0
  ];
  
  
  /**
   * Utility method for converting from meters to a standard
   * microscope magnification power. There is not real meaning in coverting pixel
   * spacing to a microscope power. Based on feedback from pathologists we came up
   * with a formula that gives pathologist the right mapping.
   */
  export function metersToMagnification(meters: number): number {
    const pixelSpacing = meters * 1e3;
    return pixelSpacingToMagnification(pixelSpacing);
  }
  
  /**
   * Utility method for converting from millimeter pixel spacing to a standard
   * microscope magnification power. There is not real meaning in coverting pixel
   * spacing to a microscope power. Based on feedback from pathologists we came up
   * with a formula that gives pathologist the right mapping.
   */
  export function pixelSpacingToMagnification(mm: number): number {
    if (mm <= 0) return 0;
    const mag = 0.01 / mm;
    // Find nearest standard value.
    const closestStdMag = standardMagnificationsValues.reduce(
        (prev, curr) =>
            Math.abs(curr - mag) < Math.abs(prev - mag) ? curr : prev);
    return closestStdMag;
  }