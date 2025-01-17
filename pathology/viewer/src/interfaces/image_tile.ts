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
 * Image Tile information, used within the app to refer a specific section of
 * the large-scale image. This provides a consistent interface between the app
 * internals and the brainmaps API methods which construct the requests to
 * actually fetch the tile data.
 */
export interface ImageTile {
    size: {x: number, y: number};
    corner: {x: number, y: number};
    scale: number;
  }