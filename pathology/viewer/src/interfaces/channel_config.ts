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
 * Describes how channels (from source volumes) map to colors.
 */
export declare interface ChannelConfig {
    entries: ChannelConfigEntry[];
    // If true, configuration panel is hidden, and performance is optimized.
    frozen?: boolean;
  }
  
  /**
   * A single channel configuration entry.
   */
  export declare interface ChannelConfigEntry {
    // Scans are split across several slides.
    // Use this field to indicate which slide we're sampling a channel from.
    volumeName: string;
    // which channel to sample?
    channel: number;
    channelCount: number;
    // UINT8
    red: number;
    green: number;
    blue: number;
    weight: number;
    // thresholds (for the raw data).
    min: number;
    max: number;
    // If set to true, don't add data above max threshold.
    clipMax?: boolean;
  }
  
  /**
   * Helper function, initializes empty channel config.
   */
  export function getDefaultChannelConfig(): ChannelConfig {
    return {
      entries: [],
    };
  }