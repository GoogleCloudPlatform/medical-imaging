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

/**
 * VisibilityDescriptor represents the serializable form of the
 * ImageOverlay Model, which is used to persist state between sessions in local
 * storage or serialize to the url.
 */
export declare interface VisibilityDescriptor {
    visible: boolean;
    opacity: number;
}

/**
 * The UrlSerializedImageOverlay is a slightly different interface than the
 * general VisibilityDescriptor since all properties are optional. This is
 * because no specific property is guaranteed to be encoded in the URL.
 */
export declare interface UrlSerializedVisibility {
  visible?: string;
  opacity?: string;
}