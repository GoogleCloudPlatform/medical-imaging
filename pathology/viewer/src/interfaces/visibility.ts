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

import {BehaviorSubject, Observable} from 'rxjs';
import { VisibilityDescriptor } from './visibility_descriptor';
/**
 * Class for managing the visibility of a layer - used by slide / imageOverlay
 * models.
 */
export class Visibility {
  private readonly opacitySubject = new BehaviorSubject<number>(100);
  private readonly visibleSubject = new BehaviorSubject<boolean>(true);

  readonly opacity$: Observable<number> = this.opacitySubject.asObservable();
  readonly visible$: Observable<boolean> = this.visibleSubject.asObservable();

  constructor(visible = true, opacity = 100) {
    this.visible = visible;
    this.opacity = opacity;
  }

  initializeFromDescriptor(descriptor: VisibilityDescriptor) {
    if (descriptor.visible !== undefined) {
      this.visible = descriptor.visible;
    }
    if (descriptor.opacity !== undefined) {
      this.opacity = descriptor.opacity;
    }
  }

  get opacity() {
    return this.opacitySubject.getValue();
  }

  set opacity(opacity: number) {
    this.opacitySubject.next(opacity);
  }

  get visible() {
    return this.visibleSubject.getValue();
  }

  set visible(isVisible: boolean) {
    this.visibleSubject.next(isVisible);
  }

  toggleVisible() {
    this.visible = !this.visible;
  }

  serialize() {
    return {
      opacity: this.opacity,
      visible: this.visible,
    };
  }
}

export {type VisibilityDescriptor};