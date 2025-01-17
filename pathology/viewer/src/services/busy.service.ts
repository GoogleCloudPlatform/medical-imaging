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

import { BehaviorSubject } from 'rxjs';
import { Injectable } from '@angular/core';

/** An interface for busy states. */
export interface BusyState {
  isBusy: boolean;
}

/** A class representing a BusyState that is busy. */
export class IsBusyState implements BusyState {
  isBusy = true;
  constructor(public reason: string) { }
}

/** A class representing a BusyState that is not busy. */
export class NotBusyState implements BusyState {
  isBusy = false;
}

/**
 * Service class to manage the busy state of the application, which controls
 * whether or not save/load of persisted data can happen.
 */
@Injectable({
  providedIn: 'root'
})
export class BusyService {
  private readonly isBusyStateSubject =
    new BehaviorSubject<BusyState>(new NotBusyState());
  private readonly isBusyState$ = this.isBusyStateSubject.asObservable();

  getIsBusyState$() {
    return this.isBusyState$;
  }

  isBusy() {
    return this.getIsBusyState().isBusy;
  }

  getIsBusyState(): BusyState {
    return this.isBusyStateSubject.getValue();
  }

  setIsBusy(reason: string) {
    this.setIsBusyState(true, reason);
  }

  setIsNotBusy() {
    this.setIsBusyState(false);
  }

  setIsBusyState(isBusy: boolean, reason = '') {
    if (isBusy) {
      this.isBusyStateSubject.next(new IsBusyState(reason));
    } else {
      this.isBusyStateSubject.next(new NotBusyState());
    }
  }
}
