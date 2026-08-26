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

import {Injectable} from '@angular/core';
import {BehaviorSubject, map, Observable} from 'rxjs';

function simpleEmailHash(email: string): string {
  let hash = 0;
  for (let i = 0; i < email.length; i++) {
    hash = ((hash << 5) - hash) + email.charCodeAt(i);
    hash = Math.abs(hash);  // Ensure hash is positive
    hash &= 0x7FFFFFFF;     // Keep only the 31 least significant bits
  }
  return String(hash);
}

/**
 * Service for managing the current user.
 */
@Injectable({providedIn: 'root'})
export class UserService {
  private readonly currentUserSubject = new BehaviorSubject<string|null>(null);
  currentUser$: Observable<string|null> =
      this.currentUserSubject.asObservable();

  getCurrentUser$() {
    return this.currentUser$;
  }

  getCurrentUserHash$() {
    return this.currentUser$.pipe(
        map(user => user ? simpleEmailHash(user) : null));
  }

  getCurrentUser() {
    return this.currentUserSubject.getValue();
  }

  setCurrentUser(currentUser: string|null) {
    this.currentUserSubject.next(currentUser);
  }

  /**
   * When no current user exists on the app window object (because we're using
   * ts_devserver) return an empty filter.
   */
  getUsernameFilter(): string[] {
    const currentUser = this.getCurrentUser();
    return currentUser === null ? [] : [currentUser];
  }
}
