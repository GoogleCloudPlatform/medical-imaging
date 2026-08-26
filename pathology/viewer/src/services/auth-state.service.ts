import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class AuthStateService {
  private readonly _reauthInProgress$ = new BehaviorSubject<boolean>(false);
  readonly reauthInProgress$ = this._reauthInProgress$.asObservable();

  get reauthInProgress(): boolean {
    return this._reauthInProgress$.getValue();
  }

  setReauthInProgress(value: boolean): void {
    this._reauthInProgress$.next(value);
  }
}