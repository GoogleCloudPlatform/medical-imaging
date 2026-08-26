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

/** Generates UUIDs for DICOM Annotations. */

import { LogService } from './log.service';

/** UID prefix used in DPAS. */
export const DPAS_UID_PREFIX = '1.3.6.1.4.1.11129.5.7';
const MAX_UID_COUNTER_VALUE = 999;
const MAX_LENGTH_OF_DICOM_UID = 64;
const TEST_TEXT_FOR_NON_NUMBER_CHAR_REGEX = '.*[^0-9].*';

/** Error class for InvalidUiDPrefix. */
export class InvalidUidPrefixError extends Error { }

/** Generates UUIDs for DICOM Annotations. */
export class UuidGenerator {
  private readonly dicomGuidPrefix;
  private counter;
  private randFraction = '';
  private lastTimeStrLen = 0;


  constructor(
    dicomGuidPrefix: string, private readonly logService: LogService) {
    if (this.validatePrefix(dicomGuidPrefix)) {
      this.dicomGuidPrefix = dicomGuidPrefix;
    }
    // Initialize counter as random value between 1 - 999.
    this.counter = Math.floor(Math.random() * 999) + 1;
  }

  /**
   * Logs and throws an error.
   * @param {Error} error
   * Error to throw.
   * @return {void}
   */
  logAndThrowError(error: Error) {
    this.logService.error(error);
    throw error;
  }

  /**
   * Tests that a UID block is correctly formatted.
   * @param {string} block
   * UID block to check format of.
   * @return {boolean}
   * Returns true if the uid block is correctly formatted.
   */
  isUidBlockCorrectlyFormatted(block: string): boolean {
    if (!block) {
      return false;
    }
    if (block.match(TEST_TEXT_FOR_NON_NUMBER_CHAR_REGEX)) {
      return false;
    }
    const firstCharValue = block.charCodeAt(0);
    if (block.length === 1 && firstCharValue < '0'.charCodeAt(0)) {
      return false;
    } else if (block.length > 1 && firstCharValue < '1'.charCodeAt(0)) {
      return false;
    }
    return true;
  }

  /**
   * Validates the customer defined UID prefix.
   * @param {string} prefix
   * UID prefix to validate.
   * @return {string}
   * Returns the valid uid prefix.
   */
  validatePrefix(prefix: string): string {
    if (prefix === '') {
      this.logAndThrowError(
        new InvalidUidPrefixError('DICOM UID prefix is undefined.'));
    }

    // Clean UID prefix. Strips whitespace and removes trailing periods from
    // the right side of the input string.
    prefix = prefix.trim().replace(/\.+$/gm, '');

    if (!prefix.startsWith(DPAS_UID_PREFIX)) {
      this.logAndThrowError(
        new InvalidUidPrefixError(`DICOM UID prefix must start with "${DPAS_UID_PREFIX}". The prefix is defined as "${prefix}."`));
    }

    const uidParts = prefix.split('.');
    const baseDpasUidBlockLen = DPAS_UID_PREFIX.split('.').length;
    const customerUidLen = uidParts.length;
    if (baseDpasUidBlockLen + 1 !== customerUidLen &&
      baseDpasUidBlockLen + 2 !== customerUidLen) {
      this.logAndThrowError(new InvalidUidPrefixError(
        `DICOM UID suffix must be defined with 1 or 2 sub-domains of "${DPAS_UID_PREFIX}"`));
    }
    for (let uidBlockIndex = baseDpasUidBlockLen;
      uidBlockIndex < customerUidLen; uidBlockIndex++) {
      const testBlock = uidParts[Number(uidBlockIndex)];
      if (!this.isUidBlockCorrectlyFormatted(testBlock)) {
        this.logAndThrowError(new InvalidUidPrefixError(
          'DICOM UID suffix is incorrectly formatted.'));
      }
      // DPAS Requirement: Block should be 3 digits or less to retain space
      // for other UID components.
      if (!testBlock || testBlock.length > 3) {
        this.logAndThrowError(new InvalidUidPrefixError(
          'DICOM UID suffix must end with a suffix of 3 digits or less.'));
      }
    }
    return prefix;
  }

  /**
   * Returns unix time in milliseconds.
   * @return {string}
   * Returns the time value in milliseconds as a string.
   */
  getTime(): string {
    const time = new Date().getTime().toString();
    if (this.lastTimeStrLen === 0) {
      this.lastTimeStrLen = time.length;
    }
    return time;
  }

  /**
   * Returns the current counter value.
   * @return {string}
   * Returns the current counter value.
   */
  getCounter(): string {
    const counterStr = String(this.counter);
    return counterStr.padStart(
      String(MAX_UID_COUNTER_VALUE).length - counterStr.length, '0');
  }

  /**
   * Initializes the counter value or increments it.
   * @return {string}
   * Returns the updated counter value.
   */
  incrementCounter(): string {
    if (!this.counter) {
      this.counter = Math.floor(Math.random() * MAX_UID_COUNTER_VALUE - 1);
    }
    this.counter++;
    if (this.counter > MAX_UID_COUNTER_VALUE) {
      this.counter = 1;
    }
    return this.getCounter();
  }

  /**
   * Initializes random fraction of guid.
   * @return {string}
   * Returns the current random fraction.
   */
  initRandomFraction(): string {
    const prefix = this.dicomGuidPrefix;
    const time = this.getTime();
    const counter = this.getCounter();
    // 1 = place holder for smallest random part.
    const uidParts = [prefix, '1', time + counter];
    // test if UID is overlength.
    const overLength = uidParts.join('.').length - MAX_LENGTH_OF_DICOM_UID;
    if (overLength === 0) {
      this.randFraction = String(Math.floor(Math.random() * 9));
    } else {
      const randFractionArr = [String(Math.floor(Math.random() * 9) + 1)];
      for (let x = 0; x > overLength; x--) {
        randFractionArr.push(String(Math.floor(Math.random() * 10)));
      }
      this.randFraction = randFractionArr.join('');
    }
    return this.randFraction;
  }

  /**
   * Returns or initializes the random fraction.
   * @return {string}
   * Returns the current random fraction.
   */
  getRandomFraction(): string {
    if (this.randFraction === '') {
      this.randFraction = this.initRandomFraction();
    }
    return this.randFraction;
  }


  /**
   * Updates the current random fraction.
   * @return {string}
   * Returns the updated random fraction.
   */
  updateRandomFraction(): string {
    this.randFraction = this.initRandomFraction();
    return this.randFraction;
  }

  /**
   * Generates a globally unique DICOM GUID for DPAS Annotations.
   * @return {string}
   * Returns generated uuid.
   */
  generateUid(): string {
    this.randFraction = this.getRandomFraction();
    const tme = this.getTime();
    if (tme.length !== this.lastTimeStrLen) {
      this.lastTimeStrLen = tme.length;
      this.updateRandomFraction();
    }
    const counter = this.getCounter();
    const uid =
      [this.dicomGuidPrefix, this.randFraction, tme + counter].join('.');
    if (uid.length > MAX_LENGTH_OF_DICOM_UID) {
      this.logAndThrowError(
        new Error('UID length exceeds max length for DICOM UID.'));
    }
    return uid;
  }
}