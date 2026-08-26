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

import {UserService} from './user.service';

/**
 * Enum containing possible event messages for LogDataMessage.
 */
export enum EventMessage {
  IMAGE_OVERLAY_VISIBLE_CHANGE = 'IMAGE_OVERLAY_VISIBLE_CHANGE',
  PAN_TOOL_DRAG_END = 'PAN_TOOL_DRAG_END',
  PINS_SERVICE_ADD_PIN = 'PINS_SERVICE_ADD_PIN',
  RULER_TOOL_DRAG_END = 'RULER_TOOL_DRAG_END',
  QUICK_VIEW_TOGGLE_CASE_FORM = 'QUICK_VIEW_TOGGLE_CASE_FORM',
  THUMBNAIL_DRAG_END = 'THUMBNAIL_DRAG_END',
  VIEWPORT_COMPONENT_INITIALIZED = 'VIEWPORT_COMPONENT_INITIALIZED',
  VIEWPORT_SHORTCUT_TRIGGERED = 'VIEWPORT_SHORTCUT_TRIGGERED',
}

/**
 * Structure for analytics log messages, only required parameter is
 * eventMessage.
 */
export declare interface LogDataMessage {
  eventMessage: string;

  // overlay visibility/opacity
  imageOverlayVisible?: boolean;

  // shortcuts
  shortcutName?: string;

  // pin tool
  pinColor?: string;
  pinPositionX?: number;
  pinPositionY?: number;
  pinNotes?: string;

  // ruler tool
  startPositionX?: number;
  startPositionY?: number;
  endPositionX?: number;
  endPositionY?: number;
}

declare interface Message {
  slideId?: string;
  id?: string|null;
  name?: string|null;
  ids?: string[];
  extraAnnotationIds?: string[];
  annotationUsers?: string[];
  extraAnnotationUsers?: string[];
  slideIds?: string[];
  taskId?: string;
  jobId?: string;
  slideMetadata?: unknown;
  slideInfo?: unknown;
  workspaceDescriptor?: unknown;
  annotationsBySlideId?: unknown;
  event: string;
  labels?: unknown;
  error?: unknown;
  errors?: unknown;
  operation?: unknown;
}

type LogEntryTypeMessage = 'admin'|'log'|'warn';
type LogEntryTypeError = 'error';

interface Dated {
  date: string;
}

declare interface LogEntryMessage extends Dated {
  type: LogEntryTypeMessage;
  message: Message;
}

declare interface LogEntryError extends Dated {
  type: LogEntryTypeError;
  message: Error;
}

declare type LogEntry = LogEntryMessage | LogEntryError;

/**
 * Service class to manage logging in the application.
 */
@Injectable({providedIn: 'root'})
export class LogService {
  private readonly logEntries: LogEntry[] = [];

  constructor(protected readonly userService: UserService) {}

  error(error: Error) {
    this.addLogEntryError('error', error);
    console.error(JSON.stringify(error));  // CONSOLE LOG OK
  }
  getLogEntries(): LogEntry[] {
    return this.logEntries;
  }

  downloadLogs() {
    const text = this.getLogText();

    const element = document.createElement('a');
    element.setAttribute(
        'href', 'data:text/plain;charset=utf-8,' + encodeURIComponent(text));
    element.setAttribute('download', `viewer-log-${this.getDateString()}.txt`);
    element.click();
  }

  private addLogEntryError(type: LogEntryTypeError, message: Error) {
    this.logEntries.push({
      type,
      message,
      date: this.getDateString(),
    });
  }

  private getLogText(): string {
    return this.logEntries
        .map(entry => {
          return [
            entry.date, entry.type, JSON.stringify(entry.message, null, 2)
          ].join(',');
        })
        .join('\n');
  }

  private getDateString() {
    return (new Date()).toLocaleString();
  }
}
