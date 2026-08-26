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

import {BehaviorSubject, defer, forkJoin, Observable, of} from 'rxjs';
import {finalize, tap} from 'rxjs/operators';

/** Simple function for batching an array of items into groups of a size. */
export function batch<T>(items: T[], batchSize: number): T[][] {
  const batches = items.reduce((batches: T[][], id, index) => {
    const batchIndex = Math.floor(index / batchSize);
    if (!batches[batchIndex]) {
      batches[batchIndex] = [];
    }
    batches[batchIndex].push(id);
    return batches;
  }, []);
  return batches;
}

/** Return progress of forkJoin and the final response. */
export function forkJoinWithProgress<T>(
    arrayOfObservables: Array<Observable<T>>) {
  return defer(() => {  // here we go
    let counter = 0;
    const percent$ = new BehaviorSubject<number>(0);
    const modilefiedObservablesList =
        arrayOfObservables.map((item, index) => item.pipe(finalize(() => {
          const percentValue = ++counter * 100 / arrayOfObservables.length;
          percent$.next(percentValue);
        })));

    const finalResult$ = forkJoin(modilefiedObservablesList).pipe(tap(() => {
      percent$.next(100);
      percent$.complete();
    }));
    return of({result: finalResult$, progress: percent$.asObservable()});
  });
}

/** Simple function for parsing a string to a number, with a default value. */
/* tslint:disable:no-any this function takes any type of value */
export function parseNumber(input: any, defaultNumber = 0): number {
  try {
    const n = Number(input);
    return !isNaN(n) ? n : defaultNumber;
  } catch (err: unknown) {
    return defaultNumber;
  }
}

/**
 * Function to generate snippets from a search result text, based on keyword
 * matches.
 */
export function textToSnippet(
    keywords: Set<string>, text: string, snippetSize: number): string {
  const defaultSnippet = text.slice(0, snippetSize);
  if (!keywords.size || !text || !snippetSize) {
    return defaultSnippet;
  }

  // Trim, remove words>snippetSize, escape, clean and join keywords for regexp
  const regexString: string =
      Array.from(keywords)
          .filter((word) => {
            return word.trim().length <= snippetSize;
          })
          .map((word) => {
            return word.trim().replace(/([.?*+^$[\]\\(){}|-])/g, '\\$1');
          })
          .join('|');
  if (!regexString) {
    return defaultSnippet;
  }
  const regex = new RegExp(regexString, 'gi');

  let wordMatchesInSnippet = 0;
  let maxWordMatchesInSnippet = 0;

  // Position of the first character of the best snippet candidate.
  let bestSnippetStart: number|undefined;
  // Position of the last character of the best snippet candidate.
  let bestSnippetEnd: number|undefined;
  // Current length of the window (snippet candidate).
  let winLen = 0;
  let firstWordIndexInCurrentSnippet = 0;

  const matches: RegExpMatchArray[] =
      [...text.matchAll(regex)].filter(({index}) => index);

  for (let i = 0; i < matches.length; i++) {
    const currentMatch = matches[i];
    const firstWordInCurrentSnippet = matches[firstWordIndexInCurrentSnippet];
    const currentMatchSearchTerm = currentMatch[0];

    wordMatchesInSnippet++;
    winLen = currentMatch.index! + currentMatchSearchTerm.length -
        firstWordInCurrentSnippet.index!;

    // Once we breached the max snippet length, complete current candidate.
    if (winLen > snippetSize) {
      // The last word should be discarded as it breached the max snippet
      // length.
      wordMatchesInSnippet--;

      if (wordMatchesInSnippet > maxWordMatchesInSnippet) {
        const prevMatch = matches[i - 1];

        const prevMatchSearchTerm = prevMatch[0];
        maxWordMatchesInSnippet = wordMatchesInSnippet;
        bestSnippetStart = matches[firstWordIndexInCurrentSnippet].index;
        bestSnippetEnd = prevMatch.index! + prevMatchSearchTerm.length;
      }

      const newFirstWordInCurrentSnippet =
          matches[firstWordIndexInCurrentSnippet + 1];
      //  We already have the next word accounted for, so we only need to reduce
      //  the candidate by the length of the 1st word.
      winLen = currentMatch.index! + currentMatchSearchTerm.length -
          newFirstWordInCurrentSnippet.index!;
      firstWordIndexInCurrentSnippet++;
    }
    // If we reached the last match but the maxlength is not
    // yet reached, then we do one last best snippet check
    else if (
        i === matches.length - 1 &&
        wordMatchesInSnippet > maxWordMatchesInSnippet) {
      maxWordMatchesInSnippet = wordMatchesInSnippet;
      bestSnippetStart = matches[firstWordIndexInCurrentSnippet].index;
      bestSnippetEnd = currentMatch.index! + currentMatchSearchTerm.length;
    }
  }
  if (!bestSnippetStart || !bestSnippetEnd) {
    return defaultSnippet;
  }

  // Prefer adding more (70%) to the front for better context.
  const spareLenPrefixPercent = 0.7;
  const snippetSpareLength = snippetSize - (bestSnippetEnd - bestSnippetStart);
  let finalSnippetStart = Math.max(
      0,
      bestSnippetStart -
          Math.round(spareLenPrefixPercent * snippetSpareLength));

  // If we are too close to the end of the overall string, then try to max
  // out chars to the beginning.
  if (finalSnippetStart + snippetSize > text.length) {
    finalSnippetStart = Math.max(0, text.length - snippetSize);
  }

  return text.substring(finalSnippetStart, finalSnippetStart + snippetSize);
}