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

const CHARS = ('ABCDEFGHIJKLMNOPQRSTUVWXYZ' +
               'abcdefghijklmnopqrstuvwxyz' +
               '0123456789')
                  .split('');
const SPECIAL_CHARS = '+/='.split('');

/** Encodes a byte array into a string. */
export function encodeByteArray(input: Uint8Array): string {
  const byteToCharMap = CHARS.concat(SPECIAL_CHARS);

  const output = [];

  for (let i = 0; i < input.length; i += 3) {
    const byte1 = input[i];
    const haveByte2 = i + 1 < input.length;
    const byte2 = haveByte2 ? input[i + 1] : 0;
    const haveByte3 = i + 2 < input.length;
    const byte3 = haveByte3 ? input[i + 2] : 0;

    const outByte1 = byte1 >> 2;
    const outByte2 = ((byte1 & 0x03) << 4) | (byte2 >> 4);
    let outByte3 = ((byte2 & 0x0F) << 2) | (byte3 >> 6);
    let outByte4 = byte3 & 0x3F;

    if (!haveByte3) {
      outByte4 = 64;

      if (!haveByte2) {
        outByte3 = 64;
      }
    }

    output.push(
        byteToCharMap[outByte1], byteToCharMap[outByte2],
        byteToCharMap[outByte3] || '', byteToCharMap[outByte4] || '');
  }
  return output.join('');
}