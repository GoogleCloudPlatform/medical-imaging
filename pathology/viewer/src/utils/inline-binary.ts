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
 * Convert a Base64-encoded string into an array of numbers.
 *
 * @function inlineBinaryToNumberArray
 *
 * @param typedArrayCtor - The typed array constructor to use for decoding the
 *     binary data.
 * @param base64Str - A Base64-encoded string representing binary data.
 * @param [isLittleEndian=true] - A boolean indicating whether the
 *     binary data is in little-endian format.
 *
 * @returns An array of numbers decoded from the binary data.
 *
 * @throws Will throw an error if the byte count is invalid for the desired
 *     output format.
 *
 * @description
 * This function first decodes the Base64 string into a byte array. Then, it
 * partitions the byte array based on the byte size of the desired number format
 * specified by `typedArrayCtor`. Subsequent conversion to actual number values
 * is handled through a typed array view on the byte data, and the final number
 * values are stored in an output array. The function returns this array of
 * decoded numbers.
 * https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_F.2.7.html
 */
export function inlineBinaryToNumberArray(
    typedArrayCtor: Int8ArrayConstructor | Int16ArrayConstructor |
        Int32ArrayConstructor | Uint8ArrayConstructor | Uint16ArrayConstructor |
        Uint32ArrayConstructor | Float32ArrayConstructor | Float64ArrayConstructor,
    base64Str: string, isLittleEndian = true): number[] {
    const bytes: Uint8Array =
        Uint8Array.from(atob(base64Str), c => c.charCodeAt(0));

    const bytesPerNumber: number = typedArrayCtor.BYTES_PER_ELEMENT;
    const byteArrayLength = bytes.length / bytesPerNumber;

    const outputArray: number[] = new Array(byteArrayLength);

    if ((bytes.length % bytesPerNumber) !== 0) {
        throw new Error(
            `Invalid byte count for ${bytesPerNumber * 8}-bit float data.`);
    }

    for (let i = 0; i < bytes.length; i += bytesPerNumber) {
        const byteArray: Uint8Array = isLittleEndian ?
            bytes.slice(i, i + bytesPerNumber) :
            bytes.slice(i, i + bytesPerNumber).reverse();

        outputArray[i / bytesPerNumber] = (new typedArrayCtor(byteArray.buffer))[0];
    }

    return outputArray;
}

/**
 * Convert an array of numbers into a Base64-encoded string.
 *
 * @function numberArrayToInlineBinary
 *
 * @param typedArrayCtor - The typed array constructor to use for encoding the
 *     number data into binary format.
 * @param numberArray - An array of numbers to be encoded into binary
 *     data.
 * @param [isLittleEndian=true] - A boolean indicating whether the
 *     binary data should be in little-endian format.
 *
 * @returns A Base64-encoded string representing the binary data
 *     derived from the input number array.
 *
 * @description
 * This function encodes an array of numbers into binary data using the
 * specified typed array constructor `typedArrayCtor`, which determines the
 * format of the binary data (i.e., the number of bytes per number). The binary
 * data is then converted into a string, with each character of the string
 * representing a byte of the binary data. Finally, this string is encoded as a
 * Base64 string and returned.
 * https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_F.2.7.html
 */
export function numberArrayToInlineBinary(
    typedArrayCtor: Int8ArrayConstructor | Int16ArrayConstructor |
        Int32ArrayConstructor | Uint8ArrayConstructor | Uint16ArrayConstructor |
        Uint32ArrayConstructor | Float32ArrayConstructor | Float64ArrayConstructor,
    numberArray: number[], isLittleEndian = true): string {
    // Get the number of bytes per number based on the typed array constructor.
    const bytesPerNumber: number = typedArrayCtor.BYTES_PER_ELEMENT;

    // Create a buffer to hold the binary data.
    const buffer = new ArrayBuffer(numberArray.length * bytesPerNumber);

    // Create a typed array view on the buffer.
    const typedArray = new typedArrayCtor(buffer);

    // Copy the numbers into the typed array.
    typedArray.set(numberArray);

    // Create a view on the buffer as an array of bytes.
    const byteArray = new Uint8Array(buffer);

    // Convert the byte data to a Base64-encoded string.
    // If the system is little endian but the desired output is big endian,
    // reverse the bytes.
    let binaryString = '';
    for (let i = 0; i < byteArray.length; i += bytesPerNumber) {
        let bytesForNumber = byteArray.slice(i, i + bytesPerNumber);
        if (!isLittleEndian) {
            bytesForNumber = bytesForNumber.reverse();
        }
        binaryString += String.fromCharCode(...bytesForNumber);
    }

    return btoa(binaryString);
}

/**
 * Converts VR (Value Representation) to corresponding TypedArray constructor.
 *
 * DICOM (Digital Imaging and Communications in Medicine) uses a set of VR
 * (Value Representations) to represent different types of data elements.
 * Each VR has a corresponding JavaScript TypedArray constructor that is used
 * to handle the data in JavaScript in a type-appropriate manner.
 * This function, given a VR string, returns the corresponding TypedArray
 * constructor.
 *
 * Refer to DICOM documentation for more about VR:
 * {@link https://dicom.nema.org/dicom/2013/output/chtml/part05/sect_6.2.html}
 *
 * @param vr - A string representing the DICOM VR.
 *
 * @returns The corresponding TypedArray
 * constructor for the given VR.
 *
 * @throws Will throw an error if the VR is not recognized.
 *
 * @example
 * new (vrToType('OB'))[1,2,3];  // Returns Uint8Array with values [1,2,3]
 */
export function vrToType(vr: string): Int8ArrayConstructor |
    Int16ArrayConstructor | Int32ArrayConstructor | Uint8ArrayConstructor |
    Uint16ArrayConstructor | Uint32ArrayConstructor | Float32ArrayConstructor |
    Float64ArrayConstructor {
    switch (vr) {
        case 'OB':
            return Uint8Array;
        case 'OW':
            return Uint16Array;
        case 'OL':
            return Uint32Array;
        case 'OF':
            return Float32Array;
        case 'OD':
            return Float64Array;
        default:
            throw new Error('Unknown annotation format');
    }
}