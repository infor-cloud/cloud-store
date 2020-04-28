/*
  Copyright 2020, Infor Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.logicblox.cloudstore;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.InputStream;
import java.util.Map;

/**
 * This interface hides the backend-specific API calls needed to download a single part of a
 * target object ({@code downloadPart}) and run arbitrary actions on the final downloaded object
 * ({@code completeDownload}), like checksum validation.
 * These methods are used by {@link DownloadCommand}.
 */
interface Download
{
  /**
   * Downloads a single part of the target object.
   *
   * @param partNumber The sequence number of this part (0-based). For example, the last part of a
   *                  2-part object would have {@code partNumber} 1.
   * @param start The index of target object's byte where this part starts
   * @param end The index of target object's byte where this parts ends (inclusive)
   * @param opl A listener to keep track of part's download progress
   * @return A future to the input stream from which the downloaded part can be read
   */
  ListenableFuture<InputStream> downloadPart(
    int partNumber, long start, long end, OverallProgressListener opl);

  /**
   * Runs any required actions after all object parts have been downloaded and combined. Examples
   * of such actions: checksum validation, cleanup.
   *
   * @return A future to this download implementation
   */
  ListenableFuture<Download> completeDownload(long fileLength, long chunkSize);

  /**
   * @return The user metadata associated with the target object
   */
  Map<String, String> getMetadata();

  /**
   * @return The content-length (size) of the target object
   */
  long getLength();

  /**
   * @return The ETag of the target object
   */
  String getETag();

  /**
   * @return The path of the target object
   */
  String getObjectKey();

  /**
   * @return The bucket where the target object resides
   */
  String getBucketName();
}
