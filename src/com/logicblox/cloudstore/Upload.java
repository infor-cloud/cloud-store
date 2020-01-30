/*
  Copyright 2018, Infor Inc.

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
import java.util.Date;
import java.util.concurrent.Callable;

/**
 * This interface hides the backend-specific API calls needed to upload a single part of a target
 * object ({@code uploadPart}) and run arbitrary actions on the final uploaded object ({@code
 * completeUpload}), like parts composition or checksum validation.
 * These methods are used by {@link UploadCommand}.
 */
interface Upload
{
  /**
   * Uploads a part of the source file to the target object and does checksum validation after
   * the (part) upload is complete. Each backend supports different ways to upload a file
   * in multiple parts.
   *
   * @param partNumber The sequence number of this part (0-based). For example, the last part of
   *                   a 2-part object would have {@code partNumber} 1.
   * @param partSize The number of bytes this part consists of
   * @param streamCallable The input stream from which the part will be read
   * @param opl A listener to keep track of part's upload progress
   * @return
   */
  ListenableFuture<Void> uploadPart(
    int partNumber, long partSize, Callable<InputStream> streamCallable,
    OverallProgressListener opl);

  /**
   * Runs any required actions after all object parts have been uploaded. Examples of such actions:
   * part composition, checksum validation, cleanup.
   *
   * @return A future to the ETag of the uploaded object
   */
  ListenableFuture<String> completeUpload();

  /**
   * Aborts an ongoing upload. Usually, it's called when a failure occurred during the upload
   * operation and we want to revert any state created on the storage service, for example
   * objects for temporary parts.
   */
  ListenableFuture<Void> abort();

  /**
   * @return The bucket where the target object resides
   */
  String getBucketName();

  /**
   * @return The path of the target object
   */
  String getObjectKey();

  /**
   * @return The id of this upload operation. This id might or might not be required by the storage
   * service.
   */
  String getId();

  /**
   * @return The date this upload operation was initiated. This date might or might not be
   * required by the storage service.
   */
  Date getInitiationDate();
}
