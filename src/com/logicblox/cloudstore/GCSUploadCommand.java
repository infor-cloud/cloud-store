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

import java.util.Map;

class GCSUploadCommand
  extends UploadCommand
{
  // There seems to be a problem chunk sizes larger than 10m bytes when uploading
  // large (i.e. 200g) encrypted files to GCS.  Each uploaded chunk returns a
  // CRC32c value that doesn't match what we compute locally.  If we limit chunk
  // sizes to 10m bytes, we don't see this problem.  Note that this may result in
  // more than 10000 chunks for large files which is a problem for AWS but doesn't
  // seem to be for GCS.
  private static final long MAX_ALLOWED_CHUNK_SIZE = 10000000;

  public GCSUploadCommand(UploadOptions options)
  {
    super(options);
  }

  @Override
  public void setChunkSize(long chunkSize)
  {
    if(chunkSize > MAX_ALLOWED_CHUNK_SIZE)
      this.chunkSize = MAX_ALLOWED_CHUNK_SIZE;
    else
      this.chunkSize = chunkSize;
  }

  @Override
  protected ListenableFuture<Upload> initiateUpload(Map<String, String> metadata)
  {
    GCSParallelUploadFactory factory = new GCSParallelUploadFactory(_options, getGCSClient(),
      _client.getApiExecutor(), metadata);
    return factory.startUpload();
  }
}
