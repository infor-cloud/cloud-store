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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.concurrent.Callable;

class S3DownloadFactory
{
  final private DownloadOptions _options;
  final private long _fileLength;
  final private long _chunkSize;
  final private ListeningExecutorService _apiExecutor;
  private final ListeningExecutorService _internalExecutor;
  final private AmazonS3 _client;

  public S3DownloadFactory(DownloadOptions options,
                           long fileLength, long chunkSize,
                           AmazonS3 client,
                           ListeningExecutorService apiExecutor,
                           ListeningExecutorService internalExecutor)
  {
    _options = options;
    _client = client;
    _fileLength = fileLength;
    _chunkSize = chunkSize;
    _apiExecutor = apiExecutor;
    _internalExecutor = internalExecutor;
  }

  ListenableFuture<Download> startDownload()
  {
    return _apiExecutor.submit(new StartCallable());
  }

  private class StartCallable
    implements Callable<Download>
  {
    public Download call()
    {
      GetObjectMetadataRequest metareq = new GetObjectMetadataRequest(_options.getBucketName(),
        _options.getObjectKey(), _options.getVersion().orElse(null));
      ObjectMetadata metadata = _client.getObjectMetadata(metareq);
      return new S3Download(_options, _fileLength, _chunkSize, _client, _apiExecutor,
        _internalExecutor, metadata);
    }
  }
}
