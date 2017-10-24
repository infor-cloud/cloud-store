/*
  Copyright 2017, Infor Inc.

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
  final private ListeningExecutorService _executor;
  final private AmazonS3 _client;

  public S3DownloadFactory(DownloadOptions options, AmazonS3 client, ListeningExecutorService
    executor)
  {
    _options = options;
    _client = client;
    _executor = executor;
  }

  ListenableFuture<S3Download> startDownload()
  {
    return _executor.submit(new StartCallable());
  }

  private class StartCallable
    implements Callable<S3Download>
  {
    public S3Download call()
    {
      GetObjectMetadataRequest metareq = new GetObjectMetadataRequest(_options.getBucketName(),
        _options.getObjectKey(), _options.getVersion().orElse(null));
      ObjectMetadata metadata = _client.getObjectMetadata(metareq);
      return new S3Download(_options, _client, _executor, metadata);
    }
  }
}
