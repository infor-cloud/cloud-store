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
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;

class S3MultipartUploadFactory
{
  final private UploadOptions _options;
  final private AmazonS3 _client;
  final private ListeningExecutorService _executor;
  final private Map<String, String> _meta;

  public S3MultipartUploadFactory(UploadOptions options, AmazonS3 client, ListeningExecutorService
    executor, Map<String, String> meta)
  {
    if(client == null)
    {
      throw new IllegalArgumentException("non-null client is required");
    }
    if(executor == null)
    {
      throw new IllegalArgumentException("non-null executor is required");
    }

    _options = options;
    _client = client;
    _executor = executor;
    _meta = meta;
  }

  ListenableFuture<Upload> startUpload()
  {
    return _executor.submit(new StartCallable());
  }

  private class StartCallable
    implements Callable<Upload>
  {
    public Upload call()
      throws Exception
    {
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setUserMetadata(_meta);
      InitiateMultipartUploadRequest req = new InitiateMultipartUploadRequest(
        _options.getBucketName(), _options.getObjectKey(), metadata);
      req.setCannedACL(S3Client.getCannedAcl(_options.getCannedAcl()));

      InitiateMultipartUploadResult res = _client.initiateMultipartUpload(req);
      return new S3MultipartUpload(_options, _client, _executor, res.getUploadId(), new Date());
    }
  }
}
