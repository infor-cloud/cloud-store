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
  private AmazonS3 _client;
  private ListeningExecutorService _executor;

  public S3MultipartUploadFactory(AmazonS3 client, ListeningExecutorService executor)
  {
    if(client == null)
    {
      throw new IllegalArgumentException("non-null client is required");
    }
    if(executor == null)
    {
      throw new IllegalArgumentException("non-null executor is required");
    }

    _client = client;
    _executor = executor;
  }

  public ListenableFuture<Upload> startUpload(
    String bucketName, String key, Map<String, String> meta, UploadOptions options)
  {
    return _executor.submit(new StartCallable(bucketName, key, meta, options));
  }

  private class StartCallable
    implements Callable<Upload>
  {
    private String _objectKey;
    private String _bucketName;
    private Map<String, String> _meta;
    private UploadOptions _options;

    public StartCallable(
      String bucketName, String objectKey, Map<String, String> meta, UploadOptions options)
    {
      _bucketName = bucketName;
      _objectKey = objectKey;
      _meta = meta;
      _options = options;
    }

    public Upload call()
      throws Exception
    {
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setUserMetadata(_meta);
      InitiateMultipartUploadRequest req = new InitiateMultipartUploadRequest(_bucketName,
        _objectKey,
        metadata);
      req.setCannedACL(S3Client.getCannedAcl(_options.getCannedAcl()));

      InitiateMultipartUploadResult res = _client.initiateMultipartUpload(req);
      return new S3MultipartUpload(_client, _bucketName, _objectKey, res.getUploadId(), new Date(),
        _executor, _options);
    }
  }
}
