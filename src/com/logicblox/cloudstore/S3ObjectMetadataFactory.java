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

class S3ObjectMetadataFactory
{
  private ListeningExecutorService _executor;
  private AmazonS3 _client;

  public S3ObjectMetadataFactory(AmazonS3 client, ListeningExecutorService executor)
  {
    _client = client;
    _executor = executor;
  }

  public ListenableFuture<S3ObjectMetadata> create(String bucketName, String key, String version)
  {
    return _executor.submit(new S3ObjectMetadataCallable(bucketName, key, version));
  }

  private class S3ObjectMetadataCallable
    implements Callable<S3ObjectMetadata>
  {
    private String _bucketName;
    private String _objectKey;
    private String _version;

    public S3ObjectMetadataCallable(String bucketName, String objectKey, String version)
    {
      _bucketName = bucketName;
      _objectKey = objectKey;
      _version = version;
    }

    public S3ObjectMetadata call()
    {
      GetObjectMetadataRequest req;
      if(_version == null)
      {
        req = new GetObjectMetadataRequest(_bucketName, _objectKey);
      }
      else
      {
        req = new GetObjectMetadataRequest(_bucketName, _objectKey, _version);
      }
      ObjectMetadata metadata = _client.getObjectMetadata(req);
      return new S3ObjectMetadata(_client, _objectKey, _bucketName, _version, metadata, _executor);
    }
  }
}
