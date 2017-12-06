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
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.concurrent.Callable;

class S3MultipartCopyFactory
{
  final private CopyOptions _options;
  final private AmazonS3 _client;
  final private ListeningExecutorService _executor;

  public S3MultipartCopyFactory(CopyOptions options, AmazonS3 client, ListeningExecutorService
    executor)
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
  }

  ListenableFuture<Copy> startCopy()
  {
    return _executor.submit(new StartCallable());
  }

  private class StartCallable
    implements Callable<Copy>
  {
    public Copy call()
      throws Exception
    {
      ObjectMetadata metadata = _client.getObjectMetadata(_options.getSourceBucketName(),
        _options.getSourceObjectKey());

      _options.getUserMetadata().ifPresent(metadata::setUserMetadata);

      if(metadata.getUserMetaDataOf("s3tool-version") == null)
      {
        long chunkSize = Utils.getDefaultChunkSize(metadata.getContentLength());

        metadata.addUserMetadata("s3tool-version", String.valueOf(Version.CURRENT));
        metadata.addUserMetadata("s3tool-chunk-size", Long.toString(chunkSize));
        metadata.addUserMetadata("s3tool-file-length", Long.toString(metadata.getContentLength()));
      }
      // It seems setting the STORAGE_CLASS metadata header is sufficient
      _options.getStorageClass().ifPresent(sc -> metadata.setHeader(Headers.STORAGE_CLASS, sc));

      InitiateMultipartUploadRequest req = new InitiateMultipartUploadRequest(
        _options.getDestinationBucketName(), _options.getDestinationObjectKey(), metadata);
      if(_options.getCannedAcl().isPresent())
      {
        req.setCannedACL(S3Client.getCannedAcl(_options.getCannedAcl().get()));
      }
      else
      {
        try
        {
          final AccessControlList objectAcl = S3Client.getObjectAcl(_client,
            _options.getSourceBucketName(), _options.getSourceObjectKey());
          req.setAccessControlList(objectAcl);
        }
        catch(AmazonS3Exception ex)
        {
          if(!ex.getErrorCode().equalsIgnoreCase("NotImplemented"))
          {
            throw ex;
          }
        }
      }
      // req.setStorageClass(StorageClass.fromValue(storageClass));
      InitiateMultipartUploadResult res = _client.initiateMultipartUpload(req);
      return new S3MultipartCopy(_options, _client, _executor, res.getUploadId(), metadata);
    }
  }
}
