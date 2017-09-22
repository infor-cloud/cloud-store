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

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.concurrent.Callable;

class S3MultipartCopyFactory
{
  private AmazonS3Client client;
  private ListeningExecutorService executor;

  public S3MultipartCopyFactory(AmazonS3Client client, ListeningExecutorService executor)
  {
    if (client == null)
    {
      throw new IllegalArgumentException("non-null client is required");
    }
    if (executor == null)
    {
      throw new IllegalArgumentException("non-null executor is required");
    }

    this.client = client;
    this.executor = executor;
  }

  public ListenableFuture<Copy> startCopy(String sourceBucketName,
                                          String sourceObjectKey,
                                          String destinationBucketName,
                                          String destinationObjectKey,
                                          CopyOptions options)
  {
    return executor.submit(
      new StartCallable(sourceBucketName, sourceObjectKey, destinationBucketName,
        destinationObjectKey, options));
  }

  private class StartCallable implements Callable<Copy>
  {
    private String sourceBucketName;
    private String sourceObjectKey;
    private String destinationBucketName;
    private String destinationObjectKey;
    private CopyOptions options;

    public StartCallable(String sourceBucketName,
                         String sourceObjectKey,
                         String destinationBucketName,
                         String destinationObjectKey,
                         CopyOptions options)
    {
      this.sourceBucketName = sourceBucketName;
      this.sourceObjectKey = sourceObjectKey;
      this.destinationBucketName = destinationBucketName;
      this.destinationObjectKey = destinationObjectKey;
      this.options = options;
    }

    public Copy call()
    throws Exception
    {
      ObjectMetadata metadata = client.getObjectMetadata(sourceBucketName, sourceObjectKey);

      options.getUserMetadata().ifPresent(metadata::setUserMetadata);

      if (metadata.getUserMetaDataOf("s3tool-version") == null)
      {
        long chunkSize = Utils.getDefaultChunkSize(metadata.getContentLength());

        metadata.addUserMetadata("s3tool-version", String.valueOf(Version.CURRENT));
        metadata.addUserMetadata("s3tool-chunk-size", Long.toString(chunkSize));
        metadata.addUserMetadata("s3tool-file-length", Long.toString(metadata.getContentLength()));
      }
      // It seems setting the STORAGE_CLASS metadata header is sufficient
      options.getStorageClass().ifPresent(sc -> metadata.setHeader(Headers.STORAGE_CLASS, sc));

      InitiateMultipartUploadRequest req =
        new InitiateMultipartUploadRequest(destinationBucketName, destinationObjectKey, metadata);
      if (options.getCannedAcl().isPresent())
      {
        req.setCannedACL(S3Client.getCannedAcl(options.getCannedAcl().get()));
      }
      else
      {
        req.setAccessControlList(S3Client.getObjectAcl(client, sourceBucketName, sourceObjectKey));
      }
      // req.setStorageClass(StorageClass.fromValue(storageClass));
      InitiateMultipartUploadResult res = client.initiateMultipartUpload(req);
      return new S3MultipartCopy(client, sourceBucketName, sourceObjectKey, destinationBucketName,
        destinationObjectKey, res.getUploadId(), metadata, executor);
    }
  }
}
