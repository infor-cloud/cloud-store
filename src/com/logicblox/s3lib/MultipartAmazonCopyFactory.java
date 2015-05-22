package com.logicblox.s3lib;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.concurrent.Callable;

class MultipartAmazonCopyFactory
{
  private AmazonS3 client;
  private ListeningExecutorService executor;

  public MultipartAmazonCopyFactory(AmazonS3 client,
                                    ListeningExecutorService executor)
  {
    if(client == null)
      throw new IllegalArgumentException("non-null client is required");
    if(executor == null)
      throw new IllegalArgumentException("non-null executor is required");

    this.client = client;
    this.executor = executor;
  }

  public ListenableFuture<Copy> startCopy(String sourceBucketName,
                                          String sourceKey,
                                          String destinationBucketName,
                                          String destinationKey,
                                          String cannedAcl)
  {
    return executor.submit(new StartCallable(sourceBucketName, sourceKey,
        destinationBucketName, destinationKey, cannedAcl));
  }

  private class StartCallable implements Callable<Copy>
  {
    private String sourceBucketName;
    private String sourceKey;
    private String destinationBucketName;
    private String destinationKey;
    private String cannedAcl;

    public StartCallable(String sourceBucketName, String sourceKey, String
        destinationBucketName, String destinationKey, String cannedAcl)
    {
      this.sourceBucketName = sourceBucketName;
      this.sourceKey = sourceKey;
      this.destinationBucketName = destinationBucketName;
      this.destinationKey = destinationKey;
      this.cannedAcl = cannedAcl;
    }

    public Copy call() throws Exception
    {
      ObjectMetadata metadata = client.getObjectMetadata(sourceBucketName,
          sourceKey);

      // TODO(geokollias): If object uploaded by another tool, add
      // s3lib-specific metadata to help future s3lib operations. fileLength
      // and chunkSize should be the same as the ones that *will*
      // be set in Copy.startParts. Maybe re-work that part.

      InitiateMultipartUploadRequest req = new InitiateMultipartUploadRequest
          (destinationBucketName, destinationKey, metadata);
      req.setCannedACL(getCannedAcl(cannedAcl));

      InitiateMultipartUploadResult res = client.initiateMultipartUpload(req);
      return new MultipartAmazonCopy(client, sourceBucketName, sourceKey,
          destinationBucketName, destinationKey, res.getUploadId(), metadata,
          executor);
    }

    private CannedAccessControlList getCannedAcl(String value)
    {
      for (CannedAccessControlList acl : CannedAccessControlList.values())
      {
        if (acl.toString().equals(value))
        {
          return acl;
        }
      }
      return null;
    }
  }
}
