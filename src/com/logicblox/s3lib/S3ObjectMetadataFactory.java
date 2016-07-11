package com.logicblox.s3lib;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.concurrent.Callable;

class S3ObjectMetadataFactory
{
  private ListeningExecutorService executor;
  private AmazonS3 client;

  public S3ObjectMetadataFactory(AmazonS3 client,
                                 ListeningExecutorService executor)
  {
    this.client = client;
    this.executor = executor;
  }

  public ListenableFuture<S3ObjectMetadata> create(String bucketName,
                                                   String key,
                                                   String version)
  {
    return executor.submit(new S3ObjectMetadataCallable(bucketName, key, version));
  }

  private class S3ObjectMetadataCallable implements Callable<S3ObjectMetadata>
  {
    private String bucketName;
    private String key;
    private String version;

    public S3ObjectMetadataCallable(String bucketName, String key, String version)
    {
      this.bucketName = bucketName;
      this.key = key;
      this.version = version;
    }

    public S3ObjectMetadata call()
    {
      GetObjectMetadataRequest req;
      if (version == null)
      {
        req = new GetObjectMetadataRequest(bucketName, key);
      }
      else
      {
        req = new GetObjectMetadataRequest(bucketName, key, version);
      }
      ObjectMetadata metadata = client.getObjectMetadata(req);
      return new S3ObjectMetadata(client, key, bucketName, version, metadata,
        executor);
    }
  }
}
