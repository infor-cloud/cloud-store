package com.logicblox.s3lib;

import java.util.concurrent.Callable;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;

class AmazonDownloadFactory
{
  private ListeningExecutorService executor;
  private AmazonS3 client;

  public AmazonDownloadFactory(AmazonS3 client, ListeningExecutorService
      executor)
  {
    this.client = client;
    this.executor = executor;
  }

  public ListenableFuture<AmazonDownload> startDownload(String bucketName, String key)
  {
    return executor.submit(new StartCallable(bucketName, key));
  }

  private class StartCallable implements Callable<AmazonDownload>
  {
    private String bucketName;
    private String key;

    public StartCallable(String bucketName, String key)
    {
      this.bucketName = bucketName;
      this.key = key;
    }

    public AmazonDownload call()
    {
      ObjectMetadata data = client.getObjectMetadata(bucketName, key);
      return new AmazonDownload(client, key, bucketName, data, executor);
    }
  }
}
