package com.logicblox.s3lib;

import java.util.Map;
import java.io.InputStream;
import java.util.concurrent.Callable;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.GetObjectRequest;

class AmazonDownload implements Download
{
  private AmazonS3 client;
  private ListeningExecutorService executor;
  private Map<String,String> meta;
  private String key;
  private String bucketName;
  private long length;

  public AmazonDownload(AmazonS3 client, String key, String bucketName, Map<String,String> meta, long length, ListeningExecutorService executor)
  {
    this.client = client;
    this.key = key;
    this.bucketName = bucketName;
    this.executor = executor;
    this.meta = meta;
    this.length = length;
  }

  public ListenableFuture<InputStream> getPart(long start, long end)
  {
    return executor.submit(new DownloadCallable(start, end));
  }

  public Map<String,String> getMeta()
  {
    return meta;
  }

  public long getLength()
  {
    return length;
  }

  private class DownloadCallable implements Callable<InputStream>
  {
    private long start;
    private long end;

    public DownloadCallable(long start, long end)
    {
      this.start = start;
      this.end = end;
    }

    public InputStream call() throws Exception
    {
      GetObjectRequest req = new GetObjectRequest(bucketName, key);
      req.setRange(start, end);
      return client.getObject(req).getObjectContent();
    }
  }
}