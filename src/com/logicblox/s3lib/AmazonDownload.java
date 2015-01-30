package com.logicblox.s3lib;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Callable;

class AmazonDownload
{
  private AmazonS3 client;
  private ListeningExecutorService executor;
  private ObjectMetadata meta;
  private String key;
  private String bucketName;
  private boolean progress;

  public AmazonDownload(
    AmazonS3 client,
    String key,
    String bucketName,
    ObjectMetadata meta,
    ListeningExecutorService executor,
    boolean progress)
  {
    this.client = client;
    this.key = key;
    this.bucketName = bucketName;
    this.executor = executor;
    this.meta = meta;
    this.progress = progress;
  }

  public ListenableFuture<InputStream> getPart(long start, long end)
  {
    return executor.submit(new DownloadCallable(start, end));
  }

  public Map<String,String> getMeta()
  {
    return meta.getUserMetadata();
  }

  public long getLength()
  {
    return meta.getContentLength();
  }

  public String getETag()
  {
    return meta.getETag();
  }

  public String getKey()
  {
    return key;
  }

  public String getBucket()
  {
    return bucketName;
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
      if (progress) {
        req.setGeneralProgressListener(
          new AmazonDownloadProgressListener(
            "range " + start + ":" + end,
            (end - start + 1) / 10,
            end - start + 1));
      }

      return client.getObject(req).getObjectContent();
    }
  }

  private static class AmazonDownloadProgressListener
    extends ProgressListener
    implements com.amazonaws.event.ProgressListener
  {

    public AmazonDownloadProgressListener(String name, long intervalInBytes, long totalSizeInBytes) {
      super(name, "download", intervalInBytes, totalSizeInBytes);
    }

    @Override
    public void progressChanged(ProgressEvent event) {
      progress(event.getBytesTransferred());
    }
  }
}
