package com.logicblox.s3lib;

import com.amazonaws.event.ProgressListener;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Optional;
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
  private String version;

  public AmazonDownload(
    AmazonS3 client,
    String key,
    String bucketName,
    String version,
    ObjectMetadata meta,
    ListeningExecutorService executor)
  {
    this.client = client;
    this.key = key;
    this.bucketName = bucketName;
    this.version = version;
    this.executor = executor;
    this.meta = meta;
  }

  public ListenableFuture<InputStream> getPart(long start, long end)
  {
    return getPart(start, end, Optional.<OverallProgressListener>absent());
  }

  public ListenableFuture<InputStream> getPart(long start, long end,
                                               Optional<OverallProgressListener>
                                                   progressListener)
  {
    return executor.submit(new DownloadCallable(start, end, progressListener));
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

  public String getVersion()
  {
    return version;
  }
  
  public String getBucket()
  {
    return bucketName;
  }

  private class DownloadCallable implements Callable<InputStream>
  {
    private long start;
    private long end;
    private Optional<OverallProgressListener> progressListener;

    public DownloadCallable(long start, long end,
                            Optional<OverallProgressListener> progressListener)
    {
      this.start = start;
      this.end = end;
      this.progressListener = progressListener;
    }

    public InputStream call() throws Exception
    {
      GetObjectRequest req = null;
      if (version == null) {
    	req = new GetObjectRequest(bucketName, key);
      }
      
      else {
    	 req = new GetObjectRequest(bucketName, key, version);
      }
      req.setRange(start, end);
      if (progressListener.isPresent()) {
        PartProgressEvent ppe = new PartProgressEvent(
            Long.toString(start) + ':' + Long.toString(end));
        ProgressListener s3pl = new S3ProgressListener(progressListener.get(),
            ppe);
        req.setGeneralProgressListener(s3pl);
      }

      return client.getObject(req).getObjectContent();
    }
  }
}
