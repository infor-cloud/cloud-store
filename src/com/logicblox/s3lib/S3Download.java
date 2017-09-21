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

package com.logicblox.s3lib;

import com.amazonaws.event.ProgressListener;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Callable;

class S3Download
{
  private AmazonS3 client;
  private ListeningExecutorService executor;
  private ObjectMetadata meta;
  private String key;
  private String bucketName;
  private String version;

  public S3Download(
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
    return getPart(start, end, null);
  }

  public ListenableFuture<InputStream> getPart(long start, long end,
                                               OverallProgressListener
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
    private OverallProgressListener progressListener;

    public DownloadCallable(long start, long end,
                            OverallProgressListener progressListener)
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
      if (progressListener != null) {
        PartProgressEvent ppe = new PartProgressEvent(
            Long.toString(start) + ':' + Long.toString(end));
        ProgressListener s3pl = new S3ProgressListener(progressListener, ppe);
        req.setGeneralProgressListener(s3pl);
      }

      return client.getObject(req).getObjectContent();
    }
  }
}
