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
  private AmazonS3 _client;
  private ListeningExecutorService _executor;
  private ObjectMetadata _meta;
  private String _objectKey;
  private String _bucketName;
  private String _version;

  public S3Download(
    AmazonS3 client, String objectKey, String bucketName, String version, ObjectMetadata meta,
    ListeningExecutorService executor)
  {
    _client = client;
    _objectKey = objectKey;
    _bucketName = bucketName;
    _version = version;
    _executor = executor;
    _meta = meta;
  }

  public ListenableFuture<InputStream> getPart(long start, long end)
  {
    return getPart(start, end, null);
  }

  public ListenableFuture<InputStream> getPart(
    long start, long end, OverallProgressListener progressListener)
  {
    return _executor.submit(new DownloadCallable(start, end, progressListener));
  }

  public Map<String, String> getMeta()
  {
    return _meta.getUserMetadata();
  }

  public long getLength()
  {
    return _meta.getContentLength();
  }

  public String getETag()
  {
    return _meta.getETag();
  }

  public String getKey()
  {
    return _objectKey;
  }

  public String getVersion()
  {
    return _version;
  }

  public String getBucket()
  {
    return _bucketName;
  }

  private class DownloadCallable
    implements Callable<InputStream>
  {
    private long _start;
    private long _end;
    private OverallProgressListener _progressListener;

    public DownloadCallable(long start, long end, OverallProgressListener progressListener)
    {
      _start = start;
      _end = end;
      _progressListener = progressListener;
    }

    public InputStream call()
      throws Exception
    {
      GetObjectRequest req = null;
      if(_version == null)
      {
        req = new GetObjectRequest(_bucketName, _objectKey);
      }
      else
      {
        req = new GetObjectRequest(_bucketName, _objectKey, _version);
      }
      req.setRange(_start, _end);
      if(_progressListener != null)
      {
        PartProgressEvent ppe = new PartProgressEvent(
          Long.toString(_start) + ':' + Long.toString(_end));
        ProgressListener s3pl = new S3ProgressListener(_progressListener, ppe);
        req.setGeneralProgressListener(s3pl);
      }

      return _client.getObject(req).getObjectContent();
    }
  }
}
