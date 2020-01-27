/*
  Copyright 2018, Infor Inc.

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
import org.apache.commons.codec.digest.DigestUtils;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

class S3Download
  implements Download
{
  private DownloadOptions _options;
  private long _fileLength;
  private long _chunkSize;
  private AmazonS3 _client;
  private ListeningExecutorService _apiExecutor;
  private ListeningExecutorService _internalExecutor;
  private ObjectMetadata _meta;
  private ConcurrentMap<Integer, HashingInputStream> _partInputStreams = new ConcurrentSkipListMap<>();

  public S3Download(
    DownloadOptions options,
    long fileLength, long chunkSize,
    AmazonS3 client,
    ListeningExecutorService apiExecutor,
    ListeningExecutorService internalExecutor,
    ObjectMetadata meta)
  {
    _options = options;
    _fileLength = fileLength;
    _chunkSize = chunkSize;
    _client = client;
    _apiExecutor = apiExecutor;
    _internalExecutor = internalExecutor;
    _meta = meta;
  }

  @Override
  public ListenableFuture<InputStream> downloadPart(
    int partNumber, long start, long end, OverallProgressListener opl)
  {
    return _apiExecutor.submit(new DownloadCallable(partNumber, start, end, opl));
  }

  public ListenableFuture<Download> completeDownload()
  {
    return _internalExecutor.submit(new CompleteCallable());
  }

  @Override
  public Map<String, String> getMetadata()
  {
    return _meta.getUserMetadata();
  }

  @Override
  public long getLength()
  {
    return _meta.getContentLength();
  }

  @Override
  public String getETag()
  {
    return _meta.getETag();
  }

  @Override
  public String getObjectKey()
  {
    return _options.getObjectKey();
  }

  @Override
  public String getBucketName()
  {
    return _options.getBucketName();
  }

  private class DownloadCallable
    implements Callable<InputStream>
  {
    private int _partNumber;
    private long _start;
    private long _end;
    private OverallProgressListener _progressListener;

    public DownloadCallable(
      int partNumber, long start, long end, OverallProgressListener progressListener)
    {
      _partNumber = partNumber;
      _start = start;
      _end = end;
      _progressListener = progressListener;
    }

    public InputStream call()
      throws Exception
    {
      GetObjectRequest req = null;
      String version = _options.getVersion().orElse(null);
      if(version == null)
      {
        req = new GetObjectRequest(getBucketName(), getObjectKey());
      }
      else
      {
        req = new GetObjectRequest(getBucketName(), getObjectKey(), version);
      }
      req.setRange(_start, _end);
      if(_progressListener != null)
      {
        PartProgressEvent ppe = new PartProgressEvent(
          Long.toString(_start) + ':' + Long.toString(_end));
        ProgressListener s3pl = new S3ProgressListener(_progressListener, ppe);
        req.setGeneralProgressListener(s3pl);
      }

      HashingInputStream in = new HashingInputStream(_client.getObject(req).getObjectContent());
      _partInputStreams.put(_partNumber, in);
      return in;
    }
  }

  private class CompleteCallable
    implements Callable<Download>
  {
    public Download call()
      throws Exception
    {
      String remoteEtag = getETag();
      String localDigest = "";
      String fn = "'s3://" + getBucketName() + "/" + getObjectKey() + "'";

      if(null == remoteEtag)
      {
        System.err.println(
          "Warning: Skipped checksum validation for " + fn + ".  No etag attached to object.");
        return S3Download.this;
      }

      if((remoteEtag.length() > 32) && (remoteEtag.charAt(32) == '-'))
      {
        // Object has been uploaded using S3's multipart upload protocol,
        // so it has a special Etag documented here:
        // http://permalink.gmane.org/gmane.comp.file-systems.s3.s3tools/583

        // Since, the Etag value depends on the value of the chuck size (and
        // the number of the chucks) we cannot validate robustly checksums for
        // files uploaded with tool other than s3tool.
        Map<String, String> meta = getMetadata();
        if(!meta.containsKey("s3tool-version"))
        {
          System.err.println(
            "Warning: Skipped checksum " + "validation for " + fn + ". It was uploaded using " +
              "other tool's multipart protocol.");
          return S3Download.this;
        }

        int expectedPartsNum = _fileLength == 0 ? 1
          : (int) Math.ceil(_fileLength / (double) _chunkSize);
        int actualPartsNum = Integer.parseInt(remoteEtag.substring(33));

        if(expectedPartsNum != actualPartsNum)
        {
          System.err.println(
            "Warning: Skipped checksum validation for " + fn + ". Actual number of parts: " +
              actualPartsNum + ", Expected number of parts: " + expectedPartsNum +
              ". Probably the ETag was changed by using another tool.");
          return S3Download.this;
        }

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        for(Integer pNum : _partInputStreams.keySet())
        {
          os.write(_partInputStreams.get(pNum).getDigest());
        }

        localDigest = DigestUtils.md5Hex(os.toByteArray()) + "-" + _partInputStreams.size();
      }
      else
      {
        // Object has been uploaded using S3's simple upload protocol,
        // so its Etag should be equal to object's MD5.
        // Same should hold for objects uploaded to GCS (if "compose" operation
        // wasn't used).
        if(_partInputStreams.size() == 1)
        {
          // Single-part download (1 range GET).
          localDigest = DatatypeConverter.printHexBinary(_partInputStreams.get(0).getDigest()).toLowerCase();
        }
        else
        {
          // Multi-part download (>1 range GETs).
          System.err.println("Warning: Skipped checksum validation for " + fn +
            ". No efficient way to compute MD5 on multipart downloads of files with " +
            "singlepart ETag.");
          return S3Download.this;
        }
      }
      if(remoteEtag.equals(localDigest))
      {
        return S3Download.this;
      }
      else
      {
        throw new BadHashException(
          "Failed checksum validation for " + getBucketName() + "/" + getObjectKey() + ". " +
            "Calculated MD5: " + localDigest + ", Expected MD5: " + remoteEtag);
      }
    }
  }
}