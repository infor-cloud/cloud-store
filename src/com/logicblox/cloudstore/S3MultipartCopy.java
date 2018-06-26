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
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.codec.digest.DigestUtils;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;


class S3MultipartCopy
  implements Copy
{
  private ConcurrentMap<Integer, PartETag> _etags = new ConcurrentSkipListMap<Integer, PartETag>();
  private AmazonS3 _client;
  private CopyOptions _options;
  private String _uploadId;
  private ObjectMetadata _meta;
  private ListeningExecutorService _executor;

  public S3MultipartCopy(
    CopyOptions options, AmazonS3 client, ListeningExecutorService executor, String uploadId,
    ObjectMetadata meta)
  {
    _client = client;
    _executor = executor;
    _options = options;
    _uploadId = uploadId;
    _meta = meta;
  }

  public ListenableFuture<Void> copyPart(
    int partNumber, Long startByte, Long endByte, OverallProgressListener progressListener)
  {
    return _executor.submit(new CopyCallable(partNumber, startByte, endByte, progressListener));
  }

  public ListenableFuture<String> completeCopy()
  {
    return _executor.submit(new CompleteCallable());
  }

  public String getSourceBucketName()
  {
    return _options.getSourceBucketName();
  }

  public String getSourceObjectKey()
  {
    return _options.getSourceObjectKey();
  }

  public String getDestinationBucketName()
  {
    return _options.getDestinationBucketName();
  }

  public String getDestinationObjectKey()
  {
    return _options.getDestinationObjectKey();
  }

  public Long getObjectSize()
  {
    return _meta.getContentLength();
  }

  public Map<String, String> getMeta()
  {
    return _meta.getUserMetadata();
  }

  private class CompleteCallable
    implements Callable<String>
  {
    public String call()
      throws Exception
    {
      String multipartDigest;
      CompleteMultipartUploadRequest req;

      req = new CompleteMultipartUploadRequest(getDestinationBucketName(),
        getDestinationObjectKey(), _uploadId, new ArrayList<PartETag>(_etags.values()));
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      for(Integer pNum : _etags.keySet())
      {
        os.write(DatatypeConverter.parseHexBinary(_etags.get(pNum).getETag()));
      }

      multipartDigest = DigestUtils.md5Hex(os.toByteArray()) + "-" + _etags.size();

      CompleteMultipartUploadResult res = _client.completeMultipartUpload(req);

      if(res.getETag().equals(multipartDigest))
      {
        return res.getETag();
      }
      else
      {
        throw new BadHashException(
          "Failed checksum validation for " + getDestinationBucketName() + "/" +
            getDestinationObjectKey() + ". " + "Calculated MD5: " + multipartDigest + ", " +
            "Expected MD5: " + res.getETag());
      }
    }
  }

  private class CopyCallable
    implements Callable<Void>
  {
    private int _partNumber;
    private Long _startByte;
    private Long _endByte;
    private OverallProgressListener _progressListener;

    public CopyCallable(
      int partNumber, Long startByte, Long endByte, OverallProgressListener progressListener)
    {
      _partNumber = partNumber;
      _startByte = startByte;
      _endByte = endByte;
      _progressListener = progressListener;
    }

    public Void call()
      throws Exception
    {
      CopyPartRequest req = new CopyPartRequest().withSourceBucketName(getSourceBucketName())
        .withSourceKey(getSourceObjectKey())
        .withDestinationBucketName(getDestinationBucketName())
        .withDestinationKey(getDestinationObjectKey())
        .withUploadId(_uploadId)
        .withFirstByte(_startByte)
        .withLastByte(_endByte)
        .withPartNumber(_partNumber + 1);

      if(_progressListener != null)
      {
        PartProgressEvent ppe = new PartProgressEvent(Integer.toString(_partNumber));
        ProgressListener s3pl = new S3ProgressListener(_progressListener, ppe);
        req.setGeneralProgressListener(s3pl);
      }

      CopyPartResult res = _client.copyPart(req);
      _etags.put(res.getPartNumber(), new PartETag(res.getPartNumber(), res.getETag()));

      return null;
    }
  }
}
