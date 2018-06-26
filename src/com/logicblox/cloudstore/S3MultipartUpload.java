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
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.codec.digest.DigestUtils;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

class S3MultipartUpload
  implements Upload
{
  private ConcurrentMap<Integer, PartETag> _etags = new ConcurrentSkipListMap<>();
  private AmazonS3 _client;
  private String _uploadId;
  private Date _initiated;
  private ListeningExecutorService _executor;
  private UploadOptions _options;

  public S3MultipartUpload(
    UploadOptions options, AmazonS3 client, ListeningExecutorService executor, String uploadId,
    Date initiated)
  {
    _options = options;
    _client = client;
    _uploadId = uploadId;
    _initiated = initiated;
    _executor = executor;
  }

  public ListenableFuture<Void> uploadPart(
    int partNumber, long partSize, Callable<InputStream> stream,
    OverallProgressListener progressListener)
  {
    return _executor.submit(new UploadCallable(partNumber, partSize, stream, progressListener));
  }

  public ListenableFuture<String> completeUpload()
  {
    return _executor.submit(new CompleteCallable());
  }

  public ListenableFuture<Void> abort()
  {
    return _executor.submit(new AbortCallable());
  }

  public String getBucketName()
  {
    return _options.getBucketName();
  }

  public String getObjectKey()
  {
    return _options.getObjectKey();
  }

  public String getId()
  {
    return _uploadId;
  }

  public Date getInitiationDate()
  {
    return _initiated;
  }

  private class AbortCallable
    implements Callable<Void>
  {
    public Void call()
      throws Exception
    {
      AbortMultipartUploadRequest req = new AbortMultipartUploadRequest(getBucketName(),
        getObjectKey(), _uploadId);
      _client.abortMultipartUpload(req);
      return null;
    }
  }

  private class CompleteCallable
    implements Callable<String>
  {
    public String call()
      throws Exception
    {
      String multipartDigest;
      CompleteMultipartUploadRequest req;

      req = new CompleteMultipartUploadRequest(getBucketName(), getObjectKey(), _uploadId,
        new ArrayList<>(_etags.values()));
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
          "Failed checksum validation for " + getBucketName() + "/" + getObjectKey() + ". " +
            "Calculated " + "MD5: " + multipartDigest + ", Expected MD5: " + res.getETag());
      }
    }
  }

  private class UploadCallable
    implements Callable<Void>
  {
    private int _partNumber;
    private long _partSize;
    private Callable<InputStream> _streamCallable;
    private OverallProgressListener _progressListener;

    public UploadCallable(
      int partNumber, long partSize, Callable<InputStream> streamCallable,
      OverallProgressListener progressListener)
    {
      _partNumber = partNumber;
      _partSize = partSize;
      _streamCallable = streamCallable;
      _progressListener = progressListener;
    }

    public Void call()
      throws Exception
    {
      try(HashingInputStream stream = new HashingInputStream(_streamCallable.call()))
      {
        return upload(stream);
      }
    }

    private Void upload(HashingInputStream stream)
      throws BadHashException
    {

      // added to support retry testing
      _options.injectAbort(_uploadId);

      UploadPartRequest req = new UploadPartRequest();
      req.setBucketName(getBucketName());
      req.setInputStream(stream);
      req.setPartNumber(_partNumber + 1);
      req.setPartSize(_partSize);
      req.setUploadId(_uploadId);
      req.setKey(getObjectKey());

      // According to
      // https://github.com/aws/aws-sdk-java/issues/427#issuecomment-100518891
      // and
      // https://github.com/aws/aws-sdk-java/issues/427#issuecomment-100583279,
      // since we don't use a File or FileInputStream directly (stream here is
      // either CipherWithInlineIVInputStream -if we encrypt- or
      // BufferedInputStream) we should set the "read limit" (which is the
      // maximum buffer size that could be consumed) as suggested in the
      // relevant error message. That limit should be the
      // expected max size of our input stream in bytes, plus 1, hence
      // _partSize+1.
      req.getRequestClientOptions().setReadLimit(Ints.checkedCast(_partSize + 1));

      if(_progressListener != null)
      {
        PartProgressEvent ppe = new PartProgressEvent(Integer.toString(_partNumber));
        ProgressListener s3pl = new S3ProgressListener(_progressListener, ppe);
        req.setGeneralProgressListener(s3pl);
      }

      UploadPartResult res = _client.uploadPart(req);
      byte[] etag = DatatypeConverter.parseHexBinary(res.getETag());
      if(Arrays.equals(etag, stream.getDigest()))
      {
        _etags.put(_partNumber, res.getPartETag());

        return null;
      }
      else
      {
        String calculatedMD5 = DatatypeConverter.printHexBinary(stream.getDigest()).toLowerCase();
        throw new BadHashException(
          "Failed checksum validation for part " + (_partNumber + 1) + " of " + getBucketName() +
            "/" + getObjectKey() + ". " + "Calculated MD5: " + calculatedMD5 + ", Expected MD5: " +
            res.getETag());
      }
    }
  }
}
