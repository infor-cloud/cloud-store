package com.logicblox.s3lib;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.io.IOException;
import java.io.InputStream;
import java.io.FilterInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.xml.bind.DatatypeConverter;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.PartETag;
import org.apache.commons.codec.digest.DigestUtils;

class MultipartAmazonUpload implements Upload
{
  private TreeMap<Integer, PartETag> etags = new TreeMap<Integer, PartETag>();
  private AmazonS3 client;
  private String bucketName;
  private String key;
  private String uploadId;
  private ListeningExecutorService executor;

  public MultipartAmazonUpload(AmazonS3 client, String bucketName, String key, String uploadId, ListeningExecutorService executor)
  {
    this.bucketName = bucketName;
    this.key = key;
    this.client = client;
    this.uploadId = uploadId;
    this.executor = executor;
  }

  public ListenableFuture<Void> uploadPart(int partNumber, InputStream stream, long partSize)
  {
    return executor.submit(new UploadCallable(partNumber, stream, partSize));
  }

  public ListenableFuture<String> completeUpload()
  {
    return executor.submit(new CompleteCallable());
  }

  public ListenableFuture<Void> abort()
  {
    return executor.submit(new AbortCallable());
  }

  private class AbortCallable implements Callable<Void>
  {
    public Void call() throws Exception
    {
      AbortMultipartUploadRequest req = new AbortMultipartUploadRequest(bucketName,key, uploadId);
      client.abortMultipartUpload(req);
      return null;
    }
  }

  private class CompleteCallable implements Callable<String>
  {
    public String call() throws Exception
    {
      String multipartDigest;
      CompleteMultipartUploadRequest req;
      synchronized(MultipartAmazonUpload.this)
      {
        req = new CompleteMultipartUploadRequest(bucketName, key, uploadId,
            new ArrayList<PartETag>(etags.values()));
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        for (Integer pNum : etags.keySet()) {
          os.write(DatatypeConverter.parseHexBinary(etags.get(pNum).getETag()));
        }

        multipartDigest = DigestUtils.md5Hex(os.toByteArray()) + "-" + etags.size();
      }
      CompleteMultipartUploadResult res = client.completeMultipartUpload(req);

      if(res.getETag().equals(multipartDigest)){
        return res.getETag();
      }
      else {
        throw new BadHashException();
      }
    }
  }

  private class UploadCallable implements Callable<Void>
  {
    private int partNumber;
    private HashingInputStream stream;
    private long partSize;

    public UploadCallable(int partNumber, InputStream stream, long partSize)
    {
      this.partNumber = partNumber;
      this.partSize = partSize;
      this.stream = new HashingInputStream(stream);
    }

    public Void call() throws Exception
    {
      UploadPartRequest req = new UploadPartRequest();
      req.setBucketName(bucketName);
      req.setInputStream(stream);
      req.setPartNumber(partNumber + 1);
      req.setPartSize(partSize);
      req.setUploadId(uploadId);
      req.setKey(key);
      UploadPartResult res = client.uploadPart(req);
      byte[] etag = DatatypeConverter.parseHexBinary(res.getETag());
      if (Arrays.equals(etag, stream.getDigest()))
      {
        synchronized(MultipartAmazonUpload.this)
        {
          etags.put(partNumber, res.getPartETag());
        }
        return null;
      }
      else
      {
        throw new BadHashException();
      }
    }
  }
}
