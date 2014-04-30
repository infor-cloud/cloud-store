package com.logicblox.s3lib;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
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

class MultipartAmazonUpload implements Upload
{
  private List<PartETag> etags = new ArrayList<PartETag>();
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
      CompleteMultipartUploadRequest req;
      synchronized(MultipartAmazonUpload.this)
      {
        req = new CompleteMultipartUploadRequest(bucketName, key, uploadId, etags);
      }
      CompleteMultipartUploadResult res = client.completeMultipartUpload(req);
      return res.getETag();
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
          etags.add(res.getPartETag());
        }
        return null;
      }
      else
      {
        throw new BadHashException();
      }
    }

    private class HashingInputStream extends FilterInputStream
    {
      private MessageDigest md;
      private byte[] digest;

      public HashingInputStream(InputStream in)
      {
        super(in);
        try
        {
          md = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
          // No MD5, give up
          throw new RuntimeException(e);
        }
      }

      public byte[] getDigest()
      {
        if (digest == null)
        {
          digest = md.digest();
        }

        return digest;
      }

      @Override
      public int read() throws IOException
      {
        int res = in.read();
        if (res != -1)
        {
          md.update((byte) res);
        }
        return res;
      }

      @Override
      public int read(byte[] b) throws IOException
      {
        int count = in.read(b);
        if (count != -1)
        {
          md.update(b, 0, count);
        }
        return count;
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException
      {
        int count = in.read(b, off, len);
        if (count != -1)
        {
          md.update(b, off, count);
        }
        return count;
      }
    }
  }
}
