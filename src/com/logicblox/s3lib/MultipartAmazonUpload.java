package com.logicblox.s3lib;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import javax.xml.bind.DatatypeConverter;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

class MultipartAmazonUpload implements Upload
{
  private List<PartETag> etags = new ArrayList<PartETag>();
  private AmazonS3 client;
  private String bucketName;
  private String key;
  private String uploadId;
  private ListeningExecutorService executor;
  private boolean progress;

  public MultipartAmazonUpload(AmazonS3 client, String bucketName, String key, String uploadId,
                               ListeningExecutorService executor, boolean progress)
  {
    this.bucketName = bucketName;
    this.key = key;
    this.client = client;
    this.uploadId = uploadId;
    this.executor = executor;
    this.progress = progress;
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
      if (progress) {
        req.setGeneralProgressListener(
                new AmazonUploadProgressListener(
                        "part " + (partNumber + 1),
                        partSize / 10,
                        partSize));
      }

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
  }

  private static class AmazonUploadProgressListener
          extends ProgressListener
          implements com.amazonaws.event.ProgressListener {

    public AmazonUploadProgressListener(String name, long intervalInBytes, long totalSizeInBytes) {
      super(name, "upload", intervalInBytes, totalSizeInBytes);
    }

    @Override
    public void progressChanged(ProgressEvent event) {
      progress(event.getBytesTransferred());
    }
  }
}
