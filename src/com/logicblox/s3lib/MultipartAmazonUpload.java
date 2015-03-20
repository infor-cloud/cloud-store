package com.logicblox.s3lib;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.io.InputStream;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.xml.bind.DatatypeConverter;

import com.amazonaws.event.ProgressListener;
import com.google.common.base.Optional;
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
  private ConcurrentMap<Integer, PartETag> etags = new ConcurrentSkipListMap<Integer, PartETag>();
  private AmazonS3 client;
  private String bucketName;
  private String key;
  private String uploadId;
  private ListeningExecutorService executor;

  public MultipartAmazonUpload(AmazonS3 client, String bucketName, String key, String uploadId,
                               ListeningExecutorService executor)
  {
    this.bucketName = bucketName;
    this.key = key;
    this.client = client;
    this.uploadId = uploadId;
    this.executor = executor;
  }

  public ListenableFuture<Void> uploadPart(int partNumber, InputStream stream, long partSize)
  {
    return uploadPart(partNumber, stream, partSize,
        Optional.<OverallProgressListener>absent());
  }

  public ListenableFuture<Void> uploadPart(int partNumber, InputStream
      stream, long partSize, Optional<OverallProgressListener> progressListener)
  {
    return executor.submit(new UploadCallable(partNumber, stream, partSize, progressListener));
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

      req = new CompleteMultipartUploadRequest(bucketName, key, uploadId,
          new ArrayList<PartETag>(etags.values()));
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      for (Integer pNum : etags.keySet()) {
        os.write(DatatypeConverter.parseHexBinary(etags.get(pNum).getETag()));
      }

      multipartDigest = DigestUtils.md5Hex(os.toByteArray()) + "-" + etags.size();

      CompleteMultipartUploadResult res = client.completeMultipartUpload(req);

      if(res.getETag().equals(multipartDigest)){
        return res.getETag();
      }
      else {
        throw new BadHashException("Failed upload checksum validation for " +
            bucketName + "/" + key + ". " +
            "Calculated MD5: " + multipartDigest +
            ", Expected MD5: " + res.getETag());
      }
    }
  }

  private class UploadCallable implements Callable<Void>
  {
    private int partNumber;
    private HashingInputStream stream;
    private long partSize;
    private Optional<OverallProgressListener> progressListener;

    public UploadCallable(int partNumber, InputStream stream, long partSize,
                          Optional<OverallProgressListener> progressListener)
    {
      this.partNumber = partNumber;
      this.partSize = partSize;
      this.stream = new HashingInputStream(stream);
      this.progressListener = progressListener;
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

      if (progressListener.isPresent()) {
        PartProgressEvent ppe = new PartProgressEvent(Integer.toString
            (partNumber));
        ProgressListener s3pl = new S3ProgressListener(progressListener.get(),
            ppe);
        req.setGeneralProgressListener(s3pl);
      }

      UploadPartResult res = client.uploadPart(req);
      byte[] etag = DatatypeConverter.parseHexBinary(res.getETag());
      if (Arrays.equals(etag, stream.getDigest()))
      {
        etags.put(partNumber, res.getPartETag());

        return null;
      }
      else
      {
        String calculatedMD5 = DatatypeConverter.printHexBinary(stream.getDigest()).toLowerCase();
        throw new BadHashException("Failed upload checksum validation for part " +
            (partNumber + 1) + " of " +
            bucketName + "/" + key + ". " +
            "Calculated MD5: " + calculatedMD5 +
            ", Expected MD5: " + res.getETag());
      }
    }
  }
}
