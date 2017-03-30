package com.logicblox.s3lib;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Callable;
import java.io.InputStream;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.xml.bind.DatatypeConverter;

import com.amazonaws.event.ProgressListener;
import com.google.common.base.Optional;
import com.google.common.primitives.Ints;
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
  private Date initiated;
  private ListeningExecutorService executor;


  public MultipartAmazonUpload(AmazonS3 client, String bucketName, String key, String uploadId,
                               Date initiated, ListeningExecutorService executor)
  {
    this.bucketName = bucketName;
    this.key = key;
    this.client = client;
    this.uploadId = uploadId;
    this.initiated = initiated;
    this.executor = executor;
  }

  public ListenableFuture<Void> uploadPart(int partNumber,
                                           long partSize,
                                           Callable<InputStream> stream,
                                           Optional<OverallProgressListener>
                                               progressListener)
  {
    // added to support retry testing
    if(UploadOptions.decrementAbortInjectionCounter(uploadId) > 0)
      throw new RuntimeException("forcing upload abort");

    return executor.submit(new UploadCallable(partNumber, partSize, stream,
        progressListener));
  }

  public ListenableFuture<String> completeUpload()
  {
    return executor.submit(new CompleteCallable());
  }

  public ListenableFuture<Void> abort()
  {
    return executor.submit(new AbortCallable());
  }

  public String getBucket()
  {
    return bucketName;
  }

  public String getKey()
  {
    return key;
  }

  public String getId()
  {
    return uploadId;
  }

  public Date getInitiationDate()
  {
    return initiated;
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
        throw new BadHashException("Failed checksum validation for " +
            bucketName + "/" + key + ". " +
            "Calculated MD5: " + multipartDigest +
            ", Expected MD5: " + res.getETag());
      }
    }
  }

  private class UploadCallable implements Callable<Void>
  {
    private int partNumber;
    private long partSize;
    private Callable<InputStream> streamCallable;
    private Optional<OverallProgressListener> progressListener;

    public UploadCallable(int partNumber,
                          long partSize,
                          Callable<InputStream> streamCallable,
                          Optional<OverallProgressListener> progressListener)
    {
      this.partNumber = partNumber;
      this.partSize = partSize;
      this.streamCallable = streamCallable;
      this.progressListener = progressListener;
    }

    public Void call() throws Exception
    {
      try (HashingInputStream stream = new HashingInputStream(streamCallable.call())) {
        return upload(stream);
      }
    }

    private Void upload(HashingInputStream stream) throws BadHashException {
      UploadPartRequest req = new UploadPartRequest();
      req.setBucketName(bucketName);
      req.setInputStream(stream);
      req.setPartNumber(partNumber + 1);
      req.setPartSize(partSize);
      req.setUploadId(uploadId);
      req.setKey(key);

      // LB-2298: According to
      // https://github.com/aws/aws-sdk-java/issues/427#issuecomment-100518891
      // and
      // https://github.com/aws/aws-sdk-java/issues/427#issuecomment-100583279,
      // since we don't use a File or FileInputStream directly (stream here is
      // either CipherWithInlineIVInputStream -if we encrypt- or
      // BufferedInputStream) we should set the "read limit" (which is the
      // maximum buffer size that could be consumed) as suggested in the
      // error message mentioned in LB-2298. That limit should be the
      // expected max size of our input stream in bytes, plus 1, hence
      // partSize+1.
      req.getRequestClientOptions().setReadLimit(Ints.checkedCast(partSize+1));

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
        throw new BadHashException("Failed checksum validation for part " +
            (partNumber + 1) + " of " +
            bucketName + "/" + key + ". " +
            "Calculated MD5: " + calculatedMD5 +
            ", Expected MD5: " + res.getETag());
      }
    }
  }
}
