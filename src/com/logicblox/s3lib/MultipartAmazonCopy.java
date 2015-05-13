package com.logicblox.s3lib;

import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.google.common.base.Optional;
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


class MultipartAmazonCopy implements Copy
{
  private ConcurrentMap<Integer, PartETag> etags = new ConcurrentSkipListMap<Integer, PartETag>();
  private AmazonS3 client;
  private String sourceBucketName;
  private String sourceKey;
  private String destinationBucketName;
  private String destinationKey;
  private String uploadId;
  private ObjectMetadata meta;
  private ListeningExecutorService executor;

  public MultipartAmazonCopy(AmazonS3 client,
                             String sourceBucketName, String sourceKey,
                             String destinationBucketName, String destinationKey,
                             String uploadId,
                             ObjectMetadata meta,
                             ListeningExecutorService executor)
  {
    this.sourceBucketName = sourceBucketName;
    this.sourceKey = sourceKey;
    this.destinationBucketName = destinationBucketName;
    this.destinationKey = destinationKey;
    this.client = client;
    this.uploadId = uploadId;
    this.meta = meta;
    this.executor = executor;
  }

  public ListenableFuture<Void> copyPart(int partNumber, long startByte, long
      endByte, Optional<OverallProgressListener> progressListener)
  {
    return executor.submit(new CopyCallable(partNumber, startByte, endByte,
        progressListener));
  }

  public ListenableFuture<String> completeCopy()
  {
    return executor.submit(new CompleteCallable());
  }

  public String getSourceBucket()
  {
    return sourceBucketName;
  }

  public String getSourceKey()
  {
    return sourceKey;
  }

  public String getDestinationBucket()
  {
    return destinationBucketName;
  }

  public String getDestinationKey()
  {
    return destinationKey;
  }

  public Long getObjectSize()
  {
    return meta.getContentLength();
  }

  public Map<String,String> getMeta()
  {
    return meta.getUserMetadata();
  }

  private class CompleteCallable implements Callable<String>
  {
    public String call() throws Exception
    {
      String multipartDigest;
      CompleteMultipartUploadRequest req;

      req = new CompleteMultipartUploadRequest(destinationBucketName,
          destinationKey, uploadId, new ArrayList<PartETag>(etags.values()));
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      for (Integer pNum : etags.keySet()) {
        os.write(DatatypeConverter.parseHexBinary(etags.get(pNum).getETag()));
      }

      multipartDigest = DigestUtils.md5Hex(os.toByteArray()) + "-" + etags.size();

      CompleteMultipartUploadResult res = client.completeMultipartUpload(req);

      if(res.getETag().equals(multipartDigest)) {
        return res.getETag();
      }
      else {
        throw new BadHashException("Failed copy checksum validation for " +
            destinationBucketName + "/" + destinationKey + ". " +
            "Calculated MD5: " + multipartDigest +
            ", Expected MD5: " + res.getETag());
      }
    }
  }

  private class CopyCallable implements Callable<Void>
  {
    private int partNumber;
    private long startByte;
    private long endByte;
    private Optional<OverallProgressListener> progressListener;

    public CopyCallable(int partNumber, long startByte, long endByte,
                        Optional<OverallProgressListener> progressListener)
    {
      this.partNumber = partNumber;
      this.startByte = startByte;
      this.endByte = endByte;
      this.progressListener = progressListener;
    }

    public Void call() throws Exception
    {
      CopyPartRequest req = new CopyPartRequest()
          .withSourceBucketName(sourceBucketName)
          .withSourceKey(sourceKey)
          .withDestinationBucketName(destinationBucketName)
          .withDestinationKey(destinationKey)
          .withUploadId(uploadId)
          .withFirstByte(startByte)
          .withLastByte(endByte)
          .withPartNumber(partNumber + 1);
      
      if (progressListener.isPresent()) {
        PartProgressEvent ppe = new PartProgressEvent(Integer.toString
            (partNumber));
        ProgressListener s3pl = new S3ProgressListener(progressListener.get(),
            ppe);
        req.setGeneralProgressListener(s3pl);
      }

      CopyPartResult res = client.copyPart(req);
      etags.put(res.getPartNumber(),
          new PartETag(res.getPartNumber(), res.getETag()));

      return null;
    }
  }
}
