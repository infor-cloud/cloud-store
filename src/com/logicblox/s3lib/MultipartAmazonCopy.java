package com.logicblox.s3lib;

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


class MultipartAmazonCopy implements Copy
{
  private ConcurrentMap<Integer, PartETag> etags = new ConcurrentSkipListMap<Integer, PartETag>();
  private AmazonS3 client;
  private String sourceBucketName;
  private String sourceObjectKey;
  private String destinationBucketName;
  private String destinationObjectKey;
  private String uploadId;
  private ObjectMetadata meta;
  private ListeningExecutorService executor;

  public MultipartAmazonCopy(AmazonS3 client,
                             String sourceBucketName, String sourceObjectKey,
                             String destinationBucketName, String destinationObjectKey,
                             String uploadId,
                             ObjectMetadata meta,
                             ListeningExecutorService executor)
  {
    this.sourceBucketName = sourceBucketName;
    this.sourceObjectKey = sourceObjectKey;
    this.destinationBucketName = destinationBucketName;
    this.destinationObjectKey = destinationObjectKey;
    this.client = client;
    this.uploadId = uploadId;
    this.meta = meta;
    this.executor = executor;
  }

  public ListenableFuture<Void> copyPart(int partNumber, Long startByte, Long
      endByte, OverallProgressListener progressListener)
  {
    return executor.submit(new CopyCallable(partNumber, startByte, endByte,
        progressListener));
  }

  public ListenableFuture<String> completeCopy()
  {
    return executor.submit(new CompleteCallable());
  }

  public String getSourceBucketName()
  {
    return sourceBucketName;
  }

  public String getSourceObjectKey()
  {
    return sourceObjectKey;
  }

  public String getDestinationBucketName()
  {
    return destinationBucketName;
  }

  public String getDestinationObjectKey()
  {
    return destinationObjectKey;
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
          destinationObjectKey, uploadId, new ArrayList<PartETag>(etags.values()));
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
        throw new BadHashException("Failed checksum validation for " +
            destinationBucketName + "/" + destinationObjectKey + ". " +
            "Calculated MD5: " + multipartDigest +
            ", Expected MD5: " + res.getETag());
      }
    }
  }

  private class CopyCallable implements Callable<Void>
  {
    private int partNumber;
    private Long startByte;
    private Long endByte;
    private OverallProgressListener progressListener;

    public CopyCallable(int partNumber, Long startByte, Long endByte,
                        OverallProgressListener progressListener)
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
          .withSourceKey(sourceObjectKey)
          .withDestinationBucketName(destinationBucketName)
          .withDestinationKey(destinationObjectKey)
          .withUploadId(uploadId)
          .withFirstByte(startByte)
          .withLastByte(endByte)
          .withPartNumber(partNumber + 1);
      
      if (progressListener != null) {
        PartProgressEvent ppe = new PartProgressEvent(Integer.toString(partNumber));
        ProgressListener s3pl = new S3ProgressListener(progressListener, ppe);
        req.setGeneralProgressListener(s3pl);
      }

      CopyPartResult res = client.copyPart(req);
      etags.put(res.getPartNumber(),
          new PartETag(res.getPartNumber(), res.getETag()));

      return null;
    }
  }
}
