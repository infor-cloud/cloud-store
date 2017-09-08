package com.logicblox.s3lib;

import java.util.Date;

public class PendingUploadsOptionsBuilder
{
  private String _bucket;
  private String _objectKey;
  private String _uploadId;
  private Date _date;

  public PendingUploadsOptionsBuilder setBucket(String bucket)
  {
    _bucket = bucket;
    return this;
  }
  
  public PendingUploadsOptionsBuilder setObjectKey(String objectKey)
  {
    _objectKey = objectKey;
    return this;
  }
  
  public PendingUploadsOptionsBuilder setUploadId(String uploadId)
  {
    _uploadId = uploadId;
    return this;
  }
  
  public PendingUploadsOptionsBuilder setDate(Date date)
  {
    _date = date;
    return this;
  }

  public PendingUploadsOptions createPendingUploadsOptions()
  {
    return new PendingUploadsOptions(_bucket, _objectKey, _uploadId, _date);
  }
}
