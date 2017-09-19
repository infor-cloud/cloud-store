package com.logicblox.s3lib;

import java.util.Date;

public class PendingUploadsOptionsBuilder extends CommandOptionsBuilder
{
  private String _bucket;
  private String _objectKey;
  private String _uploadId;
  private Date _date;

  PendingUploadsOptionsBuilder(CloudStoreClient client)
  {
    _cloudStoreClient = client;
  }

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

  @Override
  public PendingUploadsOptions createOptions()
  {
    return new PendingUploadsOptions(_cloudStoreClient, _bucket, _objectKey,
      _uploadId, _date);
  }
}
