package com.logicblox.s3lib;


public class ExistsOptions extends CommandOptions
{
  private final String _bucket;
  private final String _objectKey;

  ExistsOptions(CloudStoreClient cloudStoreClient,
                String bucket,
                String objectKey)
  {
    super(cloudStoreClient);
    _bucket = bucket;
    _objectKey = objectKey;
  }

  public String getBucketName()
  {
    return _bucket;
  }

  public String getObjectKey()
  {
    return _objectKey;
  }
}
