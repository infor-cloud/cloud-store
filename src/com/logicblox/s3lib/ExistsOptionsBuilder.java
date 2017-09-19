package com.logicblox.s3lib;

public class ExistsOptionsBuilder extends CommandOptionsBuilder
{
  private String _bucket;
  private String _objectKey;

  ExistsOptionsBuilder(CloudStoreClient client)
  {
    _cloudStoreClient = client;
  }

  public ExistsOptionsBuilder setBucket(String bucket)
  {
    _bucket = bucket;
    return this;
  }
  
  public ExistsOptionsBuilder setObjectKey(String objectKey)
  {
    _objectKey = objectKey;
    return this;
  }

  @Override
  public ExistsOptions createOptions()
  {
    return new ExistsOptions(_cloudStoreClient, _bucket, _objectKey);
  }
}
