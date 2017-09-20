package com.logicblox.s3lib;

public class ExistsOptionsBuilder extends CommandOptionsBuilder
{
  private String _bucket;
  private String _objectKey;

  ExistsOptionsBuilder(CloudStoreClient client)
  {
    _cloudStoreClient = client;
  }

  public ExistsOptionsBuilder setBucketName(String bucket)
  {
    _bucket = bucket;
    return this;
  }
  
  public ExistsOptionsBuilder setObjectKey(String objectKey)
  {
    _objectKey = objectKey;
    return this;
  }

  private void validateOptions()
  {
    if (_cloudStoreClient == null) {
      throw new UsageException("CloudStoreClient has to be set");
    }
    else if (_bucket == null) {
      throw new UsageException("Bucket has to be set");
    }
    else if (_objectKey == null) {
      throw new UsageException("Object key has to be set");
    }
  }

  @Override
  public ExistsOptions createOptions()
  {
    validateOptions();

    return new ExistsOptions(_cloudStoreClient, _bucket, _objectKey);
  }
}
