package com.logicblox.s3lib;

public class EncryptionKeyOptionsBuilder extends CommandOptionsBuilder
{
  private String _bucket;
  private String _objectKey;
  private String _encryptionKey;

  EncryptionKeyOptionsBuilder(CloudStoreClient client)
  {
    _cloudStoreClient = client;
  }

  public EncryptionKeyOptionsBuilder setBucket(String bucket)
  {
    _bucket = bucket;
    return this;
  }
  
  public EncryptionKeyOptionsBuilder setObjectKey(String objectKey)
  {
    _objectKey = objectKey;
    return this;
  }
  
  public EncryptionKeyOptionsBuilder setEncryptionKey(String encryptionKey)
  {
    _encryptionKey = encryptionKey;
    return this;
  }

  @Override
  public EncryptionKeyOptions createOptions()
  {
    return new EncryptionKeyOptions(_cloudStoreClient, _bucket, _objectKey,
      _encryptionKey);
  }
}
