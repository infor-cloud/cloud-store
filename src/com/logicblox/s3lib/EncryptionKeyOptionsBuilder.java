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

  public EncryptionKeyOptionsBuilder setBucketName(String bucket)
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
    else if (_encryptionKey == null) {
      throw new UsageException("Encryption key has to be set");
    }
  }

  @Override
  public EncryptionKeyOptions createOptions()
  {
    validateOptions();

    return new EncryptionKeyOptions(_cloudStoreClient, _bucket, _objectKey,
      _encryptionKey);
  }
}
