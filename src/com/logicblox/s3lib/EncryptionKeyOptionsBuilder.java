package com.logicblox.s3lib;

public class EncryptionKeyOptionsBuilder
{
  private String _bucket;
  private String _objectKey;
  private String _encryptionKey;

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

  public EncryptionKeyOptions createEncryptionKeyOptions()
  {
    return new EncryptionKeyOptions(_bucket, _objectKey, _encryptionKey);
  }
}
