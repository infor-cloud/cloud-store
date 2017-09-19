package com.logicblox.s3lib;

/**
 * {@code EncryptionKeyOptions} contains all the options needed for
 * adding/removing encryption keys.
 *
 * {@code EncryptionKeyOptions} objects are meant to be built by {@code
 * EncryptionKeyOptionsBuilder}. This class provides only public getter methods.
 */
public class EncryptionKeyOptions extends CommandOptions
{
  private final String _bucket;
  private final String _objectKey;
  private final String _encryptionKey;

  EncryptionKeyOptions(CloudStoreClient cloudStoreClient,
                       String bucket,
                       String objectKey,
                       String encryptionKey)
  {
    super(cloudStoreClient);
    _bucket = bucket;
    _objectKey = objectKey;
    _encryptionKey = encryptionKey;
  }

  public String getBucket()
  {
    return _bucket;
  }

  public String getObjectKey()
  {
    return _objectKey;
  }

  public String getEncryptionKey()
  {
    return _encryptionKey;
  }
}
