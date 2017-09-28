/*
  Copyright 2017, Infor Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.logicblox.cloudstore;

/**
 * {@code EncryptionKeyOptionsBuilder} is used to create and set properties for {@code EncryptionKeyOptions} 
 * objects used to control behavior of the cloud-store commands to add and remove encryption
 * keys to and from files in a cloud store service.
 * <p>
 * Setting {@code bucketName}, {@code objectKey}, and {@code encryptionKey} are mandatory. All 
 * the others are optional.
 * 
 * @see EncryptionKeyOptions
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#addEncryptionKey(EncryptionKeyOptions)
 * @see CloudStoreClient#removeEncryptionKey(EncryptionKeyOptions)
 * @see OptionsBuilderFactory#newEncryptionKeyOptionsBuilder()
 */
public class EncryptionKeyOptionsBuilder
  extends CommandOptionsBuilder
{
  private String _bucket;
  private String _objectKey;
  private String _encryptionKey;

  EncryptionKeyOptionsBuilder(CloudStoreClient client)
  {
    _cloudStoreClient = client;
  }

  /**
   * Set name of bucket containing file to be modified.
   * 
   * @param bucket name of bucket with file to be modified
   * @return this builder
   */
  public EncryptionKeyOptionsBuilder setBucketName(String bucket)
  {
    _bucket = bucket;
    return this;
  }

  /**
   * Set the key of the file to be modified.
   * 
   * @param objectKey key of file to be modified
   * @return this builder
   */
  public EncryptionKeyOptionsBuilder setObjectKey(String objectKey)
  {
    _objectKey = objectKey;
    return this;
  }

  /**
   * Set the name of the public/private key pair to be added to or removed
   * from a file.  The key pair file must exist in the local key directory.
   * 
   * @param encryptionKey name of encryption key
   * @return this builder
   */
  public EncryptionKeyOptionsBuilder setEncryptionKey(String encryptionKey)
  {
    _encryptionKey = encryptionKey;
    return this;
  }

  private void validateOptions()
  {
    if(_cloudStoreClient == null)
    {
      throw new UsageException("CloudStoreClient has to be set");
    }
    else if(_bucket == null)
    {
      throw new UsageException("Bucket has to be set");
    }
    else if(_objectKey == null)
    {
      throw new UsageException("Object key has to be set");
    }
    else if(_encryptionKey == null)
    {
      throw new UsageException("Encryption key has to be set");
    }
  }

  /**
   * Validate that all required parameters are set and if so return a new {@link EncryptionKeyOptions}
   * object.
   *
   * @return immutable options with values from this builder
   */
  @Override
  public EncryptionKeyOptions createOptions()
  {
    validateOptions();

    return new EncryptionKeyOptions(_cloudStoreClient, _bucket, _objectKey, _encryptionKey);
  }
}
