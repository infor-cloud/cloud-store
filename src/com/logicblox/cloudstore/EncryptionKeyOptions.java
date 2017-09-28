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
 * {@code EncryptionKeyOptions} contains all the options needed for adding or removing encryption
 * keys to or from files in a cloud store service.
 * <p>
 * {@code EncryptionKeyOptions} objects are meant to be built by {@code
 * EncryptionKeyOptionsBuilder}. This class provides only public accessor methods.
 * 
 * @see EncryptionKeyOptionsBuilder
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#addEncryptionKey(EncryptionKeyOptions)
 * @see CloudStoreClient#removeEncryptionKey(EncryptionKeyOptions)
 * @see OptionsBuilderFactory#newEncryptionKeyOptionsBuilder()
 */
public class EncryptionKeyOptions
  extends CommandOptions
{
  private final String _bucket;
  private final String _objectKey;
  private final String _encryptionKey;

  EncryptionKeyOptions(
    CloudStoreClient cloudStoreClient, String bucket, String objectKey, String encryptionKey)
  {
    super(cloudStoreClient);
    _bucket = bucket;
    _objectKey = objectKey;
    _encryptionKey = encryptionKey;
  }

  /**
   * Return the name of bucket containing file to be modified.
   *
   * @return bucket name
   */
  public String getBucketName()
  {
    return _bucket;
  }

  /**
   * Return the key of the file to be modified.
   *
   * @return file key
   */
  public String getObjectKey()
  {
    return _objectKey;
  }

  /**
   * Return the name of the public/private key pair to be added to or removed
   * from a file.  The key pair file must exist in the local key directory.
   *
   * @return encryption key name
   */
  public String getEncryptionKey()
  {
    return _encryptionKey;
  }
}
