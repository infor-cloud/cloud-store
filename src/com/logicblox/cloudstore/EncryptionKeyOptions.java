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
 * {@code EncryptionKeyOptions} contains all the options needed for adding/removing encryption
 * keys.
 * <p>
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

  public String getBucketName()
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
