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
 * {@code ExistsOptionsBuilder} is used to create and set properties for {@code ExistsOptions} 
 * objects used to control behavior of the cloud-store exists command.
 * <p>
 * Setting {@code bucketName} and {@code objectKey} are mandatory.
 * <p>
 * @see ExistsOptions
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#exists()
 * @see OptionsBuilderFactory#newExistsOptionsBuilder()
 */
public class ExistsOptionsBuilder
  extends CommandOptionsBuilder
{
  private String _bucket;
  private String _objectKey;

  ExistsOptionsBuilder(CloudStoreClient client)
  {
    _cloudStoreClient = client;
  }

  /**
   * Set the name of the bucket containing the file to check.
   */
  public ExistsOptionsBuilder setBucketName(String bucket)
  {
    _bucket = bucket;
    return this;
  }

  /**
   * Set the key of the file to check.
   */
  public ExistsOptionsBuilder setObjectKey(String objectKey)
  {
    _objectKey = objectKey;
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
  }

  /**
   * Validate that all required parameters are set and if so return a new {@link ExistsOptions}
   * object.
   */
  @Override
  public ExistsOptions createOptions()
  {
    validateOptions();

    return new ExistsOptions(_cloudStoreClient, _bucket, _objectKey);
  }
}
