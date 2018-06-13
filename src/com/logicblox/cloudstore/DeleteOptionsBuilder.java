/*
  Copyright 2018, Infor Inc.

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
 * {@code DeleteOptionsBuilder} is used to create and set properties for {@code DeleteOptions} 
 * objects used to control behavior of the cloud-store delete command.
 * <p>
 * Setting {@code bucketName} and {@code objectKey} are mandatory. All the others are optional.
 * 
 * @see DeleteOptions
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#delete(DeleteOptions)
 * @see CloudStoreClient#deleteRecursively(DeleteOptions)
 * @see OptionsBuilderFactory#newDeleteOptionsBuilder()
 */
public class DeleteOptionsBuilder
  extends CommandOptionsBuilder
{
  private String _bucket = null;
  private String _objectKey = null;
  private boolean _dryRun = false;
  private boolean _ignoreAbortInjection = false;

  DeleteOptionsBuilder(CloudStoreClient client)
  {
    _cloudStoreClient = client;
  }

  /**
   * Set the name of the bucket containing the file to delete.
   *
   * @param bucket name of bucket
   * @return this builder
   */
  public DeleteOptionsBuilder setBucketName(String bucket)
  {
    _bucket = bucket;
    return this;
  }

  /**
   * Set the key of the file to delete.
   *
   * @param objectKey key of file to delete
   * @return this builder
   */
  public DeleteOptionsBuilder setObjectKey(String objectKey)
  {
    _objectKey = objectKey;
    return this;
  }

  /**
   * If set to true, print operations that would be executed, but do not perform them.
   *
   * @param dryRun true if operations should be printed but not executed
   * @return this builder
   */
  public DeleteOptionsBuilder setDryRun(boolean dryRun)
  {
    _dryRun = dryRun;
    return this;
  }

  /**
   * Used by test framework to control abort injection testing.
   *
   * @param ignore true if abort injection checks should be skipped
   * @return this builder
   */
  public DeleteOptionsBuilder setIgnoreAbortInjection(boolean ignore)
  {
    _ignoreAbortInjection = ignore;
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
   * Validate that all required parameters are set and if so return a new {@link DeleteOptions}
   * object.
   *
   * @return immutable options with values from this builder
   */
  @Override
  public DeleteOptions createOptions()
  {
    validateOptions();

    return new DeleteOptions(_cloudStoreClient, _bucket, _objectKey, _dryRun, _ignoreAbortInjection);
  }
}
