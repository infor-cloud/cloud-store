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
 * {@code DeleteOptionsBuilder} is used to create and set properties for {@code DeleteOptions} 
 * objects used to control behavior of the cloud-store delete command.
 * <p>
 * Setting {@code bucketName} and {@code objectKey} are mandatory. All the others are optional.
 * 
 * @see DeleteOptions
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#delete(DeleteOptions)
 * @see CloudStoreClient#deleteDirectory(DeleteOptions)
 * @see OptionsBuilderFactory#newDeleteOptionsBuilder()
 */
public class DeleteOptionsBuilder
  extends CommandOptionsBuilder
{
  private String _bucket = null;
  private String _objectKey = null;
  private boolean _recursive = false;
  private boolean _dryRun = false;
  private boolean _forceDelete = false;
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
   * Set the recursive property of the command.  If true and if the object key
   * looks like a directory name (ends in '/'), all files that recursively
   * have the key as their prefix will be deleted.
   *
   * @param recursive true to recursively delete files
   * @return this builder
   */
  public DeleteOptionsBuilder setRecursive(boolean recursive)
  {
    _recursive = recursive;
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
   * If forceDelete is set to true, then delete command will complete successfully
   * even if the specified file does not exist.  Otherwise, the delete command
   * will fail when trying to delete a file that does not exist.
   *
   * @param force true of delete should succeed if file does not exist
   * @return this builder
   */
  public DeleteOptionsBuilder setForceDelete(boolean force)
  {
    _forceDelete = force;
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

    return new DeleteOptions(_cloudStoreClient, _bucket, _objectKey, _recursive, _dryRun,
      _forceDelete, _ignoreAbortInjection);
  }
}
