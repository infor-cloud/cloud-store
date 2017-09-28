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
 * {@code RenameOptionsBuilder} is used to create and set properties {@code RenameOptions} objects
 * used to control the behavior of the cloud-store rename command.
 * <p>
 * Setting {@code sourceBucketName}, {@code sourceObjectKey}, {@code destinationBucket} and 
 * {@code destinationObjectKey} is mandatory. All the others are optional.
 * <p>
 * @see RenameOptions
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#rename(RenameOptions)
 * @see CloudStoreClient#renameDirectory(RenameOptions)
 * @see OptionsBuilderFactory#newRenameOptionsBuilder()
 */
public class RenameOptionsBuilder
  extends CommandOptionsBuilder
{
  private String _sourceBucketName;
  private String _sourceObjectKey;
  private String _destinationBucketName;
  private String _destinationObjectKey;
  private String _cannedAcl;
  private boolean _recursive = false;
  private boolean _dryRun = false;

  RenameOptionsBuilder(CloudStoreClient client)
  {
    _cloudStoreClient = client;
  }

  /**
   * Set the name of the bucket containing the file to be renamed.
   */
  public RenameOptionsBuilder setSourceBucketName(String sourceBucketName)
  {
    _sourceBucketName = sourceBucketName;
    return this;
  }

  /**
   * Set the key of the file to be renamed.
   */
  public RenameOptionsBuilder setSourceObjectKey(String sourceObjectKey)
  {
    _sourceObjectKey = sourceObjectKey;
    return this;
  }

  /**
   * Set the name fo the bucket that will receive the renamed file.
   */
  public RenameOptionsBuilder setDestinationBucketName(String destinationBucket)
  {
    _destinationBucketName = destinationBucket;
    return this;
  }

  /**
   * Set the key of the new file to be created.
   */
  public RenameOptionsBuilder setDestinationObjectKey(String destinationObjectKey)
  {
    _destinationObjectKey = destinationObjectKey;
    return this;
  }

  /**
   * Set the name of an access control list for the renamed file.  If not specified, 
   * the access control list for the original file will be used.
   */
  public RenameOptionsBuilder setCannedAcl(String cannedAcl)
  {
    _cannedAcl = cannedAcl;
    return this;
  }

  /**
   * Set the recursive property for the rename operation.  If not set, a single file
   * whose key matches the specified key will be renamed if the key does not look like
   * a directory (ends with a '/').  If not set and the key ends in a '/', then all
   * "top-level" files matching the key will be renamed.  If recursive is set and the
   * key ends in '/', all "top-level" and all matching "sub-directory" files will be renamed.
   */
  public RenameOptionsBuilder setRecursive(boolean recursive)
  {
    _recursive = recursive;
    return this;
  }

  /**
   * If set to true, print operations that would be executed, but do not perform them.
   */
  public RenameOptionsBuilder setDryRun(boolean dryRun)
  {
    _dryRun = dryRun;
    return this;
  }

  private void validateOptions()
  {
    if(_cloudStoreClient == null)
    {
      throw new UsageException("CloudStoreClient has to be set");
    }
    else if(_sourceBucketName == null)
    {
      throw new UsageException("Source bucket name has to be set");
    }
    else if(_sourceObjectKey == null)
    {
      throw new UsageException("Source object key has to be set");
    }
    else if(_destinationBucketName == null)
    {
      throw new UsageException("Destination bucket name key has to be set");
    }
    else if(_destinationObjectKey == null)
    {
      throw new UsageException("Destination object key has to be set");
    }

    if(_cannedAcl != null)
    {
      if(!_cloudStoreClient.getAclHandler().isCannedAclValid(_cannedAcl))
      {
        throw new UsageException("Invalid canned ACL '" + _cannedAcl + "'");
      }
    }
  }

  /**
   * Validate that all required parameters are set and if so return a new {@link RenameOptions}
   * object.
   */
  @Override
  public RenameOptions createOptions()
  {
    validateOptions();

    return new RenameOptions(_cloudStoreClient, _sourceBucketName, _sourceObjectKey,
      _destinationBucketName, _destinationObjectKey, _cannedAcl, _recursive, _dryRun);
  }
}
