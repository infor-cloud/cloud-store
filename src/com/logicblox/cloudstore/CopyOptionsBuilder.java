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

import java.util.Map;

/**
 * {@code CopyOptionsBuilder} is used to create and set properties for {@code CopyOptions} objects
 * used to control the behavior of the cloud-store copy command.
 * <p>
 * Setting {@code _sourceBucketName}, {@code _sourceObjectKey}, {@code _destinationBucketName} and
 * {@code _destinationObjectKey} are mandatory. All the others are optional.
 * 
 * @see CopyOptions
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#copy(CopyOptions)
 * @see CloudStoreClient#copyDirectory(CopyOptions)
 * @see OptionsBuilderFactory#newCopyOptionsBuilder()
 */
public class CopyOptionsBuilder
  extends CommandOptionsBuilder
{
  private String _sourceBucketName;
  private String _sourceObjectKey;
  private String _destinationBucketName;
  private String _destinationObjectKey;
  private String _storageClass;
  private boolean _recursive = false;
  private String _cannedAcl;
  private Map<String, String> _userMetadata;
  private boolean _dryRun = false;
  private boolean _ignoreAbortInjection = false;
  private OverallProgressListenerFactory _overallProgressListenerFactory;

  CopyOptionsBuilder(CloudStoreClient client)
  {
    _cloudStoreClient = client;
  }

  /**
   * Set the bucket name containing the file to be copied.
   *
   * @param sourceBucketName name of source bucket
   * @return this builder
   */
  public CopyOptionsBuilder setSourceBucketName(String sourceBucketName)
  {
    _sourceBucketName = sourceBucketName;
    return this;
  }

  /**
   * Set the key of the file to be copied.
   *
   * @param sourceObjectKey key of source file
   * @return this builder
   */
  public CopyOptionsBuilder setSourceObjectKey(String sourceObjectKey)
  {
    _sourceObjectKey = sourceObjectKey;
    return this;
  }

  /**
   * Set the name of the bucket that will receive the copied file.
   *
   * @param destinationBucketName name of destination bucket
   * @return this builder
   */
  public CopyOptionsBuilder setDestinationBucketName(String destinationBucketName)
  {
    _destinationBucketName = destinationBucketName;
    return this;
  }

  /**
   * Set the key of the new file to be created.
   *
   * @param destinationObjectKey key of destination file
   * @return this builder
   */
  public CopyOptionsBuilder setDestinationObjectKey(String destinationObjectKey)
  {
    _destinationObjectKey = destinationObjectKey;
    return this;
  }

  /**
   * Set the name of an access control list for the copied file.  If not specified, 
   * the access control list for the original file will be used.
   *
   * @param cannedAcl name of access control list for the new file
   * @return this builder
   */
  public CopyOptionsBuilder setCannedAcl(String cannedAcl)
  {
    _cannedAcl = cannedAcl;
    return this;
  }

  /**
   * Set user metadata for the copied file.  If not specified, all user metadata
   * is copied to the new file.
   *
   * @param userMetadata map of user metadata for new file
   * @return this builder
   */
  public CopyOptionsBuilder setUserMetadata(Map<String, String> userMetadata)
  {
    _userMetadata = userMetadata;
    return this;
  }

  /**
   * Set the storage class for the copied file.  If not specified, the storage class
   * of the original file is used.
   *
   * @param storageClass storage class for new file
   * @return this builder
   */
  public CopyOptionsBuilder setStorageClass(String storageClass)
  {
    _storageClass = storageClass;
    return this;
  }

  /**
   * Set the recursive property for the copy operation.  If not set, a single file
   * whose key matches the specified key will be copied if the key does not look like
   * a directory (ends with a '/').  If not set and the key ends in a '/', then all
   * "top-level" files matching the key will be copied.  If recursive is set and the
   * key ends in '/', all "top-level" and all matching "sub-directory" files will be copied.
   *
   * @param recursive true if a recursive copy should be performed
   * @return this builder
   */
  public CopyOptionsBuilder setRecursive(boolean recursive)
  {
    _recursive = recursive;
    return this;
  }

  /**
   * If set to true, print operations that would be executed, but do not perform them.
   *
   * @param dryRun true if operations are to be printed but not executed
   * @return this builder
   */
  public CopyOptionsBuilder setDryRun(boolean dryRun)
  {
    _dryRun = dryRun;
    return this;
  }

  /**
   * Used by test framework to control abort injection testing.
   *
   * @param ignore true if abort injection checks shoudl be skipped
   * @return this builder
   */
  public CopyOptionsBuilder setIgnoreAbortInjection(boolean ignore)
  {
    _ignoreAbortInjection = ignore;
    return this;
  }

  // Disabled progress listener since AWS S3 copy progress indicator doesn't
  // notify about the copied bytes.
  //    public CopyOptionsBuilder setOverallProgressListenerFactory
  //        (OverallProgressListenerFactory _overallProgressListenerFactory) {
  //            _overallProgressListenerFactory = Optional.fromNullable
  //            (_overallProgressListenerFactory);
  //        return this;
  //    }

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

    if(_storageClass != null)
    {
      if(!_cloudStoreClient.getStorageClassHandler().isStorageClassValid(_storageClass))
      {
        throw new UsageException("Invalid storage class '" + _storageClass + "'");
      }
    }
  }

  /**
   * Validate that all required parameters are set and if so return a new {@link CopyOptions}
   * object.
   *
   * @return immutable options with all values from this builder
   */
  @Override
  public CopyOptions createOptions()
  {
    validateOptions();

    return new CopyOptions(_cloudStoreClient, _sourceBucketName, _sourceObjectKey,
      _destinationBucketName, _destinationObjectKey, _cannedAcl, _storageClass, _recursive, _dryRun,
      _ignoreAbortInjection, _userMetadata, _overallProgressListenerFactory);
  }
}
