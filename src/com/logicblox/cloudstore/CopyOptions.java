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
import java.util.Optional;

/**
 * {@code CopyOptions} contains all the details needed by the copy operation. The specified {@code
 * _sourceObjectKey}, under {@code _sourceBucketName} bucket, is copied to {@code
 * _destinationObjectKey}, under {@code _destinationBucketName}.
 * <p>
 * If {@code _cannedAcl} is specified then it is applied to the destination object.
 * <p>
 * If a progress listener factory has been set, then progress notifications will be recorded.
 * <p>
 * {@code CopyOptions} objects are meant to be built by {@code CopyOptionsBuilder}. This class
 * provides only public accessor methods.
 * 
 * @see CopyOptionsBuilder
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#copy(CopyOptions)
 * @see CloudStoreClient#copyDirectory(CopyOptions)
 * @see OptionsBuilderFactory#newCopyOptionsBuilder()
 */
public class CopyOptions
  extends CommandOptions
{
  private final String _sourceBucketName;
  private final String _sourceObjectKey;
  private final String _destinationBucketName;
  private final String _destinationObjectKey;
  private final boolean _recursive;
  private final boolean _dryRun;
  private final boolean _ignoreAbortInjection;
  private String _cannedAcl;
  private final String _storageClass;
  private final Map<String, String> _userMetadata;
  private final OverallProgressListenerFactory _overallProgressListenerFactory;

  // for testing injection of aborts during a copy
  private static AbortCounters _abortCounters = new AbortCounters();


  CopyOptions(
    CloudStoreClient cloudStoreClient, String sourceBucketName, String sourceObjectKey,
    String destinationBucketName, String destinationObjectKey, String cannedAcl,
    String storageClass, boolean recursive, boolean dryRun, boolean ignoreAbortInjection,
    Map<String, String> userMetadata, OverallProgressListenerFactory overallProgressListenerFactory)
  {
    super(cloudStoreClient);
    _sourceBucketName = sourceBucketName;
    _sourceObjectKey = sourceObjectKey;
    _destinationBucketName = destinationBucketName;
    _destinationObjectKey = destinationObjectKey;
    _recursive = recursive;
    _cannedAcl = cannedAcl;
    _storageClass = storageClass;
    _dryRun = dryRun;
    _ignoreAbortInjection = ignoreAbortInjection;
    _userMetadata = userMetadata;
    _overallProgressListenerFactory = overallProgressListenerFactory;
  }

  // for testing injection of aborts during a copy
  void injectAbort(String id)
  {
    if(!_ignoreAbortInjection && (_abortCounters.decrementInjectionCounter(id) > 0))
    {
      throw new AbortInjection("forcing copy abort");
    }
  }

  static AbortCounters getAbortCounters()
  {
    return _abortCounters;
  }

  /**
   * Return the bucket name containing the file to be copied.
   *
   * @return name of source bucket
   */
  public String getSourceBucketName()
  {
    return _sourceBucketName;
  }

  /**
   * Return the key of the file to be copied.
   *
   * @return key of source file
   */
  public String getSourceObjectKey()
  {
    return _sourceObjectKey;
  }

  /**
   * Return the name of the bucket that will receive the copied file.
   *
   * @return name of destination bucket
   */
  public String getDestinationBucketName()
  {
    return _destinationBucketName;
  }

  /**
   * Return the key of the new file to be created.
   *
   * @return key of destination file
   */
  public String getDestinationObjectKey()
  {
    return _destinationObjectKey;
  }

  /**
   * Return the name of an access control list for the copied file.  If not specified, 
   * the access control list for the original file will be used.
   *
   * @return optional canned access control list name
   */
  public Optional<String> getCannedAcl()
  {
    return Optional.ofNullable(_cannedAcl);
  }

  /**
   * Return the storage class for the copied file.  If not specified, the storage class
   * of the original file is used.
   *
   * @return optional storage class
   */
  public Optional<String> getStorageClass()
  {
    return Optional.ofNullable(_storageClass);
  }

  /**
   * Return the recursive property for the copy operation.  If not set, a single file
   * whose key matches the specified key will be copied if the key does not look like
   * a directory (ends with a '/').  If not sets and the key ends in a '/', then all
   * "top-level" files matching the key will be copied.  If recursive is set and the
   * key ends in '/', all "top-level" and all matching "sub-directory" files will be copied.
   *
   * @return recursive flag
   */
  public boolean isRecursive()
  {
    return _recursive;
  }

  /**
   * Return the dry-run property for the copy operation.  If set to true, print operations 
   * that would be executed, but do not perform them.
   *
   * @return dry-run flag
   */
  public boolean isDryRun()
  {
    return _dryRun;
  }

  /**
   * Return user metadata for the copied file.  If not specified, all user metadata
   * is copied to the new file.
   *
   * @return optional map containing user metadata
   */
  public Optional<Map<String, String>> getUserMetadata()
  {
    return Optional.ofNullable(_userMetadata);
  }

  /**
   * Return an optional {@link OverallProgressListenerFactory progress listener} used to
   * report progress as files are copied.
   * @return optional factory used to create progress listeners
   */
  public Optional<OverallProgressListenerFactory> getOverallProgressListenerFactory()
  {
    return Optional.ofNullable(_overallProgressListenerFactory);
  }
}
