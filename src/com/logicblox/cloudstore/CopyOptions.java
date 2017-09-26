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

import java.util.Map;
import java.util.Optional;

/**
 * {@code CopyOptions} contains all the details needed by the copy operation. The specified {@code
 * sourceObjectKey}, under {@code sourceBucketName} bucket, is copied to {@code
 * destinationObjectKey}, under {@code destinationBucketName}.
 * <p>
 * If {@code cannedAcl} is specified then it is applied to the destination object.
 * <p>
 * If a progress listener factory has been set, then progress notifications will be recorded.
 * <p>
 * {@code CopyOptions} objects are meant to be built by {@code CopyOptionsBuilder}. This class
 * provides only public accessor methods.
 * <p>
 * @see CopyOptionsBuilder
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#copy()
 * @see CloudStoreClient#copyToDir()
 * @see OptionsBuilderFactory#newCopyOptionsBuilder()
 */
public class CopyOptions
  extends CommandOptions
{
  private final String sourceBucketName;
  private final String sourceObjectKey;
  private final String destinationBucketName;
  private final String destinationObjectKey;
  private final boolean recursive;
  private final boolean dryRun;
  private final boolean ignoreAbortInjection;
  private String cannedAcl;
  private final String storageClass;
  private final Map<String, String> userMetadata;
  private final OverallProgressListenerFactory overallProgressListenerFactory;

  // for testing injection of aborts during a copy
  private static AbortCounters _abortCounters = new AbortCounters();


  CopyOptions(
    CloudStoreClient cloudStoreClient, String sourceBucketName, String sourceObjectKey,
    String destinationBucketName, String destinationObjectKey, String cannedAcl,
    String storageClass, boolean recursive, boolean dryRun, boolean ignoreAbortInjection,
    Map<String, String> userMetadata, OverallProgressListenerFactory overallProgressListenerFactory)
  {
    super(cloudStoreClient);
    this.sourceBucketName = sourceBucketName;
    this.sourceObjectKey = sourceObjectKey;
    this.destinationBucketName = destinationBucketName;
    this.destinationObjectKey = destinationObjectKey;
    this.recursive = recursive;
    this.cannedAcl = cannedAcl;
    this.storageClass = storageClass;
    this.dryRun = dryRun;
    this.ignoreAbortInjection = ignoreAbortInjection;
    this.userMetadata = userMetadata;
    this.overallProgressListenerFactory = overallProgressListenerFactory;
  }

  // for testing injection of aborts during a copy
  void injectAbort(String id)
  {
    if(!this.ignoreAbortInjection && (_abortCounters.decrementInjectionCounter(id) > 0))
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
   */
  public String getSourceBucketName()
  {
    return sourceBucketName;
  }

  /**
   * Return the key of the file to be copied.
   */
  public String getSourceObjectKey()
  {
    return sourceObjectKey;
  }

  /**
   * Return the name of the bucket that will receive the copied file.
   */
  public String getDestinationBucketName()
  {
    return destinationBucketName;
  }

  /**
   * Return the key of the new file to be created.
   */
  public String getDestinationObjectKey()
  {
    return destinationObjectKey;
  }

  /**
   * Return the name of an access control list for the copied file.  If not specified, 
   * the access control list for the original file will be used.
   */
  public Optional<String> getCannedAcl()
  {
    return Optional.ofNullable(cannedAcl);
  }

  /**
   * Return the storage class for the copied file.  If not specified, the storage class
   * of the original file is used.
   */
  public Optional<String> getStorageClass()
  {
    return Optional.ofNullable(storageClass);
  }

  /**
   * Return the recursive property for the copy operation.  If not set, a single file
   * whose key matches the specified key will be copied if the key does not look like
   * a directory (ends with a '/').  If not sets and the key ends in a '/', then all
   * "top-level" files matching the key will be copied.  If recursive is set and the
   * key ends in '/', all "top-level" and all matching "sub-directory" files will be copied.
   */
  public boolean isRecursive()
  {
    return recursive;
  }

  /**
   * Return the dry-run property for the copy operation.  If set to true, print operations 
   * that would be executed, but do not perform them.
   */
  public boolean isDryRun()
  {
    return dryRun;
  }

  /**
   * Return user metadata for the copied file.  If not specified, all user metadata
   * is copied to the new file.
   */
  public Optional<Map<String, String>> getUserMetadata()
  {
    return Optional.ofNullable(userMetadata);
  }

  /**
   * Return an optional {@link OverallprogressListenerFactory progress listener} used to
   * report progress as files are copied.
   */
  public Optional<OverallProgressListenerFactory> getOverallProgressListenerFactory()
  {
    return Optional.ofNullable(overallProgressListenerFactory);
  }
}
