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
 * If {@code cannedAcl} is specified then it's applied to the destination object.
 * <p>
 * If progress listener factory has been set, then progress notifications will be recorded.
 * <p>
 * {@code CopyOptions} objects are meant to be built by {@code CopyOptionsBuilder}. This class
 * provides only public getter methods.
 */
public class CopyOptions extends CommandOptions
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


  CopyOptions(CloudStoreClient cloudStoreClient,
              String sourceBucketName,
              String sourceObjectKey,
              String destinationBucketName,
              String destinationObjectKey,
              String cannedAcl,
              String storageClass,
              boolean recursive,
              boolean dryRun,
              boolean ignoreAbortInjection,
              Map<String, String> userMetadata,
              OverallProgressListenerFactory overallProgressListenerFactory)
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
    if (!this.ignoreAbortInjection && (_abortCounters.decrementInjectionCounter(id) > 0))
    {
      throw new AbortInjection("forcing copy abort");
    }
  }

  static AbortCounters getAbortCounters()
  {
    return _abortCounters;
  }

  public String getSourceBucketName()
  {
    return sourceBucketName;
  }

  public String getSourceObjectKey()
  {
    return sourceObjectKey;
  }

  public String getDestinationBucketName()
  {
    return destinationBucketName;
  }

  public String getDestinationObjectKey()
  {
    return destinationObjectKey;
  }

  public Optional<String> getCannedAcl()
  {
    return Optional.ofNullable(cannedAcl);
  }

  public Optional<String> getStorageClass()
  {
    return Optional.ofNullable(storageClass);
  }

  public boolean isRecursive()
  {
    return recursive;
  }

  public boolean isDryRun()
  {
    return dryRun;
  }

  public Optional<Map<String, String>> getUserMetadata()
  {
    return Optional.ofNullable(userMetadata);
  }

  public Optional<OverallProgressListenerFactory> getOverallProgressListenerFactory()
  {
    return Optional.ofNullable(overallProgressListenerFactory);
  }
}
