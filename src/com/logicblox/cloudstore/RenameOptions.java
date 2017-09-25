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

import java.util.Optional;

/**
 * {@code RenameOptions} contains all the details needed by the rename operation. The specified
 * {@code sourceObjectKey}, under {@code sourceBucketName} bucket, is renamed to {@code
 * destinationObjectKey}, under {@code destinationBucketName}.
 * <p>
 * If {@code cannedAcl} is specified then it's applied to the destination object.
 * <p>
 * {@code RenameOptions} objects are meant to be built by {@code RenameOptionsBuilder}. This class
 * provides only public getter methods.
 */
public class RenameOptions
  extends CommandOptions
{
  private final String _sourceBucketName;
  private final String _sourceObjectKey;
  private final String _destinationBucketName;
  private final String _destinationObjectKey;
  private final boolean _recursive;
  private final boolean _dryRun;
  private String _cannedAcl;

  RenameOptions(
    CloudStoreClient cloudStoreClient, String sourceBucketName, String sourceObjectKey,
    String destinationBucket, String destinationObjectKey, String cannedAcl, boolean recursive,
    boolean dryRun)
  {
    super(cloudStoreClient);
    _sourceBucketName = sourceBucketName;
    _sourceObjectKey = sourceObjectKey;
    _destinationBucketName = destinationBucket;
    _destinationObjectKey = destinationObjectKey;
    _recursive = recursive;
    _cannedAcl = cannedAcl;
    _dryRun = dryRun;
  }

  public String getSourceBucketName()
  {
    return _sourceBucketName;
  }

  public String getSourceObjectKey()
  {
    return _sourceObjectKey;
  }

  public String getDestinationBucketName()
  {
    return _destinationBucketName;
  }

  public String getDestinationObjectKey()
  {
    return _destinationObjectKey;
  }

  public Optional<String> getCannedAcl()
  {
    return Optional.ofNullable(_cannedAcl);
  }

  public boolean isRecursive()
  {
    return _recursive;
  }

  public boolean isDryRun()
  {
    return _dryRun;
  }

}
