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
 * {@code RenameOptionsBuilder} is a builder for {@code RenameOptions} objects.
 * <p>
 * Setting {@code sourceBucketName}, {@code sourceObjectKey}, {@code
 * destinationBucket} and {@code destinationObjectKey} is mandatory. All the
 * others are optional.
 */
public class RenameOptionsBuilder extends CommandOptionsBuilder
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

  public RenameOptionsBuilder setSourceBucketName(String sourceBucketName)
  {
    _sourceBucketName = sourceBucketName;
    return this;
  }

  public RenameOptionsBuilder setSourceObjectKey(String sourceObjectKey)
  {
    _sourceObjectKey = sourceObjectKey;
    return this;
  }

  public RenameOptionsBuilder setDestinationBucketName(String destinationBucket)
  {
    _destinationBucketName = destinationBucket;
    return this;
  }

  public RenameOptionsBuilder setDestinationObjectKey(String destinationObjectKey)
  {
    _destinationObjectKey = destinationObjectKey;
    return this;
  }

  public RenameOptionsBuilder setCannedAcl(String cannedAcl)
  {
    _cannedAcl = cannedAcl;
    return this;
  }

  public RenameOptionsBuilder setRecursive(boolean recursive)
  {
    _recursive = recursive;
    return this;
  }

  public RenameOptionsBuilder setDryRun(boolean dryRun)
  {
    _dryRun = dryRun;
    return this;
  }

  private void validateOptions()
  {
    if (_cloudStoreClient == null) {
      throw new UsageException("CloudStoreClient has to be set");
    }
    else if (_sourceBucketName == null) {
      throw new UsageException("Source bucket name has to be set");
    }
    else if (_sourceObjectKey == null) {
      throw new UsageException("Source object key has to be set");
    }
    else if (_destinationBucketName == null) {
      throw new UsageException("Destination bucket name key has to be set");
    }
    else if (_destinationObjectKey == null) {
      throw new UsageException("Destination object key has to be set");
    }

    if (_cannedAcl != null) {
      if (!_cloudStoreClient.getAclHandler().isCannedAclValid(_cannedAcl)) {
        throw new UsageException("Invalid canned ACL '" + _cannedAcl + "'");
      }
    }
  }

  @Override
  public RenameOptions createOptions()
  {
    validateOptions();

    return new RenameOptions(_cloudStoreClient, _sourceBucketName,
      _sourceObjectKey, _destinationBucketName, _destinationObjectKey,
      _cannedAcl, _recursive, _dryRun);
  }
}
