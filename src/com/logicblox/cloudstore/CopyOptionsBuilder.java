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

/**
 * {@code CopyOptionsBuilder} is a builder for {@code CopyOptions} objects.
 * <p>
 * Setting {@code _sourceBucketName}, {@code _sourceObjectKey}, {@code _destinationBucketName} and
 * {@code _destinationObjectKey} is mandatory. All the others are optional.
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

  public CopyOptionsBuilder setSourceBucketName(String sourceBucketName)
  {
    _sourceBucketName = sourceBucketName;
    return this;
  }

  public CopyOptionsBuilder setSourceObjectKey(String sourceObjectKey)
  {
    _sourceObjectKey = sourceObjectKey;
    return this;
  }

  public CopyOptionsBuilder setDestinationBucketName(String destinationBucketName)
  {
    _destinationBucketName = destinationBucketName;
    return this;
  }

  public CopyOptionsBuilder setDestinationObjectKey(String destinationObjectKey)
  {
    _destinationObjectKey = destinationObjectKey;
    return this;
  }

  public CopyOptionsBuilder setCannedAcl(String cannedAcl)
  {
    _cannedAcl = cannedAcl;
    return this;
  }

  public CopyOptionsBuilder setUserMetadata(Map<String, String> userMetadata)
  {
    _userMetadata = userMetadata;
    return this;
  }

  public CopyOptionsBuilder setStorageClass(String storageClass)
  {
    _storageClass = storageClass;
    return this;
  }

  public CopyOptionsBuilder setRecursive(boolean recursive)
  {
    _recursive = recursive;
    return this;
  }

  public CopyOptionsBuilder setDryRun(boolean dryRun)
  {
    _dryRun = dryRun;
    return this;
  }

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

  @Override
  public CopyOptions createOptions()
  {
    validateOptions();

    return new CopyOptions(_cloudStoreClient, _sourceBucketName, _sourceObjectKey,
      _destinationBucketName, _destinationObjectKey, _cannedAcl, _storageClass, _recursive, _dryRun,
      _ignoreAbortInjection, _userMetadata, _overallProgressListenerFactory);
  }
}
