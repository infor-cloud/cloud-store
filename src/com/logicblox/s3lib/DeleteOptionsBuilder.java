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

package com.logicblox.s3lib;

public class DeleteOptionsBuilder extends CommandOptionsBuilder
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

  public DeleteOptionsBuilder setBucketName(String bucket)
  {
    _bucket = bucket;
    return this;
  }
  
  public DeleteOptionsBuilder setObjectKey(String objectKey)
  {
    _objectKey = objectKey;
    return this;
  }
  
  public DeleteOptionsBuilder setRecursive(boolean recursive)
  {
    _recursive = recursive;
    return this;
  }
  
  public DeleteOptionsBuilder setDryRun(boolean dryRun)
  {
    _dryRun = dryRun;
    return this;
  }
  
  public DeleteOptionsBuilder setForceDelete(boolean force)
  {
    _forceDelete = force;
    return this;
  }
  
  public DeleteOptionsBuilder setIgnoreAbortInjection(boolean ignore)
  {
    _ignoreAbortInjection = ignore;
    return this;
  }

  private void validateOptions()
  {
    if (_cloudStoreClient == null) {
      throw new UsageException("CloudStoreClient has to be set");
    }
    else if (_bucket == null) {
      throw new UsageException("Bucket has to be set");
    }
    else if (_objectKey == null) {
      throw new UsageException("Object key has to be set");
    }
  }

  @Override
  public DeleteOptions createOptions()
  {
    validateOptions();

    return new DeleteOptions(_cloudStoreClient, _bucket, _objectKey,
      _recursive, _dryRun, _forceDelete, _ignoreAbortInjection);
  }
}
