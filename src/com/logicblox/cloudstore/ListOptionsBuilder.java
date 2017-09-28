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

public class ListOptionsBuilder
  extends CommandOptionsBuilder
{
  private String _bucketName;
  private String _objectKey;
  private boolean _recursive;
  private boolean _includeVersions;
  private boolean _excludeDirs;

  ListOptionsBuilder(CloudStoreClient client)
  {
    _cloudStoreClient = client;
  }

  public ListOptionsBuilder setBucketName(String bucket)
  {
    _bucketName = bucket;
    return this;
  }

  public ListOptionsBuilder setObjectKey(String objectKey)
  {
    _objectKey = objectKey;
    return this;
  }

  public ListOptionsBuilder setRecursive(boolean recursive)
  {
    _recursive = recursive;
    return this;
  }

  public ListOptionsBuilder setIncludeVersions(boolean includeVersions)
  {
    _includeVersions = includeVersions;
    return this;
  }

  public ListOptionsBuilder setExcludeDirs(boolean excludeDirs)
  {
    _excludeDirs = excludeDirs;
    return this;
  }

  private void validateOptions()
  {
    if(_cloudStoreClient == null)
    {
      throw new UsageException("CloudStoreClient has to be set");
    }
    else if(_bucketName == null)
    {
      throw new UsageException("Bucket has to be set");
    }
  }

  @Override
  public ListOptions createOptions()
  {
    validateOptions();

    return new ListOptions(_cloudStoreClient, _bucketName, _objectKey, _recursive, _includeVersions,
      _excludeDirs);
  }
}
