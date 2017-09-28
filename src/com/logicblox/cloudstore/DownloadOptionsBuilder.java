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

import java.io.File;

/**
 * {@code DownloadOptionsBuilder} is a builder for {@code DownloadOptions} objects.
 * <p>
 * Setting fields {@code _file}, {@code _bucketName} and {@code _objectKey} is mandatory. All the others
 * are optional.
 */
public class DownloadOptionsBuilder
  extends CommandOptionsBuilder
{
  private File _file;
  private String _bucketName;
  private String _objectKey;
  private String _version;
  private boolean _recursive = false;
  private boolean _overwrite = false;
  private boolean _dryRun = false;
  private OverallProgressListenerFactory _overallProgressListenerFactory;

  DownloadOptionsBuilder(CloudStoreClient client)
  {
    _cloudStoreClient = client;
  }

  public DownloadOptionsBuilder setFile(File file)
  {
    _file = file;
    return this;
  }

  public DownloadOptionsBuilder setBucketName(String bucket)
  {
    _bucketName = bucket;
    return this;
  }

  public DownloadOptionsBuilder setObjectKey(String objectKey)
  {
    _objectKey = objectKey;
    return this;
  }

  public DownloadOptionsBuilder setRecursive(boolean recursive)
  {
    _recursive = recursive;
    return this;
  }

  public DownloadOptionsBuilder setVersion(String version)
  {
    _version = version;
    return this;
  }

  public DownloadOptionsBuilder setOverwrite(boolean overwrite)
  {
    _overwrite = overwrite;
    return this;
  }

  public DownloadOptionsBuilder setDryRun(boolean dryRun)
  {
    _dryRun = dryRun;
    return this;
  }

  public DownloadOptionsBuilder setOverallProgressListenerFactory(
    OverallProgressListenerFactory overallProgressListenerFactory)
  {
    _overallProgressListenerFactory = overallProgressListenerFactory;
    return this;
  }

  private void validateOptions()
  {
    if(_cloudStoreClient == null)
    {
      throw new UsageException("CloudStoreClient has to be set");
    }
    else if(_file == null)
    {
      throw new UsageException("File has to be set");
    }
    else if(_bucketName == null)
    {
      throw new UsageException("Bucket has to be set");
    }
    else if(_objectKey == null)
    {
      throw new UsageException("Object key has to be set");
    }
  }

  @Override
  public DownloadOptions createOptions()
  {
    validateOptions();

    return new DownloadOptions(_cloudStoreClient, _file, _bucketName, _objectKey, _version,
      _recursive, _overwrite, _dryRun, _overallProgressListenerFactory);
  }
}
