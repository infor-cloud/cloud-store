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
 * {@code DownloadOptionsBuilder} is a builder for {@code DownloadOptions} objects, used
 * to control the behavior of the cloud-store download command.  This can be used to
 * download both individual files and all files in directories.
 * <p>
 * Fields {@code _file}, {@code _bucketName} and {@code _objectKey} are mandatory. All the others
 * are optional.
 * 
 * @see DownloadOptions
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#download(DownloadOptions)
 * @see CloudStoreClient#downloadDirectory(DownloadOptions)
 * @see OptionsBuilderFactory#newDownloadOptionsBuilder()
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

  /**
   * Set the local file (or directory) that will receive the data in the file from the cloud
   * store service.
   *
   * @param file local file to receive data from cloud file
   * @return this builder
   */
  public DownloadOptionsBuilder setFile(File file)
  {
    _file = file;
    return this;
  }

  /**
   * Set the name of the bucket containing the file to download.
   *
   * @param bucket name of bucket
   * @return this builder
   */
  public DownloadOptionsBuilder setBucketName(String bucket)
  {
    _bucketName = bucket;
    return this;
  }

  /**
   * Set the key of the file to be downloaded.
   *
   * @param objectKey key of file to download
   * @return this builder
   */
  public DownloadOptionsBuilder setObjectKey(String objectKey)
  {
    _objectKey = objectKey;
    return this;
  }

  /**
   * Set the recursive property of the command.  If this property is true
   * and the key looks like a directory (ends in '/'), all "top-level"
   * and "subdirectory" files will be downloaded.
   *
   * @param recursive recursive flag
   * @return this builder
   */
  public DownloadOptionsBuilder setRecursive(boolean recursive)
  {
    _recursive = recursive;
    return this;
  }

  /**
   * Set the version of the file to be downloaded.
   *
   * @param version version of file to download
   * @return this builder
   */
  public DownloadOptionsBuilder setVersion(String version)
  {
    _version = version;
    return this;
  }

  /**
   * Set the overwrite property for the download operation.  If false,
   * downloads will fail if they need to overwrite a file that is
   * already on the local file system.
   *
   * @param overwrite overwrite flag
   * @return this builder
   */
  public DownloadOptionsBuilder setOverwrite(boolean overwrite)
  {
    _overwrite = overwrite;
    return this;
  }

  /**
   * If set to true, print operations that would be executed, but do not perform them.
   *
   * @param dryRun dry-run flag
   * @return this builder
   */
  public DownloadOptionsBuilder setDryRun(boolean dryRun)
  {
    _dryRun = dryRun;
    return this;
  }

  /**
   * Set a progress listener that can be used to track download progress.
   *
   * @param overallProgressListenerFactory factory to create progress listeners
   * @return this builder
   */
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

  /**
   * Validate that all required parameters are set and if so return a new {@link DownloadOptions}
   * object.
   *
   * @return immutable options object with values from this builder
   */
  @Override
  public DownloadOptions createOptions()
  {
    validateOptions();

    return new DownloadOptions(_cloudStoreClient, _file, _bucketName, _objectKey, _version,
      _recursive, _overwrite, _dryRun, _overallProgressListenerFactory);
  }
}
