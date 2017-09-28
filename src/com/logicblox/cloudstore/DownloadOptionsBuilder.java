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
 * Fields {@code file}, {@code bucket} and {@code objectKey} are mandatory. All the others
 * are optional.
 * <p>
 * @see DownloadOptions
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#download(DownloadOptions)
 * @see CloudStoreClient#downloadDirectory(DownloadOptions)
 * @see OptionsBuilderFactory#newDownloadOptionsBuilder()
 */
public class DownloadOptionsBuilder
  extends CommandOptionsBuilder
{
  private File file;
  private String bucket;
  private String objectKey;
  private String version;
  private boolean recursive = false;
  private boolean overwrite = false;
  private boolean dryRun = false;
  private OverallProgressListenerFactory overallProgressListenerFactory;

  DownloadOptionsBuilder(CloudStoreClient client)
  {
    _cloudStoreClient = client;
  }

  /**
   * Set the local file (or directory) that will receive the data in the file from the cloud
   * store service.
   */
  public DownloadOptionsBuilder setFile(File file)
  {
    this.file = file;
    return this;
  }

  /**
   * Set the name of the bucket containing the file to download.
   */
  public DownloadOptionsBuilder setBucketName(String bucket)
  {
    this.bucket = bucket;
    return this;
  }

  /**
   * Set the key of the file to be downloaded.
   */
  public DownloadOptionsBuilder setObjectKey(String objectKey)
  {
    this.objectKey = objectKey;
    return this;
  }

  /**
   * Set the recursive property of the command.  If this property is true
   * and the key looks like a directory (ends in '/'), all "top-level"
   * and "subdirectory" files will be downloaded.
   */
  public DownloadOptionsBuilder setRecursive(boolean recursive)
  {
    this.recursive = recursive;
    return this;
  }

  /**
   * Set the version of the file to be downloaded.
   */
  public DownloadOptionsBuilder setVersion(String version)
  {
    this.version = version;
    return this;
  }

  /**
   * Set the overwrite property for the download operation.  If false,
   * downloads will fail if they need to overwrite a file that is
   * already on the local file system.
   */
  public DownloadOptionsBuilder setOverwrite(boolean overwrite)
  {
    this.overwrite = overwrite;
    return this;
  }

  /**
   * If set to true, print operations that would be executed, but do not perform them.
   */
  public DownloadOptionsBuilder setDryRun(boolean dryRun)
  {
    this.dryRun = dryRun;
    return this;
  }

  /**
   * Set a progress listener that can be used to track download progress.
   */
  public DownloadOptionsBuilder setOverallProgressListenerFactory(
    OverallProgressListenerFactory overallProgressListenerFactory)
  {
    this.overallProgressListenerFactory = overallProgressListenerFactory;
    return this;
  }

  private void validateOptions()
  {
    if(_cloudStoreClient == null)
    {
      throw new UsageException("CloudStoreClient has to be set");
    }
    else if(file == null)
    {
      throw new UsageException("File has to be set");
    }
    else if(bucket == null)
    {
      throw new UsageException("Bucket has to be set");
    }
    else if(objectKey == null)
    {
      throw new UsageException("Object key has to be set");
    }
  }

  /**
   * Validate that all required parameters are set and if so return a new {@link DownloadOptions}
   * object.
   */
  @Override
  public DownloadOptions createOptions()
  {
    validateOptions();

    return new DownloadOptions(_cloudStoreClient, file, bucket, objectKey, version, recursive,
      overwrite, dryRun, overallProgressListenerFactory);
  }
}
