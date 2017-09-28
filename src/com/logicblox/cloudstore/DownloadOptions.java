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
import java.util.Optional;

/**
 * {@code DownloadOptions} contains all the details needed by the download operation. The specified
 * {@code object}, under {@code bucket}, is downloaded to a local {@code file}.
 * <p>
 * If {@code recursive} is set, then all objects under {@code object} key will be downloaded.
 * Otherwise, only the top-level objects will be downloaded.
 * <p>
 * If {@code overwrite} is set, then newly downloaded files is possible to overwrite existing local
 * files.
 * <p>
 * If progress listener factory has been set, then progress notifications will be recorded.
 * <p>
 * {@code DownloadOptions} objects are meant to be built by {@code DownloadOptionsBuilder}. This
 * class provides only public accessor methods.
 * <p>
 * @see DownloadOptionsBuilder
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#download(DownloadOptions)
 * @see CloudStoreClient#downloadDirectory(DownloadOptions)
 * @see OptionsBuilderFactory#newDownloadOptionsBuilder()
 */
public class DownloadOptions
  extends CommandOptions
{
  private File file;
  private String bucket;
  private String objectKey;
  private boolean recursive;
  private String version;
  private boolean overwrite;
  private boolean dryRun;
  private OverallProgressListenerFactory overallProgressListenerFactory;

  DownloadOptions(
    CloudStoreClient cloudStoreClient, File file, String bucket, String objectKey, String version,
    boolean recursive, boolean overwrite, boolean dryRun,
    OverallProgressListenerFactory overallProgressListenerFactory)
  {
    super(cloudStoreClient);
    this.file = file;
    this.bucket = bucket;
    this.objectKey = objectKey;
    this.recursive = recursive;
    this.version = version;
    this.overwrite = overwrite;
    this.dryRun = dryRun;
    this.overallProgressListenerFactory = overallProgressListenerFactory;
  }

  /**
   * Return the local file (or directory) that will receive the data in the file from the cloud
   * store service.
   */
  public File getFile()
  {
    return file;
  }

  /**
   * Return the name of the bucket containing the file to download.
   */
  public String getBucketName()
  {
    return bucket;
  }

  /**
   * Return the key of the file to be downloaded.
   */
  public String getObjectKey()
  {
    return objectKey;
  }

  /**
   * Return the recursive property of the command.  If this property is true
   * and the key looks like a directory (ends in '/'), all "top-level"
   * and "subdirectory" files will be downloaded.
   */
  public boolean isRecursive()
  {
    return recursive;
  }

  /**
   * Return the version of the file to be downloaded.
   */
  public Optional<String> getVersion()
  {
    return Optional.ofNullable(version);
  }

  /**
   * Return the overwrite property for the download operation.  If false,
   * downloads will fail if they need to overwrite a file that is
   * already on the local file system.
   */
  public boolean doesOverwrite()
  {
    return overwrite;
  }

  /**
   * If set to true, print operations that would be executed, but do not perform them.
   */
  public boolean isDryRun()
  {
    return dryRun;
  }

  /**
   * Return the progress listener that can be used to track download progress.
   */
  public Optional<OverallProgressListenerFactory> getOverallProgressListenerFactory()
  {
    return Optional.ofNullable(overallProgressListenerFactory);
  }
}
