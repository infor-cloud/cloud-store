/*
  Copyright 2018, Infor Inc.

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
 * {@code object}, under {@code _bucketName}, is downloaded to a local {@code _file}.
 * <p>
 * If {@code _recursive} is set, then all objects under {@code _objectKey} key will be downloaded.
 * Otherwise, only the top-level objects will be downloaded.
 * <p>
 * If {@code _overwrite} is set, then newly downloaded files is possible to _overwrite existing local
 * files.
 * <p>
 * If progress listener factory has been set, then progress notifications will be recorded.
 * <p>
 * {@code DownloadOptions} objects are meant to be built by {@code DownloadOptionsBuilder}. This
 * class provides only public accessor methods.
 * 
 * @see DownloadOptionsBuilder
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#download(DownloadOptions)
 * @see CloudStoreClient#downloadDirectory(DownloadOptions)
 * @see OptionsBuilderFactory#newDownloadOptionsBuilder()
 */
public class DownloadOptions
  extends CommandOptions
{
  private File _file;
  private String _bucketName;
  private String _objectKey;
  private boolean _recursive;
  private String _version;
  private boolean _overwrite;
  private boolean _dryRun;
  private OverallProgressListenerFactory _overallProgressListenerFactory;

  DownloadOptions(
    CloudStoreClient cloudStoreClient, File file, String bucketName, String objectKey, String version,
    boolean recursive, boolean overwrite, boolean dryRun,
    OverallProgressListenerFactory overallProgressListenerFactory)
  {
    super(cloudStoreClient);
    _file = file;
    _bucketName = bucketName;
    _objectKey = objectKey;
    _recursive = recursive;
    _version = version;
    _overwrite = overwrite;
    _dryRun = dryRun;
    _overallProgressListenerFactory = overallProgressListenerFactory;
  }

  /**
   * Return the local file (or directory) that will receive the data in the file from the cloud
   * store service.
   *
   * @return downloaded file object
   */
  public File getFile()
  {
    return _file;
  }

  /**
   * Return the name of the bucket containing the file to download.
   *
   * @return bucket name
   */
  public String getBucketName()
  {
    return _bucketName;
  }

  /**
   * Return the key of the file to be downloaded.
   *
   * @return downloaded file key
   */
  public String getObjectKey()
  {
    return _objectKey;
  }

  /**
   * Return the recursive property of the command.  If this property is true
   * and the key looks like a directory (ends in '/'), all "top-level"
   * and "subdirectory" files will be downloaded.
   *
   * @return recursive flag
   */
  public boolean isRecursive()
  {
    return _recursive;
  }

  /**
   * Return the version of the file to be downloaded.
   *
   * @return optional version of downloaded file
   */
  public Optional<String> getVersion()
  {
    return Optional.ofNullable(_version);
  }

  /**
   * Return the overwrite property for the download operation.  If false,
   * downloads will fail if they need to overwrite a file that is
   * already on the local file system.
   *
   * @return overwrite flag
   */
  public boolean doesOverwrite()
  {
    return _overwrite;
  }

  /**
   * If set to true, print operations that would be executed, but do not perform them.
   *
   * @return dry-run flag
   */
  public boolean isDryRun()
  {
    return _dryRun;
  }

  /**
   * Return the progress listener that can be used to track download progress.
   *
   * @return optional factory used to create progress listeners
   */
  public Optional<OverallProgressListenerFactory> getOverallProgressListenerFactory()
  {
    return Optional.ofNullable(_overallProgressListenerFactory);
  }
}
