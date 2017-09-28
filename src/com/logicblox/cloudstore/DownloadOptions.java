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
 * {@code object}, under {@code _bucketName}, is downloaded to a local {@code _file}.
 * <p>
 * If {@code _recursive} is set, then all objects under {@code object} key will be downloaded.
 * Otherwise, only the first-level objects will be downloaded.
 * <p>
 * If {@code _overwrite} is set, then newly downloaded files is possible to _overwrite existing local
 * files.
 * <p>
 * If progress listener factory has been set, then progress notifications will be recorded.
 * <p>
 * {@code DownloadOptions} objects are meant to be built by {@code DownloadOptionsBuilder}. This
 * class provides only public getter methods.
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

  public File getFile()
  {
    return _file;
  }

  public String getBucketName()
  {
    return _bucketName;
  }

  public String getObjectKey()
  {
    return _objectKey;
  }

  public boolean isRecursive()
  {
    return _recursive;
  }

  public Optional<String> getVersion()
  {
    return Optional.ofNullable(_version);
  }

  public boolean doesOverwrite()
  {
    return _overwrite;
  }

  public boolean isDryRun()
  {
    return _dryRun;
  }

  public Optional<OverallProgressListenerFactory> getOverallProgressListenerFactory()
  {
    return Optional.ofNullable(_overallProgressListenerFactory);
  }
}
