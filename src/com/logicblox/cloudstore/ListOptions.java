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

import java.util.Optional;

/**
 * {@code ListOptions} contains all the details needed by the cloud-store list command.
 * <p>
 * @see ListOptionsBuilder
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#listObjects()
 * @see OptionsBuilderFactory#newListOptionsBuilder()
 */
public class ListOptions
  extends CommandOptions
{
  private String bucket;
  private String objectKey;
  private boolean recursive;
  private boolean includeVersions;
  private boolean excludeDirs;

  ListOptions(
    CloudStoreClient cloudStoreClient, String bucket, String objectKey, boolean recursive,
    boolean includeVersions, boolean excludeDirs)
  {
    super(cloudStoreClient);
    this.bucket = bucket;
    this.objectKey = objectKey;
    this.recursive = recursive;
    this.includeVersions = includeVersions;
    this.excludeDirs = excludeDirs;
  }

  /**
   * Return the name of the bucket containing files to list.
   */
  public String getBucketName()
  {
    return bucket;
  }

  /**
   * Return the key of the file or file prefix to be matched to files to be listed.
   */
  public Optional<String> getObjectKey()
  {
    return Optional.ofNullable(objectKey);
  }

  /**
   * Return the recursive option for the command.  If true, recursively list files
   * from all subdirectories as well as top-level directories.
   */
  public boolean isRecursive()
  {
    return recursive;
  }

  /**
   * If set to true, list all version information for files.
   */
  public boolean versionsIncluded()
  {
    return includeVersions;
  }

  /**
   * If set to true, do not list any files that look like directories
   * (end with '/').
   */
  public boolean dirsExcluded()
  {
    return excludeDirs;
  }

}
