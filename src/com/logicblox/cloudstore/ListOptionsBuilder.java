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

/**
 * {@code ListOptionsBuilder} is used to create and set properties for {@code ListOptions} 
 * objects used to control behavior of the cloud-store list command.
 * <p>
 * Setting {@code bucketName} is mandatory.  Other properties are optional.
 * 
 * @see ListOptions
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#listObjects(ListOptions)
 * @see OptionsBuilderFactory#newListOptionsBuilder()
 */
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

  /**
   * Set the name of the bucket containing files to list.
   *
   * @param bucket bucket name
   * @return this builder
   */
  public ListOptionsBuilder setBucketName(String bucket)
  {
    _bucketName = bucket;
    return this;
  }

  /**
   * Set the key of the file or file prefix to be matched to files to be listed.
   *
   * @param objectKey file key
   * @return this builder
   */
  public ListOptionsBuilder setObjectKey(String objectKey)
  {
    _objectKey = objectKey;
    return this;
  }

  /**
   * Set recursive option for the command.  If true, recursively list files
   * from all subdirectories as well as top-level directories.
   *
   * @param recursive true to set recursive list flag
   * @return this builder
   */
  public ListOptionsBuilder setRecursive(boolean recursive)
  {
    _recursive = recursive;
    return this;
  }

  /**
   * If set to true, list all version information for files.
   *
   * @param includeVersions true to list all versions of files
   * @return this builder
   */
  public ListOptionsBuilder setIncludeVersions(boolean includeVersions)
  {
    _includeVersions = includeVersions;
    return this;
  }

  /**
   * If set to true, do not list any files that look like directories
   * (end with '/').
   *
   * @param excludeDirs true to exclude directories from the list
   * @return this builder
   */
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

  /**
   * Validate that all required parameters are set and if so return a new {@link ListOptions}
   * object.
   *
   * @return immutable options object with values from this builder
   */
  @Override
  public ListOptions createOptions()
  {
    validateOptions();

    return new ListOptions(_cloudStoreClient, _bucketName, _objectKey, _recursive, _includeVersions,
      _excludeDirs);
  }
}
