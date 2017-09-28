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
 * {@code RenameOptions} contains all the details needed by the rename operation. The specified
 * {@code sourceObjectKey}, under {@code sourceBucketName} bucket, is renamed to {@code
 * destinationObjectKey}, under {@code destinationBucketName}.
 * <p>
 * If {@code cannedAcl} is specified then it is applied to the destination object.
 * <p>
 * {@code RenameOptions} objects are meant to be built by {@code RenameOptionsBuilder}. This class
 * provides only public accessor methods.
 * <p>
 * @see RenameOptionsBuilder
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#rename()
 * @see CloudStoreClient#renameDirectory()
 * @see OptionsBuilderFactory#newRenameOptionsBuilder()
 */
public class RenameOptions
  extends CommandOptions
{
  private final String _sourceBucketName;
  private final String _sourceObjectKey;
  private final String _destinationBucketName;
  private final String _destinationObjectKey;
  private final boolean _recursive;
  private final boolean _dryRun;
  private String _cannedAcl;

  RenameOptions(
    CloudStoreClient cloudStoreClient, String sourceBucketName, String sourceObjectKey,
    String destinationBucket, String destinationObjectKey, String cannedAcl, boolean recursive,
    boolean dryRun)
  {
    super(cloudStoreClient);
    _sourceBucketName = sourceBucketName;
    _sourceObjectKey = sourceObjectKey;
    _destinationBucketName = destinationBucket;
    _destinationObjectKey = destinationObjectKey;
    _recursive = recursive;
    _cannedAcl = cannedAcl;
    _dryRun = dryRun;
  }

  /**
   * Return the name of the bucket containing the file to be renamed.
   */
  public String getSourceBucketName()
  {
    return _sourceBucketName;
  }

  /**
   * Return the key of the file to be renamed.
   */
  public String getSourceObjectKey()
  {
    return _sourceObjectKey;
  }

  /**
   * Return the name fo the bucket that will receive the renamed file.
   */
  public String getDestinationBucketName()
  {
    return _destinationBucketName;
  }

  /**
   * Return the key of the new file to be created.
   */
  public String getDestinationObjectKey()
  {
    return _destinationObjectKey;
  }

  /**
   * Return the name of an access control list for the renamed file.  If not specified, 
   * the access control list for the original file will be used.
   */
  public Optional<String> getCannedAcl()
  {
    return Optional.ofNullable(_cannedAcl);
  }

  /**
   * Return the recursive property for the rename operation.  If not set, a single file
   * whose key matches the specified key will be renamed if the key does not look like
   * a directory (ends with a '/').  If not set and the key ends in a '/', then all
   * "top-level" files matching the key will be renamed.  If recursive is set and the
   * key ends in '/', all "top-level" and all matching "sub-directory" files will be renamed.
   */
  public boolean isRecursive()
  {
    return _recursive;
  }

  /**
   * If set to true, print operations that would be executed, but do not perform them.
   */
  public boolean isDryRun()
  {
    return _dryRun;
  }

}
