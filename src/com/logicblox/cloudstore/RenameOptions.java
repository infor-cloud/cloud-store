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
 * 
 * @see RenameOptionsBuilder
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#rename(RenameOptions)
 * @see CloudStoreClient#renameRecursively(RenameOptions)
 * @see OptionsBuilderFactory#newRenameOptionsBuilder()
 */
public class RenameOptions
  extends CommandOptions
{
  private final String _sourceBucketName;
  private final String _sourceObjectKey;
  private final String _destinationBucketName;
  private final String _destinationObjectKey;
  private final boolean _dryRun;
  private String _cannedAcl;

  RenameOptions(
    CloudStoreClient cloudStoreClient, String sourceBucketName, String sourceObjectKey,
    String destinationBucket, String destinationObjectKey, String cannedAcl, boolean dryRun)
  {
    super(cloudStoreClient);
    _sourceBucketName = sourceBucketName;
    _sourceObjectKey = sourceObjectKey;
    _destinationBucketName = destinationBucket;
    _destinationObjectKey = destinationObjectKey;
    _cannedAcl = cannedAcl;
    _dryRun = dryRun;
  }

  /**
   * Return the name of the bucket containing the file to be renamed.
   *
   * @return source bucket name
   */
  public String getSourceBucketName()
  {
    return _sourceBucketName;
  }

  /**
   * Return the key of the file to be renamed.
   *
   * @return source file key
   */
  public String getSourceObjectKey()
  {
    return _sourceObjectKey;
  }

  /**
   * Return the name fo the bucket that will receive the renamed file.
   *
   * @return destination bucket name
   */
  public String getDestinationBucketName()
  {
    return _destinationBucketName;
  }

  /**
   * Return the key of the new file to be created.
   *
   * @return destination file key
   */
  public String getDestinationObjectKey()
  {
    return _destinationObjectKey;
  }

  /**
   * Return the name of an access control list for the renamed file.  If not specified, 
   * the access control list for the original file will be used.
   *
   * @return optional canned access control list name
   */
  public Optional<String> getCannedAcl()
  {
    return Optional.ofNullable(_cannedAcl);
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

}
