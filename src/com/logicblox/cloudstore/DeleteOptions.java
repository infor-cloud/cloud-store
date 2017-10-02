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
 * {@code DeleteOptions} contains all the details needed by the cloud-store delete
 * command. The specified {@code objectKey}, under {@code bucketName} bucket, is deleted
 * from the store.  If the key looks like a directory (ends in a '/'), all "top-level"
 * files in the directory will be deleted.  If the {@code recursive} property is set to
 * true, all matching sub-directory files will also be deleted.
 * <p>
 * {@code DeleteOptions} objects are meant to be built by {@code DeleteOptionsBuilder}. This class
 * provides only public accessor methods.
 * 
 * @see DeleteOptionsBuilder
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#delete(DeleteOptions)
 * @see CloudStoreClient#deleteDirectory(DeleteOptions)
 * @see OptionsBuilderFactory#newDeleteOptionsBuilder()
 */
public class DeleteOptions
  extends CommandOptions
{
  private String _bucket;
  private String _objectKey;
  private boolean _recursive;
  private boolean _dryRun;
  private boolean _forceDelete;
  private boolean _ignoreAbortInjection;

  // for testing injecion of aborts during a delete
  private static AbortCounters _abortCounters = new AbortCounters();


  DeleteOptions(
    CloudStoreClient cloudStoreClient, String bucket, String objectKey, boolean recursive,
    boolean dryRun, boolean forceDelete, boolean ignoreAbortInjection)
  {
    super(cloudStoreClient);
    _bucket = bucket;
    _objectKey = objectKey;
    _recursive = recursive;
    _dryRun = dryRun;
    _forceDelete = forceDelete;
    _ignoreAbortInjection = ignoreAbortInjection;
  }

  // for testing injection of aborts during a copy
  void injectAbort(String id)
  {
    if(!_ignoreAbortInjection && (_abortCounters.decrementInjectionCounter(id) > 0))
    {
      throw new AbortInjection("forcing delete abort");
    }
  }

  static AbortCounters getAbortCounters()
  {
    return _abortCounters;
  }

  /**
   * Return the name of the bucket containing the file to delete.
   *
   * @return name of bucket
   */
  public String getBucketName()
  {
    return _bucket;
  }

  /**
   * Return the key of the file to delete.
   *
   * @return file key
   */
  public String getObjectKey()
  {
    return _objectKey;
  }

  /**
   * Return the recursive property of the command.  If true and if the object key
   * looks like a directory name (ends in '/'), all files that recursively
   * have the key as their prefix will be deleted.
   *
   * @return recursive flag
   */
  public boolean isRecursive()
  {
    return _recursive;
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
   * If forceDelete is set to true, then delete command will complete successfully
   * even if the specified file does not exist.  Otherwise, the delete command
   * will fail when trying to delete a file that does not exist.
   *
   * @return force delete flag
   */
  public boolean forceDelete()
  {
    return _forceDelete;
  }
}
