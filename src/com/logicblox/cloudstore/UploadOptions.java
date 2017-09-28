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
 * {@code UploadOptions} contains all the details needed by the upload operation. The specified
 * {@code _file} will be uploaded under the specified {@code _bucketName} and {@code _objectKey}.
 * <p>
 * If the {@code _chunkSize} is {@code null}, then {@code getChunkSize} will try to compute a chunk
 * size so that the number of the uploaded parts be less than 10000 (current S3 limit). If the
 * {@code _chunkSize} is explicit (i.e. not {@code null}, then no check will take place and any
 * possible failure due to more than 10000 parts will happen later.
 * <p>
 * The specified {@code _cannedAcl} is applied to the uploaded file.
 * <p>
 * If the {@code enckey} is present, the {@code keyProvider} will be asked to provide a public key
 * with that name. This key will be used to encrypt the {@code _file} at the client side.
 * <p>
 * If progress listener factory has been set, then progress notifications will be recorded.
 * <p>
 * {@code UploadOptions} objects are meant to be built by {@code UploadOptionsBuilder}. This class
 * provides only public getter methods.
 */
public class UploadOptions
  extends CommandOptions
{
  private File _file;
  private String _bucketName;
  private String _objectKey;
  private long _chunkSize = -1;
  private String _encKey;
  private String _cannedAcl;
  private boolean _dryRun;
  private boolean _ignoreAbortInjection;
  private OverallProgressListenerFactory _overallProgressListenerFactory;

  // for testing
  private static AbortCounters _abortCounters = new AbortCounters();


  UploadOptions(
    CloudStoreClient cloudStoreClient, File file, String bucketName, String objectKey, long chunkSize,
    String encKey, String cannedAcl, boolean dryRun, boolean ignoreAbortInjection,
    OverallProgressListenerFactory overallProgressListenerFactory)
  {
    super(cloudStoreClient);
    _file = file;
    _bucketName = bucketName;
    _objectKey = objectKey;
    _chunkSize = chunkSize;
    _encKey = encKey;
    _cannedAcl = cannedAcl;
    _dryRun = dryRun;
    _ignoreAbortInjection = ignoreAbortInjection;
    _overallProgressListenerFactory = overallProgressListenerFactory;
  }


  // for testing injection of aborts during a copy
  void injectAbort(String id)
  {
    if(!_ignoreAbortInjection && (_abortCounters.decrementInjectionCounter(id) > 0))
    {
      throw new AbortInjection("forcing upload abort");
    }
  }

  static AbortCounters getAbortCounters()
  {
    return _abortCounters;
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

  public long getChunkSize()
  {
    if(_file.isDirectory())
    {
      return -1;
    }
    if(_chunkSize == -1)
    {
      return Utils.getDefaultChunkSize(_file.length());
    }
    return _chunkSize;
  }

  public String getCannedAcl()
  {
    return _cannedAcl;
  }

  public boolean isDryRun()
  {
    return _dryRun;
  }

  public Optional<String> getEncKey()
  {
    return Optional.ofNullable(_encKey);
  }

  public Optional<OverallProgressListenerFactory> getOverallProgressListenerFactory()
  {
    return Optional.ofNullable(_overallProgressListenerFactory);
  }
}
