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


public class DeleteOptions extends CommandOptions
{
  private String _bucket;
  private String _objectKey;
  private boolean _recursive;
  private boolean _dryRun;
  private boolean _forceDelete;
  private boolean _ignoreAbortInjection;

  // for testing injecion of aborts during a delete
  private static AbortCounters _abortCounters = new AbortCounters();


  DeleteOptions(CloudStoreClient cloudStoreClient,
                String bucket,
                String objectKey,
                boolean recursive,
                boolean dryRun,
                boolean forceDelete,
                boolean ignoreAbortInjection)
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
    if (!_ignoreAbortInjection && (_abortCounters.decrementInjectionCounter(id) > 0))
    {
      throw new AbortInjection("forcing delete abort");
    }
  }

  static AbortCounters getAbortCounters()
  {
    return _abortCounters;
  }

  public String getBucketName()
  {
    return _bucket;
  }

  public String getObjectKey()
  {
    return _objectKey;
  }

  public boolean isRecursive()
  {
    return _recursive;
  }

  public boolean isDryRun()
  {
    return _dryRun;
  }

  public boolean forceDelete()
  {
    return _forceDelete;
  }
}
