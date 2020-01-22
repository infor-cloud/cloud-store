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

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.io.IOException;
import java.util.concurrent.Callable;

class GCSDownloadFactory
{
  final private DownloadOptions _options;
  final private ListeningExecutorService _apiExecutor;
  private final ListeningExecutorService _internalExecutor;
  final private Storage _client;

  public GCSDownloadFactory(DownloadOptions options,
                            Storage client,
                            ListeningExecutorService apiExecutor,
                            ListeningExecutorService internalExecutor)
  {
    _options = options;
    _client = client;
    _apiExecutor = apiExecutor;
    _internalExecutor = internalExecutor;
  }

  ListenableFuture<Download> startDownload()
  {
    return _apiExecutor.submit(new StartCallable());
  }

  private class StartCallable
    implements Callable<Download>
  {
    public Download call()
      throws IOException
    {
      StorageObject storageObject = _client.objects().get(_options.getBucketName(),
        _options.getObjectKey()).execute();
      return new GCSDownload(_options, _client, _apiExecutor, _internalExecutor, storageObject);
    }
  }
}
