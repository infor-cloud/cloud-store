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

import com.google.api.services.storage.Storage;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;

class GCSUploadFactory
{
  private Storage _client;
  private ListeningExecutorService _executor;

  public GCSUploadFactory(Storage client, ListeningExecutorService executor)
  {
    if(client == null)
    {
      throw new IllegalArgumentException("non-null client is required");
    }
    if(executor == null)
    {
      throw new IllegalArgumentException("non-null executor is required");
    }

    _client = client;
    _executor = executor;
  }

  public ListenableFuture<Upload> startUpload(
    String bucketName, String key, Map<String, String> meta, UploadOptions options)
  {
    return _executor.submit(new StartCallable(bucketName, key, meta, options));
  }

  private class StartCallable
    implements Callable<Upload>
  {
    private String _objectKey;
    private String _bucketName;
    private Map<String, String> _meta;
    private UploadOptions _options;

    public StartCallable(
      String bucketName, String objectKey, Map<String, String> meta, UploadOptions options)
    {
      _bucketName = bucketName;
      _objectKey = objectKey;
      _meta = meta;
      _options = options;
    }

    public Upload call()
      throws Exception
    {
      return new GCSUpload(_client, _bucketName, _objectKey, _meta, new Date(), _executor,
        _options);
    }
  }
}
