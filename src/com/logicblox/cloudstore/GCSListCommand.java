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
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;


class GCSListCommand
  extends Command
{

  private ListOptions _options;

  public GCSListCommand(ListOptions options)
  {
    super(options);
    _options = options;
  }


  public ListenableFuture<List<StoreFile>> run()
  {
    ListenableFuture<List<StoreFile>> future = executeWithRetry(_client.getInternalExecutor(),
      new Callable<ListenableFuture<List<StoreFile>>>()
      {
        public ListenableFuture<List<StoreFile>> call()
        {
          return runActual();
        }

        public String toString()
        {
          return "listing objects and directories for " +
            getUri(_options.getBucketName(), _options.getObjectKey().orElse(""));
        }
      });

    return future;
  }


  private ListenableFuture<List<StoreFile>> runActual()
  {
    return _client.getApiExecutor().submit(new Callable<List<StoreFile>>()
    {
      public List<StoreFile> call()
        throws IOException
      {
        List<StoreFile> s3files = new ArrayList<StoreFile>();
        List<StorageObject> allObjs = new ArrayList<StorageObject>();
        Storage.Objects.List cmd = getGCSClient().objects().list(_options.getBucketName());
        cmd.setPrefix(_options.getObjectKey().orElse(null));
        if(!_options.isRecursive())
        {
          cmd.setDelimiter("/");
        }
        boolean ver = _options.versionsIncluded();
        cmd.setVersions(ver);
        Objects objs;
        do
        {
          objs = cmd.execute();
          List<StorageObject> items = objs.getItems();
          if(items != null)
          {
            allObjs.addAll(items);
          }
          cmd.setPageToken(objs.getNextPageToken());
        }
        while(objs.getNextPageToken() != null);

        for(StorageObject s : allObjs)
          s3files.add(createStoreFile(s, ver));
        return s3files;
      }
    });
  }

  private StoreFile createStoreFile(StorageObject obj, boolean includeVersion)
  {
    StoreFile f = new StoreFile();
    f.setObjectKey(obj.getName());
    f.setETag(obj.getEtag());
    f.setBucketName(obj.getBucket());
    f.setSize(obj.getSize().longValue());
    if(includeVersion && (null != obj.getGeneration()))
    {
      f.setVersionId(obj.getGeneration().toString());
    }
    f.setTimestamp(new java.util.Date(obj.getUpdated().getValue()));
    return f;
  }

}
