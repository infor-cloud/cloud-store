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

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;


class DeleteDirCommand
  extends Command
{
  private DeleteOptions _options;

  public DeleteDirCommand(DeleteOptions options)
  {
    super(options);
    _options = options;
  }

  public ListenableFuture<List<StoreFile>> run()
    throws InterruptedException, ExecutionException
  {
    ListenableFuture<List<StoreFile>> listObjs = queryFiles();
    ListenableFuture<List<StoreFile>> result = Futures.transform(listObjs,
      new AsyncFunction<List<StoreFile>, List<StoreFile>>()
      {
        public ListenableFuture<List<StoreFile>> apply(List<StoreFile> potential)
        {
          List<StoreFile> matches = new ArrayList<StoreFile>();
          for(StoreFile f : potential)
          {
            if(!f.getObjectKey().endsWith("/"))
            {
              matches.add(f);
            }
          }
          if(!_options.forceDelete() && matches.isEmpty())
          {
            throw new UsageException("No objects found that match '" +
              getUri(_options.getBucketName(), _options.getObjectKey()) + "'");
          }

          List<ListenableFuture<StoreFile>> futures = prepareFutures(matches);

          if(_options.isDryRun())
          {
            return Futures.immediateFuture(null);
          }
          else
          {
            return Futures.allAsList(futures);
          }
        }
      });
    return result;
  }


  private List<ListenableFuture<StoreFile>> prepareFutures(List<StoreFile> toDelete)
  {
    List<ListenableFuture<StoreFile>> futures = new ArrayList<ListenableFuture<StoreFile>>();
    for(StoreFile src : toDelete)
    {
      if(_options.isDryRun())
      {
        System.out.println("<DRYRUN> deleting '" + getUri(src.getBucketName(), src.getObjectKey()) + "'");
      }
      else
      {
        DeleteOptions opts = _client.getOptionsBuilderFactory()
          .newDeleteOptionsBuilder()
          .setBucketName(src.getBucketName())
          .setObjectKey(src.getObjectKey())
          .createOptions();
        futures.add(_client.delete(opts));
      }
    }
    return futures;
  }


  private ListenableFuture<List<StoreFile>> queryFiles()
  {
    // find all files that need to be deleted
    ListOptions opts = _client.getOptionsBuilderFactory()
      .newListOptionsBuilder()
      .setBucketName(_options.getBucketName())
      .setObjectKey(_options.getObjectKey())
      .setRecursive(_options.isRecursive())
      .createOptions();
    return _client.listObjects(opts);
  }
}
