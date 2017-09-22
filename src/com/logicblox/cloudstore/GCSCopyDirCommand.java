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
import com.google.api.services.storage.model.StorageObject;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;


public class GCSCopyDirCommand extends Command
{
  private CopyOptions _options;

  public GCSCopyDirCommand(CopyOptions options)
  {
    super(options);
    _options = options;
  }

  public ListenableFuture<List<StoreFile>> run()
  throws IOException
  {
    if (!_options.getDestinationObjectKey().endsWith("/") &&
      !_options.getDestinationObjectKey().equals(""))
    {
      throw new UsageException("Destination directory key should end with a '/'");
    }

    String baseDirPath = "";
    if (_options.getSourceObjectKey().length() > 0)
    {
      int endIndex = _options.getSourceObjectKey().lastIndexOf("/");
      if (endIndex != -1)
      {
        baseDirPath = _options.getSourceObjectKey().substring(0, endIndex + 1);
      }
    }
    final String baseDirPathF = baseDirPath;

    ListenableFuture<List<StoreFile>> listFuture =
      getListFuture(_options.getSourceBucketName(), _options.getSourceObjectKey(),
        _options.isRecursive());
    ListenableFuture<List<StoreFile>> result =
      Futures.transform(listFuture, new AsyncFunction<List<StoreFile>, List<StoreFile>>()
      {
        public ListenableFuture<List<StoreFile>> apply(List<StoreFile> filesToCopy)
        {
          List<ListenableFuture<StoreFile>> files = new ArrayList<>();
          List<ListenableFuture<StoreFile>> futures = new ArrayList<>();
          for (StoreFile src : filesToCopy)
            createCopyOp(futures, src, baseDirPathF);

          if (_options.isDryRun())
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


  private void createCopyOp(List<ListenableFuture<StoreFile>> futures,
                            final StoreFile src,
                            String baseDirPath)
  {
    if (!src.getKey().endsWith("/"))
    {
      String destKeyLastPart = src.getKey().substring(baseDirPath.length());
      final String destKey = _options.getDestinationObjectKey() + destKeyLastPart;
      if (_options.isDryRun())
      {
        System.out.println(
          "<DRYRUN> copying '" + getUri(_options.getSourceBucketName(), src.getKey()) + "' to '" +
            getUri(_options.getDestinationBucketName(), destKey) + "'");
      }
      else
      {
        futures.add(wrapCopyWithRetry(src, destKey));
      }
    }
  }

  private ListenableFuture<StoreFile> wrapCopyWithRetry(final StoreFile src, final String destKey)
  {
    return executeWithRetry(_client.getInternalExecutor(),
      new Callable<ListenableFuture<StoreFile>>()
      {
        public ListenableFuture<StoreFile> call()
        {
          return _client.getApiExecutor().submit(new Callable<StoreFile>()
          {
            public StoreFile call()
            throws IOException
            {
              return performCopy(src, destKey);
            }
          });
        }
      });
  }

  private StoreFile performCopy(StoreFile src, String destKey)
  throws IOException
  {
    // support for testing failures
    String srcUri = getUri(_options.getSourceBucketName(), src.getKey());
    _options.injectAbort(srcUri);

    Storage.Objects.Copy cmd = getGCSClient().objects()
      .copy(_options.getSourceBucketName(), src.getKey(), _options.getDestinationBucketName(),
        destKey, null);
    StorageObject resp = cmd.execute();
    return createStoreFile(resp, false);
  }

  private ListenableFuture<List<StoreFile>> getListFuture(String bucket,
                                                          String prefix,
                                                          boolean isRecursive)
  {
    // FIXME - if we gave commands a CloudStoreClient when they were created
    //         we could then use client.listObjects() instead of all this....
    ListOptions listOpts = _client.getOptionsBuilderFactory()
      .newListOptionsBuilder()
      .setBucketName(bucket)
      .setObjectKey(prefix)
      .setRecursive(isRecursive)
      .createOptions();

    return _client.listObjects(listOpts);
  }

  private StoreFile createStoreFile(StorageObject obj, boolean includeVersion)
  {
    StoreFile f = new StoreFile();
    f.setKey(obj.getName());
    f.setETag(obj.getEtag());
    f.setBucketName(obj.getBucket());
    f.setSize(obj.getSize().longValue());
    if (includeVersion && (null != obj.getGeneration()))
    {
      f.setVersionId(obj.getGeneration().toString());
    }
    f.setTimestamp(new java.util.Date(obj.getUpdated().getValue()));
    return f;
  }

}
