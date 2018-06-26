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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;


class GCSCopyCommand
  extends Command
{
  private CopyOptions _options;

  public GCSCopyCommand(CopyOptions options)
  {
    super(options);
    _options = options;
  }

  public ListenableFuture<StoreFile> run()
  {
    if(_options.getSourceObjectKey().endsWith("/") || _options.getSourceObjectKey().equals(""))
    {
      String uri = getUri(_options.getSourceBucketName(), _options.getSourceObjectKey());
      throw new UsageException("Source key should be fully qualified: " + uri + ". Source " +
        "prefix keys are supported only by the recursive variant.");
    }

    if(_options.isDryRun())
    {
      System.out.println("<DRYRUN> copying '" +
        getUri(_options.getSourceBucketName(), _options.getSourceObjectKey()) + "' to '" +
        getUri(_options.getDestinationBucketName(), _options.getDestinationObjectKey()) + "'");
      return Futures.immediateFuture(null);
    }
    else
    {
      ExistsOptions opts = _client.getOptionsBuilderFactory()
        .newExistsOptionsBuilder()
        .setBucketName(_options.getSourceBucketName())
        .setObjectKey(_options.getSourceObjectKey())
        .createOptions();
      ListenableFuture<Metadata> sourceExists = _client.exists(opts);
      ListenableFuture<StoreFile> future = Futures.transform(sourceExists, startCopyAsyncFunction());

      return future;
    }
  }

  private AsyncFunction<Metadata, StoreFile> startCopyAsyncFunction()
  {
    return new AsyncFunction<Metadata, StoreFile>()
    {
      public ListenableFuture<StoreFile> apply(Metadata mdata)
      {
        if(mdata == null)
        {
          throw new UsageException("Source object not found at " + getUri(
            _options.getSourceBucketName(), _options.getSourceObjectKey()));
        }
        return startCopy();
      }
    };
  }

  private ListenableFuture<StoreFile> startCopy()
  {
    return executeWithRetry(_client.getInternalExecutor(),
      new Callable<ListenableFuture<StoreFile>>()
      {
        public ListenableFuture<StoreFile> call()
        {
          return runActual();
        }

        public String toString()
        {
          return "starting copy of " +
            getUri(_options.getSourceBucketName(), _options.getSourceObjectKey()) + " to " +
            getUri(_options.getDestinationBucketName(), _options.getDestinationObjectKey());
        }
      });
  }

  private ListenableFuture<StoreFile> runActual()
  {
    return _client.getApiExecutor().submit(new Callable<StoreFile>()
    {
      public StoreFile call()
        throws IOException
      {
        // support for testing failures
        String srcUri = getUri(_options.getSourceBucketName(), _options.getSourceObjectKey());
        _options.injectAbort(srcUri);

        StorageObject objectMetadata = null;
        Map<String, String> userMetadata = _options.getUserMetadata().orElse(null);
        if(userMetadata != null)
        {
          Storage.Objects.Get get = getGCSClient().objects()
            .get(_options.getSourceBucketName(), _options.getSourceObjectKey());
          StorageObject sourceObject = get.execute();
          // Map<String,String> sourceUserMetadata = sourceObject.getMetadata();

          objectMetadata = new StorageObject().setMetadata(ImmutableMap.copyOf(userMetadata))
            .setContentType(sourceObject.getContentType());
          // .setContentDisposition(sourceObject.getContentDisposition())
          // other metadata to be set?

          if(!_options.getCannedAcl().isPresent())
          {
            objectMetadata.setAcl(sourceObject.getAcl());
          }
        }

        Storage.Objects.Copy cmd = getGCSClient().objects()
          .copy(_options.getSourceBucketName(), _options.getSourceObjectKey(),
            _options.getDestinationBucketName(), _options.getDestinationObjectKey(),
            objectMetadata);

        _options.getCannedAcl().ifPresent(cmd::setDestinationPredefinedAcl);

        StorageObject resp = cmd.execute();
        return createStoreFile(resp, false);
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
