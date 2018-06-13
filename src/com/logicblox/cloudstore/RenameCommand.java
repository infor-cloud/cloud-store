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

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;


class RenameCommand
  extends Command
{
  private RenameOptions _options;

  public RenameCommand(RenameOptions options)
  {
    super(options);
    _options = options;
  }


  public ListenableFuture<StoreFile> run()
  {
    if(_options.getSourceObjectKey().endsWith("/") || _options.getSourceObjectKey().equals(""))
    {
      String uri = getUri(_options.getSourceBucketName(), _options.getSourceObjectKey());
      throw new UsageException("Source key should be a fully qualified URI: " + uri + ". Prefix " +
        "source URIs are supported only by the recursive variant.");
    }

    if(_options.isDryRun())
    {
      System.out.println("<DRYRUN> renaming '" + getSourceUri() + "' to '" + getDestUri() + "'");
      return Futures.immediateFuture(null);
    }

    return Futures.withFallback(buildFutureChain(), cleanupOnError());
  }


  // try to clean up if a failure occurs.  just have to worry
  // about failure during a delete phase and remove the copied
  // file
  private FutureFallback<StoreFile> cleanupOnError()
  {
    return new FutureFallback<StoreFile>()
    {
      public ListenableFuture<StoreFile> create(Throwable t)
      {
        DeleteOptions deleteOpts = _client.getOptionsBuilderFactory()
          .newDeleteOptionsBuilder()
          .setBucketName(_options.getDestinationBucketName())
          .setObjectKey(_options.getDestinationObjectKey())
          .setIgnoreAbortInjection(true)
          .createOptions();
        ListenableFuture<StoreFile> deleteFuture = Futures.withFallback(_client.delete(deleteOpts),
          ignoreMissingDestinationFile());

        return Futures.transform(deleteFuture,
          new AsyncFunction<StoreFile, StoreFile>()
          {
            public ListenableFuture<StoreFile> apply(StoreFile f)
            {
              return Futures.immediateFailedFuture(t);
            }
          });
      }
    };
  }

  private FutureFallback<StoreFile> ignoreMissingDestinationFile()
  {
    return new FutureFallback<StoreFile>()
    {
      public ListenableFuture<StoreFile> create(Throwable t)
      {
        // It's ok if destintation is not there. Copy might have failed.
        if(t instanceof UsageException && t.getMessage().contains("Object not found"))
          return Futures.immediateFuture(null);
        return Futures.immediateFailedFuture(t);
      }
    };
  }

  // start by checking that source exists, then follow with dest check
  private ListenableFuture<StoreFile> buildFutureChain()
  {
    ExistsOptions opts = _client.getOptionsBuilderFactory()
      .newExistsOptionsBuilder()
      .setBucketName(_options.getSourceBucketName())
      .setObjectKey(_options.getSourceObjectKey())
      .createOptions();

    ListenableFuture<Metadata> sourceExists = _client.exists(opts);

    return Futures.transform(sourceExists, new AsyncFunction<Metadata, StoreFile>()
    {
      public ListenableFuture<StoreFile> apply(Metadata mdata)
        throws UsageException
      {
        if(null == mdata)
        {
          throw new UsageException("Source object '" + getSourceUri() + "' does not exist");
        }
        return checkDestExists();
      }
    });
  }


  // follow dest check with copy op
  private ListenableFuture<StoreFile> checkDestExists()
  {
    ExistsOptions opts = _client.getOptionsBuilderFactory()
      .newExistsOptionsBuilder()
      .setBucketName(_options.getDestinationBucketName())
      .setObjectKey(getDestKey())
      .createOptions();

    ListenableFuture<Metadata> destExists = _client.exists(opts);

    return Futures.transform(destExists, new AsyncFunction<Metadata, StoreFile>()
    {
      public ListenableFuture<StoreFile> apply(Metadata mdata)
        throws UsageException
      {
        if(null != mdata)
        {
          throw new UsageException("Cannot overwrite existing destination object '" + getDestUri());
        }
        return copyObject();
      }
    });
  }


  // copy is followed by delete
  private ListenableFuture<StoreFile> copyObject()
  {
    return Futures.transform(getCopyOp(), new AsyncFunction<StoreFile, StoreFile>()
    {
      public ListenableFuture<StoreFile> apply(StoreFile srcFile)
      {
        return deleteObject();
      }
    });

  }


  // delete is followed by return of the dest file
  private ListenableFuture<StoreFile> deleteObject()
  {
    return Futures.transform(getDeleteOp(), new Function<StoreFile, StoreFile>()
    {
      public StoreFile apply(StoreFile deletedFile)
      {
        return new StoreFile(_options.getDestinationBucketName(), getDestKey());
      }
    });
  }


  private ListenableFuture<StoreFile> getDeleteOp()
  {
    DeleteOptions deleteOpts = _client.getOptionsBuilderFactory()
      .newDeleteOptionsBuilder()
      .setBucketName(_options.getSourceBucketName())
      .setObjectKey(_options.getSourceObjectKey())
      .createOptions();

    return _client.delete(deleteOpts);
  }


  private ListenableFuture<StoreFile> getCopyOp()
  {
    CopyOptions copyOpts = _client.getOptionsBuilderFactory()
      .newCopyOptionsBuilder()
      .setSourceBucketName(_options.getSourceBucketName())
      .setSourceObjectKey(_options.getSourceObjectKey())
      .setDestinationBucketName(_options.getDestinationBucketName())
      .setDestinationObjectKey(getDestKey())
      .setCannedAcl(_options.getCannedAcl().orElse(null))
      .createOptions();

    return _client.copy(copyOpts);
  }


  private String getDestKey()
  {
    String key = _options.getDestinationObjectKey();
    if(key.endsWith("/"))
    {
      // moving a file into a folder....
      String src = _options.getSourceObjectKey();
      int idx = src.lastIndexOf("/");
      if(-1 != idx)
      {
        key = key + src.substring(idx + 1);
      }
    }
    return key;
  }


  private String getSourceUri()
  {
    return getUri(_options.getSourceBucketName(), _options.getSourceObjectKey());
  }


  private String getDestUri()
  {
    return getUri(_options.getDestinationBucketName(), getDestKey());
  }

}
