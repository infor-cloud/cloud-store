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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Stream;


class RenameRecursivelyCommand
  extends Command
{
  private RenameOptions _options;

  public RenameRecursivelyCommand(RenameOptions options)
  {
    super(options);
    _options = options;
  }

  public ListenableFuture<List<StoreFile>> run()
    throws InterruptedException, ExecutionException, IOException
  {
    if(!_options.getDestinationObjectKey().endsWith("/") &&
      !_options.getDestinationObjectKey().equals(""))
    {
      throw new UsageException("Destination key should end with a '/': " +
        getUri(_options.getDestinationBucketName(), _options.getDestinationObjectKey()));
    }


    return startCopyThenDelete();
  }

  private ListenableFuture<List<StoreFile>> startCopyThenDelete()
    throws InterruptedException, ExecutionException, IOException
  {
    final String bucket = _options.getDestinationBucketName();
    final String key = stripSlash(_options.getDestinationObjectKey());
    // exists command doesn't allow trailing slash

    ExistsOptions opts = _client.getOptionsBuilderFactory()
      .newExistsOptionsBuilder()
      .setBucketName(bucket)
      .setObjectKey(key)
      .createOptions();

    ListenableFuture<Metadata> destExists = _client.exists(opts);
    return Futures.transform(destExists, new AsyncFunction<Metadata, List<StoreFile>>()
    {
      public ListenableFuture<List<StoreFile>> apply(Metadata mdata)
        throws Exception
      {
        if(mdata != null)
        {
          throw new UsageException(
            "Cannot overwrite existing destination object '" + getUri(bucket, key));
        }
        return copyThenDelete();
      }
    });
  }


  private String stripSlash(String s)
  {
    if(s.endsWith("/"))
    {
      return s.substring(0, s.length() - 1);
    }
    else
    {
      return s;
    }
  }


  private ListenableFuture<List<StoreFile>> copyThenDelete()
  {
    // hack -- exceptions are a bit of a mess.  copyRecursively throws all sorts of stuff that
    //         should be collected into an ExecutionException?
    ListenableFuture<List<StoreFile>> listObjs = queryFiles();

    ListenableFuture<List<StoreFile>> result = Futures.transform(listObjs,
      new AsyncFunction<List<StoreFile>, List<StoreFile>>()
      {
        public ListenableFuture<List<StoreFile>> apply(List<StoreFile> toDelete)
          throws InterruptedException, ExecutionException, IOException
        {
          CopyOptions copyOpts = _client.getOptionsBuilderFactory()
            .newCopyOptionsBuilder()
            .setSourceBucketName(_options.getSourceBucketName())
            .setSourceObjectKey(_options.getSourceObjectKey())
            .setDestinationBucketName(_options.getDestinationBucketName())
            .setDestinationObjectKey(_options.getDestinationObjectKey())
            .setDryRun(_options.isDryRun())
            .setCannedAcl(_options.getCannedAcl().orElse(null))
            .createOptions();

          ListenableFuture<List<StoreFile>> copyFuture = _client.copyRecursively(copyOpts);

          String srcBaseDirURI = Utils.getBaseDirURI(getUri(_options.getSourceBucketName(),
            _options.getSourceObjectKey()));
          String destDirURI = getUri(_options.getDestinationBucketName(),
            _options.getDestinationObjectKey());
          if(srcBaseDirURI.equals(destDirURI))
          {
            // Protect against the case we move objects to themselves. We don't want to
            // delete them.
            return copyFuture;
          }
          else
          {
            return Futures.transform(copyFuture,
              new AsyncFunction<List<StoreFile>, List<StoreFile>>()
              {
                public ListenableFuture<List<StoreFile>> apply(final List<StoreFile> copied)
                  throws InterruptedException, ExecutionException
                {
                  // need to return list of dest files
                  return Futures.transform(deleteFiles(toDelete),
                    new Function<List<StoreFile>, List<StoreFile>>()
                    {
                      public List<StoreFile> apply(List<StoreFile> deletedFiles)
                      {
                        return copied;
                      }
                    });
                }
              });
          }
        }
      });
    return result;
  }

  private ListenableFuture<List<StoreFile>> queryFiles()
  {
    // find all files that need to be deleted
    ListOptions opts = _client.getOptionsBuilderFactory()
      .newListOptionsBuilder()
      .setBucketName(_options.getSourceBucketName())
      .setObjectKey(_options.getSourceObjectKey())
      .setRecursive(true)
      .createOptions();
    return _client.listObjects(opts);
  }

  private ListenableFuture<List<StoreFile>> deleteFiles(List<StoreFile> toDelete)
  {
    List<StoreFile> matches = new ArrayList<StoreFile>();
    for(StoreFile f : toDelete)
    {
      if(!f.getObjectKey().endsWith("/"))
        matches.add(f);
    }

    List<ListenableFuture<StoreFile>> futures = new ArrayList<ListenableFuture<StoreFile>>();

    for(StoreFile src : matches)
    {
      DeleteOptions opts = _client.getOptionsBuilderFactory()
        .newDeleteOptionsBuilder()
        .setBucketName(src.getBucketName())
        .setObjectKey(src.getObjectKey())
        .setDryRun(_options.isDryRun())
        .createOptions();
      futures.add(_client.delete(opts));
    }

    return Futures.allAsList(futures);
  }

}
