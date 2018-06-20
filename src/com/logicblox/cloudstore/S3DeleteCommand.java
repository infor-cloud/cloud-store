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

import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Callable;


class S3DeleteCommand
  extends Command
{
  private DeleteOptions _options;

  public S3DeleteCommand(DeleteOptions options)
  {
    super(options);
    _options = options;
  }


  public ListenableFuture<StoreFile> run()
  {
    if(_options.getObjectKey().endsWith("/") || _options.getObjectKey().equals(""))
    {
      String uri = getUri(_options.getBucketName(), _options.getObjectKey());
      throw new UsageException("Key should be fully qualified: " + uri + ". Prefix keys are" +
        " supported only by the recursive variant.");
    }

    final String bucket = _options.getBucketName();
    final String key = _options.getObjectKey();

    ExistsOptions opts = _client.getOptionsBuilderFactory()
      .newExistsOptionsBuilder()
      .setBucketName(bucket)
      .setObjectKey(key)
      .createOptions();

    ListenableFuture<Metadata> existsFuture = _client.exists(opts);

    ListenableFuture<StoreFile> result = Futures.transform(existsFuture,
      new AsyncFunction<Metadata, StoreFile>()
      {
        public ListenableFuture<StoreFile> apply(Metadata mdata)
          throws UsageException
        {
          if(mdata == null)
          {
            throw new UsageException("Object not found at " + getUri(bucket, key));
          }
          return getDeleteFuture();
        }
      });

    return result;
  }


  private ListenableFuture<StoreFile> getDeleteFuture()
  {
    final String bucket = _options.getBucketName();
    final String key = _options.getObjectKey();

    ListenableFuture<StoreFile> deleteFuture = executeWithRetry(_client.getInternalExecutor(),
      new Callable<ListenableFuture<StoreFile>>()
      {
        public ListenableFuture<StoreFile> call()
        {
          return runActual();
        }

        public String toString()
        {
          return "delete " + getUri(bucket, key);
        }
      });
    return deleteFuture;
  }


  private ListenableFuture<StoreFile> runActual()
  {
    final String srcUri = getUri(_options.getBucketName(), _options.getObjectKey());
    if(_options.isDryRun())
    {
      System.out.println("<DRYRUN> deleting '" + srcUri + "'");
      return Futures.immediateFuture(null);
    }
    else
    {
      return _client.getApiExecutor().submit(new Callable<StoreFile>()
      {
        public StoreFile call()
        {
          // support for testing failures
          _options.injectAbort(srcUri);

          String bucket = _options.getBucketName();
          String key = _options.getObjectKey();
          DeleteObjectRequest req = new DeleteObjectRequest(bucket, key);
          getS3Client().deleteObject(req);
          StoreFile file = new StoreFile();
          file.setBucketName(bucket);
          file.setObjectKey(key);
          return file;
        }
      });
    }
  }
}
