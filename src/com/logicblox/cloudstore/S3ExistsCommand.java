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


import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Callable;

public class S3ExistsCommand extends Command
{
  private ExistsOptions _options;

  public S3ExistsCommand(ExistsOptions options)
  {
    super(options);
    _options = options;
  }

  public ListenableFuture<Metadata> run()
  {
    ListenableFuture<Metadata> future =
      executeWithRetry(
        _client.getInternalExecutor(),
        new Callable<ListenableFuture<Metadata>>()
        {
          public ListenableFuture<Metadata> call()
          {
            return runActual();
          }
            
            public String toString()
          {
            return "check for existence of " + getUri(_options.getBucketName(),
              _options.getObjectKey());
          }
        });
    
    // The result will throw a 404 exception if after possible retries
    // the object is still not found. We transform this here into a
    // null value.
    return Futures.withFallback(
      future,
      new FutureFallback<Metadata>()
      {
        public ListenableFuture<Metadata> create(Throwable t)
        {
          if(t instanceof AmazonS3Exception)
          {
            AmazonS3Exception exc = (AmazonS3Exception) t;
            if (exc.getStatusCode() == 404)
              return Futures.immediateFuture(null);
          }
          
          
          return Futures.immediateFailedFuture(t);
        }
      });    
  }
  
  private ListenableFuture<Metadata> runActual()
  {
    return _client.getApiExecutor().submit(
      new Callable<Metadata>()
      {
        public Metadata call()
        {
          // Note: we on purpose do not catch the 404 exception here
          // to make sure that the retry facility works when the
          // --stubborn option is used, which retries client
          // exceptions as well.
          return new Metadata(getS3Client().getObjectMetadata(
	    _options.getBucketName(), _options.getObjectKey()));
        }
      });
  }
}
