/*
  Copyright 2021, Infor Inc.

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

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


class GCSExistsCommand
  extends Command
{
  private ExistsOptions _options;

  public GCSExistsCommand(ExistsOptions options)
  {
    super(options);
    _options = options;
  }

  public ListenableFuture<Metadata> run()
  {
    ListenableFuture<Metadata> future = executeWithRetry(_client.getInternalExecutor(),
      new Callable<ListenableFuture<Metadata>>()
      {
        public ListenableFuture<Metadata> call()
        {
          return runActual();
        }

        public String toString()
        {
          return "check for existence of " +
            getUri(_options.getBucketName(), _options.getObjectKey());
        }
      });

    // The result will throw a 404 exception if after possible retries
    // the object is still not found. We transform this here into a
    // null value.
//    return Futures.withFallback(future, new FutureFallback<Metadata>()
    return Futures.catchingAsync(
      future, 
      Throwable.class, 
      new AsyncFunction<Throwable, Metadata>()
      {
//      public ListenableFuture<Metadata> create(Throwable t)
        public ListenableFuture<Metadata> apply(Throwable t)
        {
          if(t instanceof GoogleJsonResponseException)
          {
            GoogleJsonResponseException exc = (GoogleJsonResponseException) t;
            if(exc.getStatusCode() == 404)
            {
              return Futures.immediateFuture(null);
            }
          }
          return Futures.immediateFailedFuture(t);
        }
      },
      MoreExecutors.directExecutor());
  }


  protected ThrowableRetryPolicy getRetryPolicy(int initialDelay, int maxDelay, int retryCount, TimeUnit timeUnits)
  {
    // retry if exception is not 404 (file doesn't exist) or if stubborn is set.
    // the code we inherit from Command is specialized for AWS client exceptions so we
    // need to override it here.  if we don't, we will always retry over and over when we can't find a
    // file.  we may want to add a GcsCommand class that all GCS command
    // overrides can use where we can share stuff like this?
    ThrowableRetryPolicy trp = new ExpBackoffRetryPolicy(initialDelay, maxDelay, retryCount, timeUnits)
    {
      @Override
      public boolean retryOnThrowable(Throwable thrown)
      {
        if(_stubborn)
          return true;
        if(thrown instanceof GoogleJsonResponseException)
        {
          GoogleJsonResponseException exc = (GoogleJsonResponseException) thrown;
          if(exc.getStatusCode() == 404)
            return false;
        }
        return true;
      }
    };
    return trp;
  }

  private ListenableFuture<Metadata> runActual()
  {
    return _client.getApiExecutor().submit(new Callable<Metadata>()
    {
      public Metadata call()
        throws IOException
      {
        // Note: we on purpose do not catch the 404 exception here
        // to make sure that the retry facility works when the
        // --stubborn option is used, which retries client
        // exceptions as well.
//        return new Metadata(
//          getS3Client().getObjectMetadata(_options.getBucketName(), _options.getObjectKey()));

//System.err.println("!!!!!!!!!!!!!!!!!!!!!!!! EXISTS OVERRIDE - " + _options.getObjectKey());

        Storage.Objects.Get getCmd = getGCSClient().objects().get(
          _options.getBucketName(), _options.getObjectKey());
        StorageObject obj = getCmd.execute();
//System.err.println(obj.getClass());
//System.err.println("" + obj);
//System.err.println("-----------------");

        ObjectMetadata awsMd = new ObjectMetadata();
// FIXME:
  // setBucketKeyEnabled()??
  // setHeader()??
  // setHttpExpiresDate()??
  // setSSEAlgorithm()??
  // acl??

// FIXME - look to see what the exists command prints for s3 and compare to what we can get from gcs
           // ---> run a version of cloud-store with guava 15 and see what exists returns for a gcs object

        awsMd.setCacheControl(obj.getCacheControl());
        awsMd.setContentDisposition(obj.getContentDisposition());
        awsMd.setContentEncoding(obj.getContentEncoding());
        awsMd.setContentLanguage(obj.getContentLanguage());
        awsMd.setContentLength(obj.getSize().longValue());

// fixme - this doesn't match what the s3 compatibility layer returns
//        awsMd.setContentMD5(obj.getMd5Hash());

        awsMd.setContentType(obj.getContentType());
        if(null != obj.getRetentionExpirationTime())
          awsMd.setExpirationTime(new Date(obj.getRetentionExpirationTime().getValue()));
        if(null != obj.getUpdated())
          awsMd.setLastModified(new Date(obj.getUpdated().getValue())); // ??????
        if(null != obj.getMetadata())
           awsMd.setUserMetadata(obj.getMetadata());

        Metadata md = new Metadata(awsMd);
// FIXME - etag doesn't seem to be the same value as when i use the old interface
        md.setETag(obj.getEtag());
        return md;
      }
    });
  }

}
