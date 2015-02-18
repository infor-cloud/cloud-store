package com.logicblox.s3lib;


import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.ObjectInputStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.base.Functions;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

public class ExistsCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;

  public ExistsCommand(
    ListeningExecutorService httpExecutor,
    ListeningScheduledExecutorService internalExecutor)
  {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
  }

  public ListenableFuture<ObjectMetadata> run(final String bucket, final String key)
  {
    ListenableFuture<ObjectMetadata> future =
      executeWithRetry(
        _executor,
        new Callable<ListenableFuture<ObjectMetadata>>()
        {
          public ListenableFuture<ObjectMetadata> call()
          {
            return runActual(bucket, key);
          }
            
            public String toString()
          {
            return "check for existence of " + getScheme() + bucket + "/" + key;
          }
        });
    
    // The result will throw a 404 exception if after possible retries
    // the object is still not found. We transform this here into a
    // null value.
    return Futures.withFallback(
      future,
      new FutureFallback<ObjectMetadata>()
      {
        public ListenableFuture<ObjectMetadata> create(Throwable t)
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
  
  private ListenableFuture<ObjectMetadata> runActual(final String bucket, final String key)
  {
    return _httpExecutor.submit(
      new Callable<ObjectMetadata>()
      {
        public ObjectMetadata call()
        {
          // Note: we on purpose do not catch the 404 exception here
          // to make sure that the retry facility works when the
          // --stubborn option is used, which retries client
          // exceptions as well.
          return getAmazonS3Client().getObjectMetadata(bucket, key);
        }
      });
  }
}
