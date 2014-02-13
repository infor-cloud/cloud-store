package com.logicblox.s3lib;


import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.google.common.util.concurrent.*;

import java.util.concurrent.Callable;

public class ListObjectsCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;

  public ListObjectsCommand(
          ListeningExecutorService httpExecutor,
          ListeningScheduledExecutorService internalExecutor)
  {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
  }

  public ListenableFuture<ObjectListing> run(final String bucket, final String prefix, final boolean recursive)
  {
    ListenableFuture<ObjectListing> future =
      executeWithRetry(
        _executor,
        new Callable<ListenableFuture<ObjectListing>>()
        {
          public ListenableFuture<ObjectListing> call()
          {
            return runActual(bucket, prefix, recursive);
          }
            
          public String toString()
          {
            return "list all object of s3://" + bucket + "/" + prefix;
          }
        });
    
    // The result will throw a 404 exception if after possible retries
    // the object is still not found. We transform this here into a
    // null value.
    return Futures.withFallback(
      future,
      new FutureFallback<ObjectListing>()
      {
        public ListenableFuture<ObjectListing> create(Throwable t)
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
  
  private ListenableFuture<ObjectListing> runActual(final String bucket, final String prefix, final boolean recursive)
  {
    return _httpExecutor.submit(
      new Callable<ObjectListing>()
      {
        public ObjectListing call()
        {
          // Note: we on purpose do not catch the 404 exception here
          // to make sure that the retry facility works when the
          // --stubborn option is used, which retries client
          // exceptions as well.
          ListObjectsRequest req = new ListObjectsRequest().withBucketName(bucket).withPrefix(prefix);
          if (! recursive) req.setDelimiter("/");

          return getAmazonS3Client().listObjects(req);
        }
      });
  }
}
