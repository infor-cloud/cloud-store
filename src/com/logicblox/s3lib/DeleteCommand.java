package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;


public class DeleteCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  private DeleteOptions _options;
  private CloudStoreClient _client;

  public DeleteCommand(
    ListeningExecutorService httpExecutor,
    ListeningScheduledExecutorService internalExecutor,
    CloudStoreClient client,
    DeleteOptions opts)
  {
    if(httpExecutor == null)
      throw new IllegalArgumentException("non-null http executor is required");
    if(internalExecutor == null)
      throw new IllegalArgumentException("non-null internal executor is required");

    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
    _client = client;
    _options = opts;
  }


  public ListenableFuture<S3File> run()
  {
    final String bucket = _options.getBucket();
    final String key = _options.getObjectKey();

    boolean exists = false;
    try
    {
      exists = (_client.exists(bucket, key).get() != null);

    }
    catch(InterruptedException | ExecutionException ex)
    {
      throw new UsageException(
        "Error checking object existance: " + ex.getMessage(), ex);
    }
    if(!exists)
    {
      if(_options.forceDelete())
        return Futures.immediateFuture(new S3File());
      else
        throw new UsageException("Object not found at " + getUri(bucket, key));
    }

    ListenableFuture<S3File> future =
      executeWithRetry(
        _executor,
        new Callable<ListenableFuture<S3File>>()
        {
          public ListenableFuture<S3File> call()
          {
            return runActual();
          }

          public String toString()
          {
            return "delete " + getUri(bucket, key);
          }
        });

    return future;
  }

  private ListenableFuture<S3File> runActual()
  {
    if(_options.isDryRun())
    {
      System.out.println("<DRYRUN> deleting '"
        + getUri(_options.getBucket(), _options.getObjectKey()));
      return Futures.immediateFuture(new S3File());
    }
    else
    {
      return _httpExecutor.submit(
        new Callable<S3File>()
        {
          public S3File call()
          {
	    String bucket = _options.getBucket();
	    String key = _options.getObjectKey();
            DeleteObjectRequest req = new DeleteObjectRequest(bucket, key);
            getAmazonS3Client().deleteObject(req);
            S3File file = new S3File();
            file.setBucketName(bucket);
            file.setKey(key);
            return file;
          }
        });
    }
  }
}
