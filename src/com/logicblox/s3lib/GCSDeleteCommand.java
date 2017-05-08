package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import java.io.IOException;
import java.util.concurrent.Callable;


public class GCSDeleteCommand extends Command
{

  private ListeningExecutorService _s3Executor;
  private ListeningScheduledExecutorService _executor;
  private Storage _storage;
  private DeleteOptions _options;

  public GCSDeleteCommand(
    Storage storage, ListeningExecutorService s3Executor,
    ListeningScheduledExecutorService internalExecutor,
    DeleteOptions options)
  {
    _storage = storage;
    _s3Executor = s3Executor;
    _executor = internalExecutor;
    _options = options;
  }


  public ListenableFuture<S3File> run()
  {
    ListenableFuture<S3File> future =
      executeWithRetry(_executor, new Callable<ListenableFuture<S3File>>()
      {
        public ListenableFuture<S3File> call()
        {
          return runActual();
        }
          
      });
    
    return future;
  }
  

  private ListenableFuture<S3File> runActual()
  {
    return _s3Executor.submit(new Callable<S3File>()
    {
      public S3File call() throws IOException
      {
        String bucket = _options.getBucket();
        String key = _options.getObjectKey();
        Storage.Objects.Delete cmd = _storage.objects().delete(bucket, key);
        try
	{
          cmd.execute();
// FIXME - return a populated file object
          return new S3File();
	}
	catch(Exception ex)
	{
// FIXME - tighten this up if it works
	  if(ex.getMessage().contains("404 Not Found"))
	  {
System.out.println("!!!!!!!!!!! DEL MISSING " + key);
            return null;
	  }
	  else
	  {
	    throw ex;
	  }
        }
      }
    });
  }


}
