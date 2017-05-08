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


public class GCSGetCommand extends Command
{

  private ListeningExecutorService _s3Executor;
  private ListeningScheduledExecutorService _executor;
  private Storage _storage;
  private String _bucket;
  private String _key;

  public GCSGetCommand(
    Storage storage, ListeningExecutorService s3Executor,
    ListeningScheduledExecutorService internalExecutor,
    String bucket, String key)
  {
    _storage = storage;
    _s3Executor = s3Executor;
    _executor = internalExecutor;
    _bucket = bucket;
    _key = key;
  }


  public ListenableFuture<ObjectMetadata> run()
  {
    ListenableFuture<ObjectMetadata> future =
      executeWithRetry(_executor, new Callable<ListenableFuture<ObjectMetadata>>()
      {
        public ListenableFuture<ObjectMetadata> call()
        {
          return runActual();
        }
          
      });
    
    return future;
  }
  

  private ListenableFuture<ObjectMetadata> runActual()
  {
    return _s3Executor.submit(new Callable<ObjectMetadata>()
    {
      public ObjectMetadata call() throws IOException
      {
        Storage.Objects.Get cmd = _storage.objects().get(_bucket, _key);
        try
	{
          StorageObject resp = cmd.execute();
          if(resp == null)
          {
            return null;
          }
          else
	  {
            return new ObjectMetadata();
	  }
	}
	catch(Exception ex)
	{
	  // FIXME - tighten this up if it works
	  if(ex.getMessage().contains("404 Not Found"))
	  {
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
