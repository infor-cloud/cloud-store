package com.logicblox.s3lib;

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import java.io.IOException;
import java.util.concurrent.Callable;


public class GCSCopyCommand extends Command
{

  private ListeningExecutorService _s3Executor;
  private ListeningScheduledExecutorService _executor;
  private Storage _storage;

  public GCSCopyCommand(
      Storage storage,
      ListeningExecutorService s3Executor,
      ListeningScheduledExecutorService internalExecutor)
  {
    _storage = storage;
    _s3Executor = s3Executor;
    _executor = internalExecutor;
  }


  public ListenableFuture<S3File> run(final CopyOptions options)
  {
    ListenableFuture<S3File> future =
        executeWithRetry(_executor, new Callable<ListenableFuture<S3File>>()
        {
          public ListenableFuture<S3File> call()
          {
            return runActual(options);
          }
          
          public String toString()
          {
            return "copying object from "
                + getUri(options.getSourceBucketName(), options.getSourceKey()) + " to "
                + getUri(options.getDestinationBucketName(), options.getDestinationKey());
          }
        });
    
    return future;
  }
  

  private ListenableFuture<S3File> runActual(final CopyOptions options)
  {
    return _s3Executor.submit(new Callable<S3File>()
    {
      public S3File call() throws IOException
      {
        Storage.Objects.Copy cmd = _storage.objects().copy(
          options.getSourceBucketName(), options.getSourceKey(),
          options.getDestinationBucketName(), options.getDestinationKey(),
          null);
        StorageObject resp = cmd.execute();
        return createS3File(resp, false);
      }
    });
  }

  private S3File createS3File(StorageObject obj, boolean includeVersion)
  {
    S3File f = new S3File();
    f.setKey(obj.getName());
    f.setETag(obj.getEtag());
    f.setBucketName(obj.getBucket());
    f.setSize(obj.getSize().longValue());
    if(includeVersion && (null != obj.getGeneration()))
      f.setVersionId(obj.getGeneration().toString());
    f.setTimestamp(new java.util.Date(obj.getUpdated().getValue()));
    return f;
  }

}
