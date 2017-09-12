package com.logicblox.s3lib;

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;


public class GCSCopyCommand extends Command
{

  private ListeningExecutorService _s3Executor;
  private ListeningScheduledExecutorService _executor;

  public GCSCopyCommand(
      ListeningExecutorService s3Executor,
      ListeningScheduledExecutorService internalExecutor)
  {
    _s3Executor = s3Executor;
    _executor = internalExecutor;
  }


  public ListenableFuture<StoreFile> run(final CopyOptions options)
  {
    if(options.isDryRun())
    {
      System.out.println("<DRYRUN> copying '"
        + getUri(options.getSourceBucketName(), options.getSourceKey())
        + "' to '"
        + getUri(options.getDestinationBucketName(), options.getDestinationKey()) + "'");
      return Futures.immediateFuture(null);
    }
    else
    {
      ListenableFuture<StoreFile> future =
        executeWithRetry(_executor, new Callable<ListenableFuture<StoreFile>>()
        {
          public ListenableFuture<StoreFile> call()
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
  }
  

  private ListenableFuture<StoreFile> runActual(final CopyOptions options)
  {
    return _s3Executor.submit(new Callable<StoreFile>()
    {
      public StoreFile call() throws IOException
      {
        // support for testing failures
        String srcUri = getUri(options.getSourceBucketName(), options.getSourceKey());
        options.injectAbort(srcUri);

        StorageObject objectMetadata = null;
        Map<String,String> userMetadata = options.getUserMetadata().orNull();
        if (userMetadata != null)
        {
          Storage.Objects.Get get = getGCSClient().objects().get(
            options.getSourceBucketName(), options.getSourceKey());
          StorageObject sourceObject = get.execute();
          // Map<String,String> sourceUserMetadata = sourceObject.getMetadata();

          objectMetadata = new StorageObject()
            .setMetadata(ImmutableMap.copyOf(userMetadata))
            .setContentType(sourceObject.getContentType())
            .setAcl(sourceObject.getAcl());
            // .setContentDisposition(sourceObject.getContentDisposition())
            // other metadata to be set?
        }

        Storage.Objects.Copy cmd = getGCSClient().objects().copy(
          options.getSourceBucketName(), options.getSourceKey(),
          options.getDestinationBucketName(), options.getDestinationKey(),
          objectMetadata);
        StorageObject resp = cmd.execute();
        return createStoreFile(resp, false);
      }
    });
  }

  private StoreFile createStoreFile(StorageObject obj, boolean includeVersion)
  {
    StoreFile f = new StoreFile();
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
