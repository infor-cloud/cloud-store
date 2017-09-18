package com.logicblox.s3lib;

import com.google.api.services.storage.Storage;
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
  private CopyOptions _options;
  private ListeningExecutorService _apiExecutor;
  private ListeningScheduledExecutorService _executor;

  public GCSCopyCommand(CopyOptions options)
  {
    _options = options;
    _apiExecutor = _options.getCloudStoreClient().getApiExecutor();
    _executor = _options.getCloudStoreClient().getInternalExecutor();
  }

  public ListenableFuture<S3File> run()
  {
    if(_options.isDryRun())
    {
      System.out.println("<DRYRUN> copying '"
                         + getUri(_options.getSourceBucketName(), _options.getSourceObjectKey())
                         + "' to '"
                         + getUri(_options.getDestinationBucketName(), _options.getDestinationObjectKey()) + "'");
      return Futures.immediateFuture(null);
    }
    else
    {
      ListenableFuture<S3File> future =
        executeWithRetry(_executor, new Callable<ListenableFuture<S3File>>()
        {
          public ListenableFuture<S3File> call()
          {
            return runActual();
          }
          
          public String toString()
          {
            return "copying object from "
                   + getUri(_options.getSourceBucketName(), _options
              .getSourceObjectKey()) + " to "
                   + getUri(_options.getDestinationBucketName(), _options.getDestinationObjectKey());
          }
        });
    
      return future;
    }
  }
  

  private ListenableFuture<S3File> runActual()
  {
    return _apiExecutor.submit(new Callable<S3File>()
    {
      public S3File call() throws IOException
      {
        // support for testing failures
        String srcUri = getUri(_options.getSourceBucketName(), _options.getSourceObjectKey());
        _options.injectAbort(srcUri);

        StorageObject objectMetadata = null;
        Map<String,String> userMetadata = _options.getUserMetadata().orElse(null);
        if (userMetadata != null)
        {
          Storage.Objects.Get get = getGCSClient().objects().get(
            _options.getSourceBucketName(), _options.getSourceObjectKey());
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
          _options.getSourceBucketName(), _options.getSourceObjectKey(),
          _options.getDestinationBucketName(), _options.getDestinationObjectKey(),
          objectMetadata);

        _options.getCannedACL().ifPresent(ca -> cmd.setDestinationPredefinedAcl(ca));

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
