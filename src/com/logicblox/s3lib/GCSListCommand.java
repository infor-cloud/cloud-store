package com.logicblox.s3lib;

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;


public class GCSListCommand extends Command
{

  private ListOptions _options;
  private ListeningExecutorService _apiExecutor;
  private ListeningScheduledExecutorService _executor;

  public GCSListCommand(ListOptions options)
  {
    _options = options;
    _apiExecutor = _options.getCloudStoreClient().getApiExecutor();
    _executor = _options.getCloudStoreClient().getInternalExecutor();
  }


  public ListenableFuture<List<S3File>> run()
  {
    ListenableFuture<List<S3File>> future =
        executeWithRetry(_executor, new Callable<ListenableFuture<List<S3File>>>()
        {
          public ListenableFuture<List<S3File>> call()
          {
            return runActual();
          }
          
          public String toString()
          {
            return "listing objects and directories for "
                + getUri(_options.getBucketName(), _options.getObjectKey().orElse(""));
          }
        });
    
    return future;
  }
  

  private ListenableFuture<List<S3File>> runActual()
  {
    return _apiExecutor.submit(new Callable<List<S3File>>()
    {
      public List<S3File> call()
        throws IOException
      {
        List<S3File> s3files = new ArrayList<S3File>();
        List<StorageObject> allObjs = new ArrayList<StorageObject>();
        Storage.Objects.List cmd = getGCSClient().objects().list(_options.getBucketName());
        cmd.setPrefix(_options.getObjectKey().orElse(null));
        if(!_options.isRecursive())
          cmd.setDelimiter("/");
        boolean ver = _options.versionsIncluded();
        cmd.setVersions(ver);
        Objects objs;
        do
        {
          objs = cmd.execute();
          List<StorageObject> items = objs.getItems();
          if(items != null)
            allObjs.addAll(items);
          cmd.setPageToken(objs.getNextPageToken());
        } while (objs.getNextPageToken() != null);

        for(StorageObject s : allObjs)
          s3files.add(createS3File(s, ver));
        return s3files;
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
