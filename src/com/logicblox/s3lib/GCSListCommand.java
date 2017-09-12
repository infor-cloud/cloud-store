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

  private ListeningExecutorService _s3Executor;
  private ListeningScheduledExecutorService _executor;

  public GCSListCommand(
      ListeningExecutorService s3Executor,
      ListeningScheduledExecutorService internalExecutor)
  {
    _s3Executor = s3Executor;
    _executor = internalExecutor;
  }


  public ListenableFuture<List<StoreFile>> run(final ListOptions lsOptions)
  {
    ListenableFuture<List<StoreFile>> future =
        executeWithRetry(_executor, new Callable<ListenableFuture<List<StoreFile>>>()
        {
          public ListenableFuture<List<StoreFile>> call()
          {
            return runActual(lsOptions);
          }
          
          public String toString()
          {
            return "listing objects and directories for "
                + getUri(lsOptions.getBucket(), lsOptions.getObjectKey());
          }
        });
    
    return future;
  }
  

  private ListenableFuture<List<StoreFile>> runActual(final ListOptions lsOptions)
  {
    return _s3Executor.submit(new Callable<List<StoreFile>>()
    {
      public List<StoreFile> call()
        throws IOException
      {
        List<StoreFile> s3files = new ArrayList<StoreFile>();
        List<StorageObject> allObjs = new ArrayList<StorageObject>();
        Storage.Objects.List cmd = getGCSClient().objects().list(lsOptions.getBucket());
        cmd.setPrefix(lsOptions.getObjectKey());
        if(!lsOptions.isRecursive())
          cmd.setDelimiter("/");
        boolean ver = lsOptions.versionsIncluded();
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
          s3files.add(createStoreFile(s, ver));
        return s3files;
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
