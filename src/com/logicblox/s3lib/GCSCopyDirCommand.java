package com.logicblox.s3lib;

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;


public class GCSCopyDirCommand extends Command
{

  private ListeningExecutorService _s3Executor;
  private ListeningScheduledExecutorService _executor;

  public GCSCopyDirCommand(
      ListeningExecutorService s3Executor,
      ListeningScheduledExecutorService internalExecutor)
  {
    _s3Executor = s3Executor;
    _executor = internalExecutor;
  }


  public ListenableFuture<List<StoreFile>> run(final CopyOptions options)
    throws IOException
  {
    if(!options.getDestinationKey().endsWith("/") && !options.getDestinationKey().equals(""))
      throw new UsageException("Destination directory key should end with a '/'");

    String baseDirPath = "";
    if(options.getSourceKey().length() > 0)
    {
      int endIndex = options.getSourceKey().lastIndexOf("/");
      if(endIndex != -1)
        baseDirPath = options.getSourceKey().substring(0, endIndex+1);
    }
    final String baseDirPathF = baseDirPath;

    ListenableFuture<List<StoreFile>> listFuture = getListFuture(
      options.getSourceBucketName(), options.getSourceKey(), options.isRecursive());
    ListenableFuture<List<StoreFile>> result = Futures.transform(
      listFuture,
      new AsyncFunction<List<StoreFile>, List<StoreFile>>()
      {
        public ListenableFuture<List<StoreFile>> apply(List<StoreFile> filesToCopy)
        {
          List<ListenableFuture<StoreFile>> files = new ArrayList<>();
          List<ListenableFuture<StoreFile>> futures = new ArrayList<>();
          for(StoreFile src : filesToCopy)
            createCopyOp(futures, src, options, baseDirPathF);

          if(options.isDryRun())
            return Futures.immediateFuture(null);
          else
            return Futures.allAsList(futures);
        }
      });
    return result;
  }


  private void createCopyOp(
    List<ListenableFuture<StoreFile>> futures, final StoreFile src,
    final CopyOptions options, String baseDirPath)
  {
    if(!src.getKey().endsWith("/"))
    {
      String destKeyLastPart = src.getKey().substring(baseDirPath.length());
      final String destKey = options.getDestinationKey() + destKeyLastPart;
      if(options.isDryRun())
      {
        System.out.println("<DRYRUN> copying '"
          + getUri(options.getSourceBucketName(), src.getKey())
          + "' to '"
          + getUri(options.getDestinationBucketName(), destKey) + "'");
      }
      else
      {
        futures.add(wrapCopyWithRetry(options, src, destKey));
      }
    }
  }

  private ListenableFuture<StoreFile> wrapCopyWithRetry(
    final CopyOptions options, final StoreFile src, final String destKey)
  {
    return executeWithRetry(_executor, new Callable<ListenableFuture<StoreFile>>()
    {
      public ListenableFuture<StoreFile> call()
      {
        return _s3Executor.submit(new Callable<StoreFile>()
        {
          public StoreFile call() throws IOException
          {
            return performCopy(options, src, destKey);
          }
        });
      }
    });
  }

  
  private StoreFile performCopy(CopyOptions options, StoreFile src, String destKey)
    throws IOException
  {
    // support for testing failures
    String srcUri = getUri(options.getSourceBucketName(), src.getKey());
    options.injectAbort(srcUri);

    Storage.Objects.Copy cmd = getGCSClient().objects().copy(
      options.getSourceBucketName(), src.getKey(),
      options.getDestinationBucketName(), destKey,
      null);
    StorageObject resp = cmd.execute();
    return createStoreFile(resp, false);
  }
  

  private ListenableFuture<List<StoreFile>> getListFuture(
    String bucket, String prefix, boolean isRecursive)
  {
    // FIXME - if we gave commands a CloudStoreClient when they were created
    //         we could then use client.listObjects() instead of all this....
    ListOptions listOpts = (new ListOptionsBuilder())
      .setBucket(bucket)
      .setObjectKey(prefix)
      .setRecursive(isRecursive)
      .createListOptions();
    GCSListCommand cmd = new GCSListCommand(_s3Executor, _executor);
    cmd.setRetryClientException(_stubborn);
    cmd.setRetryCount(_retryCount);
    cmd.setAmazonS3Client(getAmazonS3Client());
    cmd.setGCSClient(getGCSClient());
    cmd.setScheme("gs://");
    return cmd.run(listOpts);
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
