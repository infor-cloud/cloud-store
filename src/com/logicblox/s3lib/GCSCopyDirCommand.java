package com.logicblox.s3lib;

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
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
  private Storage _storage;

  public GCSCopyDirCommand(
      Storage storage,
      ListeningExecutorService s3Executor,
      ListeningScheduledExecutorService internalExecutor)
  {
    _storage = storage;
    _s3Executor = s3Executor;
    _executor = internalExecutor;
  }


  public ListenableFuture<List<S3File>> run(final CopyOptions options)
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

    List<ListenableFuture<S3File>> files = new ArrayList<>();

    List<S3File> filesToCopy = listObjects(
      options.getSourceBucketName(), options.getSourceKey(), options.isRecursive());

    List<ListenableFuture<S3File>> futures = new ArrayList<>();
    for(S3File src : filesToCopy)
      createCopyOp(futures, src, options, baseDirPath);

    if(options.isDryRun())
    {
      return Futures.immediateFuture(null);
    }
    else
    {
      return Futures.allAsList(futures);
    }
  }


  private void createCopyOp(
    List<ListenableFuture<S3File>> futures, final S3File src,
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

  private ListenableFuture<S3File> wrapCopyWithRetry(
    final CopyOptions options, final S3File src, final String destKey)
  {
    return executeWithRetry(_executor, new Callable<ListenableFuture<S3File>>()
    {
      public ListenableFuture<S3File> call()
      {
        return _s3Executor.submit(new Callable<S3File>()
        {
          public S3File call() throws IOException
          {
            return performCopy(options, src, destKey);
          }
        });
      }
    });
  }

  
  private S3File performCopy(CopyOptions options, S3File src, String destKey)
    throws IOException
  {
    // support for testing failures
    String srcUri = getUri(options.getSourceBucketName(), src.getKey());
    options.injectAbort(srcUri);

    Storage.Objects.Copy cmd = _storage.objects().copy(
      options.getSourceBucketName(), src.getKey(),
      options.getDestinationBucketName(), destKey,
      null);
    StorageObject resp = cmd.execute();
    return createS3File(resp, false);
  }
  

  private List<S3File> listObjects(String bucket, String prefix, boolean isRecursive)
    throws IOException
  {
    List<S3File> s3files = new ArrayList<S3File>();
    List<StorageObject> allObjs = new ArrayList<StorageObject>();
    Storage.Objects.List cmd = _storage.objects().list(bucket);
    cmd.setPrefix(prefix);
    if(!isRecursive)
      cmd.setDelimiter("/");
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
      s3files.add(createS3File(s, false));
    return s3files;
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
