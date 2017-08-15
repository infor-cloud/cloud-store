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
    final String baseDirPathF = baseDirPath;

    ListenableFuture<List<S3File>> listFuture = getListFuture(
      options.getSourceBucketName(), options.getSourceKey(), options.isRecursive());
    ListenableFuture<List<S3File>> result = Futures.transform(
      listFuture,
      new AsyncFunction<List<S3File>, List<S3File>>()
      {
        public ListenableFuture<List<S3File>> apply(List<S3File> filesToCopy)
        {
          List<ListenableFuture<S3File>> files = new ArrayList<>();
          List<ListenableFuture<S3File>> futures = new ArrayList<>();
          for(S3File src : filesToCopy)
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

    Storage.Objects.Copy cmd = getGCSClient().objects().copy(
      options.getSourceBucketName(), src.getKey(),
      options.getDestinationBucketName(), destKey,
      null);
    StorageObject resp = cmd.execute();
    return createS3File(resp, false);
  }
  

  private ListenableFuture<List<S3File>> getListFuture(
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
