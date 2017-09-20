package com.logicblox.s3lib;

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;


public class GCSCopyDirCommand extends Command
{
  private CopyOptions _options;

  public GCSCopyDirCommand(CopyOptions options)
  {
    super(options);
    _options = options;
  }

  public ListenableFuture<List<S3File>> run()
    throws IOException
  {
    if(!_options.getDestinationObjectKey().endsWith("/") && !_options.getDestinationObjectKey().equals(""))
      throw new UsageException("Destination directory key should end with a '/'");

    String baseDirPath = "";
    if(_options.getSourceObjectKey().length() > 0)
    {
      int endIndex = _options.getSourceObjectKey().lastIndexOf("/");
      if(endIndex != -1)
        baseDirPath = _options.getSourceObjectKey().substring(0, endIndex + 1);
    }
    final String baseDirPathF = baseDirPath;

    ListenableFuture<List<S3File>> listFuture = getListFuture(
      _options.getSourceBucketName(), _options.getSourceObjectKey(), _options.isRecursive());
    ListenableFuture<List<S3File>> result = Futures.transform(
      listFuture,
      new AsyncFunction<List<S3File>, List<S3File>>()
      {
        public ListenableFuture<List<S3File>> apply(List<S3File> filesToCopy)
        {
          List<ListenableFuture<S3File>> files = new ArrayList<>();
          List<ListenableFuture<S3File>> futures = new ArrayList<>();
          for(S3File src : filesToCopy)
            createCopyOp(futures, src, baseDirPathF);

          if(_options.isDryRun())
            return Futures.immediateFuture(null);
          else
            return Futures.allAsList(futures);
        }
      });
    return result;
  }


  private void createCopyOp(
    List<ListenableFuture<S3File>> futures, final S3File src, String baseDirPath)
  {
    if(!src.getKey().endsWith("/"))
    {
      String destKeyLastPart = src.getKey().substring(baseDirPath.length());
      final String destKey = _options.getDestinationObjectKey() + destKeyLastPart;
      if(_options.isDryRun())
      {
        System.out.println("<DRYRUN> copying '"
          + getUri(_options.getSourceBucketName(), src.getKey())
          + "' to '"
          + getUri(_options.getDestinationBucketName(), destKey) + "'");
      }
      else
      {
        futures.add(wrapCopyWithRetry(src, destKey));
      }
    }
  }

  private ListenableFuture<S3File> wrapCopyWithRetry(
    final S3File src, final String destKey)
  {
    return executeWithRetry(_client.getInternalExecutor(), new Callable<ListenableFuture<S3File>>()
    {
      public ListenableFuture<S3File> call()
      {
        return _client.getApiExecutor().submit(new Callable<S3File>()
        {
          public S3File call() throws IOException
          {
            return performCopy(src, destKey);
          }
        });
      }
    });
  }

  private S3File performCopy(S3File src, String destKey)
    throws IOException
  {
    // support for testing failures
    String srcUri = getUri(_options.getSourceBucketName(), src.getKey());
    _options.injectAbort(srcUri);

    Storage.Objects.Copy cmd = getGCSClient().objects().copy(
      _options.getSourceBucketName(), src.getKey(),
      _options.getDestinationBucketName(), destKey,
      null);
    StorageObject resp = cmd.execute();
    return createS3File(resp, false);
  }

  private ListenableFuture<List<S3File>> getListFuture(
    String bucket, String prefix, boolean isRecursive)
  {
    // FIXME - if we gave commands a CloudStoreClient when they were created
    //         we could then use client.listObjects() instead of all this....
    ListOptions listOpts = _client.getOptionsBuilderFactory()
      .newListOptionsBuilder()
      .setBucketName(bucket)
      .setObjectKey(prefix)
      .setRecursive(isRecursive)
      .createOptions();

    return _client.listObjects(listOpts);
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
