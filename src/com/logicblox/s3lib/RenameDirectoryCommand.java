package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;


public class RenameDirectoryCommand extends Command
{
  private RenameOptions _options;
  private CloudStoreClient _client;

  public RenameDirectoryCommand(RenameOptions options)
  {
    _options = options;
    _client = _options.getCloudStoreClient();
  }

  public ListenableFuture<List<S3File>> run()
    throws InterruptedException, ExecutionException, IOException
  {
    return startCopyThenDelete();
  }

  private ListenableFuture<List<S3File>> startCopyThenDelete()
    throws InterruptedException, ExecutionException, IOException
  {
    final String bucket = _options.getDestinationBucketName();
    final String key = stripSlash(_options.getDestinationObjectKey());
       // exists command doesn't allow trailing slash
    ListenableFuture<ObjectMetadata> destExists = _client.exists(bucket, key);
    return Futures.transform(
      destExists,
      new AsyncFunction<ObjectMetadata,List<S3File>>()
      {
        public ListenableFuture<List<S3File>> apply(ObjectMetadata mdata)
          throws Exception
        {
          if(null != mdata)
          {
            throw new UsageException("Cannot overwrite existing destination object '"
              + getUri(bucket, key));
          }
          return copyThenDelete();
        }
      });
  }


  private String stripSlash(String s)
  {
    if(s.endsWith("/"))
      return s.substring(0, s.length() - 1);
    else
      return s;
  }


  private ListenableFuture<List<S3File>> copyThenDelete()
    throws InterruptedException, ExecutionException, IOException
  {
    CopyOptions copyOpts = new CopyOptionsBuilder()
       .setCloudStoreClient(_options.getCloudStoreClient())
       .setSourceBucketName(_options.getSourceBucketName())
       .setSourceObjectKey(_options.getSourceObjectKey())
       .setDestinationBucketName(_options.getDestinationBucketName())
       .setDestinationObjectKey(_options.getDestinationObjectKey())
       .setRecursive(_options.isRecursive())
       .setDryRun(_options.isDryRun())
       .setCannedAcl(_options.getCannedAcl())
       .createCopyOptions();

    // hack -- exceptions are a bit of a mess.  copyToDir throws all sorts of stuff that 
    //         should be collected into an ExecutionException?
    ListenableFuture<List<S3File>> copyFuture = null;
    copyFuture = _client.copyToDir(copyOpts);

    return Futures.transform(
      copyFuture,
      new AsyncFunction<List<S3File>, List<S3File>>()
      {
        public ListenableFuture<List<S3File>> apply(final List<S3File> destFiles)
          throws InterruptedException, ExecutionException
        {
          DeleteOptions delOpts = new DeleteOptionsBuilder()
            .setCloudStoreClient((_options.getCloudStoreClient()))
            .setBucketName(_options.getSourceBucketName())
            .setObjectKey(_options.getSourceObjectKey())
            .setRecursive(_options.isRecursive())
            .setDryRun(_options.isDryRun())
            .createDeleteOptions();

          // need to return list of dest files
          return Futures.transform(
            _client.deleteDir(delOpts),
            new Function<List<S3File>, List<S3File>>()
            {
              public List<S3File> apply(List<S3File> deletedFiles)
              {
                return destFiles;
              }
            });
        }
      });
  }

}
