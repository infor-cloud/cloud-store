package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Callable;


public class DeleteCommand extends Command
{
  private DeleteOptions _options;

  public DeleteCommand(DeleteOptions options)
  {
    super(options);
    _options = options;
  }


  public ListenableFuture<S3File> run()
  {
    final String bucket = _options.getBucket();
    final String key = _options.getObjectKey();
    final boolean forceDelete = _options.forceDelete();

    ExistsOptions opts = _client.getOptionsBuilderFactory()
      .newExistsOptionsBuilder()
      .setBucket(bucket)
      .setObjectKey(key)
      .createOptions();

    ListenableFuture<ObjectMetadata> existsFuture = _client.exists(opts);

    ListenableFuture<S3File> result = Futures.transform(
      existsFuture,
      new AsyncFunction<ObjectMetadata,S3File>()
      {
        public ListenableFuture<S3File> apply(ObjectMetadata mdata)
          throws UsageException
        {
          if(null == mdata)
          {
            if(forceDelete)
              return Futures.immediateFuture(new S3File());
            else
              throw new UsageException("Object not found at " + getUri(bucket, key));
          }
          return getDeleteFuture();
        }
      });

      return result;
  }


  private ListenableFuture<S3File> getDeleteFuture()
  {
    final String bucket = _options.getBucket();
    final String key = _options.getObjectKey();

    ListenableFuture<S3File> deleteFuture = executeWithRetry(
      _client.getInternalExecutor(),
      new Callable<ListenableFuture<S3File>>()
      {
        public ListenableFuture<S3File> call()
        {
          return runActual();
        }

        public String toString()
        {
          return "delete " + getUri(bucket, key);
        }
      });
    return deleteFuture;
  }


  private ListenableFuture<S3File> runActual()
  {
    final String srcUri = getUri(_options.getBucket(), _options.getObjectKey());
    if(_options.isDryRun())
    {
      System.out.println("<DRYRUN> deleting '" + srcUri + "'");
      return Futures.immediateFuture(null);
    }
    else
    {
      return _client.getApiExecutor().submit(
        new Callable<S3File>()
        {
          public S3File call()
          {
            // support for testing failures
            _options.injectAbort(srcUri);

            String bucket = _options.getBucket();
            String key = _options.getObjectKey();
            DeleteObjectRequest req = new DeleteObjectRequest(bucket, key);
            getAmazonS3Client().deleteObject(req);
            S3File file = new S3File();
            file.setBucketName(bucket);
            file.setKey(key);
            return file;
          }
        });
    }
  }
}
