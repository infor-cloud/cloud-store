package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.util.concurrent.Callable;


public class DeleteCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;

  public DeleteCommand(
    ListeningExecutorService httpExecutor,
    ListeningScheduledExecutorService internalExecutor)
  {
    if(httpExecutor == null)
      throw new IllegalArgumentException("non-null http executor is required");
    if(internalExecutor == null)
      throw new IllegalArgumentException("non-null internal executor is required");

    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
  }


  public ListenableFuture<S3File> run(final String bucket, final String object)
  {
    ListenableFuture<S3File> future =
      executeWithRetry(
        _executor,
        new Callable<ListenableFuture<S3File>>()
        {
          public ListenableFuture<S3File> call()
          {
            return runActual(bucket, object);
          }

          public String toString()
          {
            return "delete s3://" + bucket + "/" + object;
          }
        });

    return future;
  }

  private ListenableFuture<S3File> runActual(
    final String bucket, final String object)
  {
    return _httpExecutor.submit(
      new Callable<S3File>()
      {
        public S3File call()
        {
          DeleteObjectRequest req = new DeleteObjectRequest(bucket, object);
          getAmazonS3Client().deleteObject(req);
          S3File file = new S3File();
          file.setBucketName(bucket);
          file.setKey(object);
          return file;
        }
      });
  }

}
