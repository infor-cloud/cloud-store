package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.util.List;
import java.util.concurrent.Callable;

public class ListObjectsCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;

  public ListObjectsCommand(
          ListeningExecutorService httpExecutor,
          ListeningScheduledExecutorService internalExecutor)
  {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
  }

  public ListenableFuture<List<S3ObjectSummary>> run(final String bucket,
                                                     final String prefix,
                                                     final boolean recursive)
  {
    ListenableFuture<List<S3ObjectSummary>> future =
      executeWithRetry(
        _executor,
        new Callable<ListenableFuture<List<S3ObjectSummary>>>()
        {
          public ListenableFuture<List<S3ObjectSummary>> call()
          {
            return runActual(bucket, prefix, recursive);
          }

          public String toString()
          {
            return "listing objects for " + getUri(bucket, prefix);
          }
        });

    return future;
  }

  private ListenableFuture<List<S3ObjectSummary>> runActual(final String bucket,
                                                            final String prefix,
                                                            final boolean recursive)
  {
    return _httpExecutor.submit(
      new Callable<List<S3ObjectSummary>>()
      {
        public List<S3ObjectSummary> call()
        {
          ListObjectsRequest req = new ListObjectsRequest()
              .withBucketName(bucket)
              .withPrefix(prefix);
          if (!recursive) req.setDelimiter("/");

          ObjectListing current = getAmazonS3Client().listObjects(req);
          List<S3ObjectSummary> keyList = current.getObjectSummaries();
          current = getAmazonS3Client().listNextBatchOfObjects(current);

          while (current.isTruncated()){
            keyList.addAll(current.getObjectSummaries());
            current = getAmazonS3Client().listNextBatchOfObjects(current);
          }
          keyList.addAll(current.getObjectSummaries());

          return keyList;
        }
      });
  }
}
