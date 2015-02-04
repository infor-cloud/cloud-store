package com.logicblox.s3lib;


import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.util.concurrent.*;
import java.util.List;
import java.util.concurrent.Callable;

public class ListObjectsAndDirsCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;

  public ListObjectsAndDirsCommand(
          ListeningExecutorService httpExecutor,
          ListeningScheduledExecutorService internalExecutor)
  {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
  }

  public ListenableFuture<List<String>> run(final String bucket, final String prefix, final boolean recursive)
  {
    ListenableFuture<List<String>> future =
      executeWithRetry(
        _executor,
        new Callable<ListenableFuture<List<String>>>()
        {
          public ListenableFuture<List<String>> call()
          {
            return runActual(bucket, prefix, recursive);
          }

          public String toString()
          {
            return "list all object of s3://" + bucket + "/" + prefix;
          }
        });

    return future;
  }

  private ListenableFuture<List<String>> runActual(final String bucket, final String prefix, final boolean recursive)
  {
    return _httpExecutor.submit(
      new Callable<List<String>>()
      {
        public List<String> call()
        {
          ListObjectsRequest req = new ListObjectsRequest().withBucketName(bucket).withPrefix(prefix);
          if (! recursive) req.setDelimiter("/");

          ObjectListing current = getAmazonS3Client().listObjects(req);
          List<S3ObjectSummary> keyList = current.getObjectSummaries();
          List<String> all = current.getCommonPrefixes();
          current = getAmazonS3Client().listNextBatchOfObjects(current);

          while (current.isTruncated()){
            keyList.addAll(current.getObjectSummaries());
            all.addAll(current.getCommonPrefixes());
            current = getAmazonS3Client().listNextBatchOfObjects(current);
          }
          keyList.addAll(current.getObjectSummaries());
          all.addAll(current.getCommonPrefixes());

          for (S3ObjectSummary key: keyList)
          {
            all.add(key.getKey());
          }

          return all;
        }
      });
  }
}
