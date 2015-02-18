package com.logicblox.s3lib;


import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.util.ArrayList;
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

  public ListenableFuture<List<S3File>> run(final String bucket, final String prefix, final boolean recursive)
  {
    ListenableFuture<List<S3File>> future =
      executeWithRetry(
        _executor,
        new Callable<ListenableFuture<List<S3File>>>()
        {
          public ListenableFuture<List<S3File>> call()
          {
            return runActual(bucket, prefix, recursive);
          }

          public String toString()
          {
            return "list all object of " + getScheme() + bucket + "/" + prefix;
          }
        });

    return future;
  }

  private ListenableFuture<List<S3File>> runActual(final String bucket, final String prefix, final boolean recursive)
  {
    return _httpExecutor.submit(
      new Callable<List<S3File>>()
      {
        public List<S3File> call()
        {
          ListObjectsRequest req = new ListObjectsRequest().withBucketName(bucket).withPrefix(prefix);
          if (! recursive) req.setDelimiter("/");

          List<S3File> all = new ArrayList<S3File>();
          ObjectListing current = getAmazonS3Client().listObjects(req);
          appendS3ObjectSummaryList(all, current.getObjectSummaries());
          appendS3DirStringList(all, current.getCommonPrefixes(), bucket);
          current = getAmazonS3Client().listNextBatchOfObjects(current);

          while (current.isTruncated()){
            appendS3ObjectSummaryList(all, current.getObjectSummaries());
            appendS3DirStringList(all, current.getCommonPrefixes(), bucket);
            current = getAmazonS3Client().listNextBatchOfObjects(current);
          }
          appendS3ObjectSummaryList(all, current.getObjectSummaries());
          appendS3DirStringList(all, current.getCommonPrefixes(), bucket);

          return all;
        }
      });
  }

  private List<S3File> appendS3ObjectSummaryList(List<S3File> all, List<S3ObjectSummary> appendList) {
    for (S3ObjectSummary o : appendList) {
      all.add(S3ObjectSummaryToS3File(o));
    }

    return all;
  }

  private List<S3File> appendS3DirStringList(List<S3File> all, List<String> appendList, String bucket) {
    for (String o : appendList) {
      all.add(S3DirStringToS3File(o, bucket));
    }

    return all;
  }

  private S3File S3ObjectSummaryToS3File(S3ObjectSummary o) {
    S3File of = new S3File();
    of.setKey(o.getKey());
    of.setETag(o.getETag());
    of.setBucketName(o.getBucketName());
    return of;
  }

  private S3File S3DirStringToS3File(String dir, String bucket) {
    S3File df = new S3File();
    df.setKey(dir);
    df.setBucketName(bucket);

    return df;
  }
}
