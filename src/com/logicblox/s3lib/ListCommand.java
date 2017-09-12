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

public class ListCommand extends Command {
  

  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;

  public ListCommand(ListeningExecutorService httpExecutor,
      ListeningScheduledExecutorService internalExecutor) {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
  }

  public ListenableFuture<List<StoreFile>> run(final ListOptions lsOptions) {
    ListenableFuture<List<StoreFile>> future =
        executeWithRetry(_executor, new Callable<ListenableFuture<List<StoreFile>>>() {
          public ListenableFuture<List<StoreFile>> call() {
            return runActual(lsOptions);
          }
          
          public String toString() {
            return "listing objects and directories for "
                + getUri(lsOptions.getBucket(), lsOptions.getObjectKey());
          }
        });
    
    return future;
  }
  
  private ListenableFuture<List<StoreFile>> runActual(final ListOptions lsOptions) {
    return _httpExecutor.submit(new Callable<List<StoreFile>>() {

      public List<StoreFile> call() {
        ListObjectsRequest req = new ListObjectsRequest()
            .withBucketName(lsOptions.getBucket())
            .withPrefix(lsOptions.getObjectKey());
        if (! lsOptions.isRecursive()) {
          req.setDelimiter("/");
        }

        List<StoreFile> all = new ArrayList<StoreFile>();
        ObjectListing current = getAmazonS3Client().listObjects(req);
        appendS3ObjectSummaryList(all, current.getObjectSummaries());
        if (! lsOptions.dirsExcluded()) {
          appendS3DirStringList(all, current.getCommonPrefixes(), lsOptions.getBucket());
        }
        current = getAmazonS3Client().listNextBatchOfObjects(current);
        
        while (current.isTruncated()) {
          appendS3ObjectSummaryList(all, current.getObjectSummaries());
          if (! lsOptions.dirsExcluded()) {
            appendS3DirStringList(all, current.getCommonPrefixes(), lsOptions.getBucket());
          }
          current = getAmazonS3Client().listNextBatchOfObjects(current);
        }
        appendS3ObjectSummaryList(all, current.getObjectSummaries());
        if (! lsOptions.dirsExcluded()) {
          appendS3DirStringList(all, current.getCommonPrefixes(), lsOptions.getBucket());
        }
        
        return all;
      }
    });
  }
  
  private List<StoreFile> appendS3ObjectSummaryList(
      List<StoreFile> all,
      List<S3ObjectSummary> appendList) {
    for (S3ObjectSummary o : appendList) {
      all.add(S3ObjectSummaryToStoreFile(o));
    }
    
    return all;
  }
  
  private List<StoreFile> appendS3DirStringList(
      List<StoreFile> all,
      List<String> appendList,
      String bucket) {
    for (String o : appendList) {
      all.add(S3DirStringToStoreFile(o, bucket));
    }
    
    return all;
  }
  
  private StoreFile S3ObjectSummaryToStoreFile(S3ObjectSummary o) {
    StoreFile of = new StoreFile();
    of.setKey(o.getKey());
    of.setETag(o.getETag());
    of.setBucketName(o.getBucketName());
    of.setSize(o.getSize());
    return of;
  }
  
  private StoreFile S3DirStringToStoreFile(String dir, String bucket) {
    StoreFile df = new StoreFile();
    df.setKey(dir);
    df.setBucketName(bucket);
    df.setSize(new Long(0));
    
    return df;
  }
}
