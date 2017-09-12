package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.VersionListing;

import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.amazonaws.services.s3.internal.Constants;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

public class ListVersionsCommand extends Command {
  
  
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  
  public ListVersionsCommand(ListeningExecutorService httpExecutor,
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
        ListVersionsRequest req = new ListVersionsRequest()
            .withBucketName(lsOptions.getBucket())
            .withPrefix(lsOptions.getObjectKey());
        if (! lsOptions.isRecursive()) {
          req.setDelimiter("/");
        }
        
        List<StoreFile> all = new ArrayList<StoreFile>();
        VersionListing current = getAmazonS3Client().listVersions(req);
        appendVersionSummaryList(all, current.getVersionSummaries());
        if (! lsOptions.dirsExcluded()) {
          appendVersionsDirStringList(all, current.getCommonPrefixes(), lsOptions.getBucket());
        }
        current = getAmazonS3Client().listNextBatchOfVersions(current);
        
        while (current.isTruncated()) {
          appendVersionSummaryList(all, current.getVersionSummaries());
          if (! lsOptions.dirsExcluded()) {
            appendVersionsDirStringList(all, current.getCommonPrefixes(), lsOptions.getBucket());
          }
          current = getAmazonS3Client().listNextBatchOfVersions(current);
        }
        appendVersionSummaryList(all, current.getVersionSummaries());
        if (! lsOptions.dirsExcluded()) {
          appendVersionsDirStringList(all, current.getCommonPrefixes(), lsOptions.getBucket());
        }
        
        return all;
      }
    });
  }
  
  private List<StoreFile> appendVersionSummaryList(
      List<StoreFile> all,
      List<S3VersionSummary> appendList) {
    for (S3VersionSummary o : appendList) {
      all.add(versionSummaryToStoreFile(o));
    }
    
    return all;
  }
  
  private List<StoreFile> appendVersionsDirStringList(
      List<StoreFile> all,
      List<String> appendList,
      String bucket) {
    for (String o : appendList) {
      all.add(versionsDirStringToStoreFile(o, bucket));
    }
    
    return all;
  }
  
  private StoreFile versionSummaryToStoreFile(S3VersionSummary o) {
    StoreFile of = new StoreFile();
    of.setKey(o.getKey());
    of.setETag(o.getETag());
    of.setBucketName(o.getBucketName());
    if (! o.getVersionId().equals(Constants.NULL_VERSION_ID)) {
      of.setVersionId(o.getVersionId());
    }
    if (o.getLastModified() != null) {
      of.setTimestamp(o.getLastModified());
    }
    of.setSize(o.getSize());
    return of;
  }

  private StoreFile versionsDirStringToStoreFile(String dir, String bucket) {
    StoreFile df = new StoreFile();
    df.setKey(dir);
    df.setBucketName(bucket);
    return df;
  }
}
