package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.VersionListing;

import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.google.common.util.concurrent.ListenableFuture;
import com.amazonaws.services.s3.internal.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class ListVersionsCommand extends Command {
  
  private ListOptions _options;

  public ListVersionsCommand(ListOptions options) {
    super(options);
    _options = options;
  }
  
  public ListenableFuture<List<StoreFile>> run() {
    ListenableFuture<List<StoreFile>> future =
        executeWithRetry(_client.getInternalExecutor(), new Callable<ListenableFuture<List<StoreFile>>>() {
          
          
          public ListenableFuture<List<StoreFile>> call() {
            return runActual();
          }
          
          public String toString() {
            return "listing objects and directories for "
                + getUri(_options.getBucketName(), _options.getObjectKey().orElse(""));
          }
        });
    
    return future;
  }
  
  private ListenableFuture<List<StoreFile>> runActual() {
    return _client.getApiExecutor().submit(new Callable<List<StoreFile>>() {
      
      
      public List<StoreFile> call() {
        ListVersionsRequest req = new ListVersionsRequest()
            .withBucketName(_options.getBucketName())
            .withPrefix(_options.getObjectKey().orElse(null));
        if (! _options.isRecursive()) {
          req.setDelimiter("/");
        }
        
        List<StoreFile> all = new ArrayList<StoreFile>();
        VersionListing current = getAmazonS3Client().listVersions(req);
        appendVersionSummaryList(all, current.getVersionSummaries());
        if (! _options.dirsExcluded()) {
          appendVersionsDirStringList(all, current.getCommonPrefixes(), _options.getBucketName());
        }
        current = getAmazonS3Client().listNextBatchOfVersions(current);
        
        while (current.isTruncated()) {
          appendVersionSummaryList(all, current.getVersionSummaries());
          if (! _options.dirsExcluded()) {
            appendVersionsDirStringList(all, current.getCommonPrefixes(), _options.getBucketName());
          }
          current = getAmazonS3Client().listNextBatchOfVersions(current);
        }
        appendVersionSummaryList(all, current.getVersionSummaries());
        if (! _options.dirsExcluded()) {
          appendVersionsDirStringList(all, current.getCommonPrefixes(), _options.getBucketName());
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
