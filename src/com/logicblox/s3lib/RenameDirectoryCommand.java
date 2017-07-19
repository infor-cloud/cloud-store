package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;


public class RenameDirectoryCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  private RenameOptions _options;
  private CloudStoreClient _client;
  private List<ListenableFuture<S3File>> _futures = new ArrayList<ListenableFuture<S3File>>();
  private Map<String,String> _cleanupTable = new HashMap<String,String>();
     // maps src to dest keys.  used to clean up if something fails


  public RenameDirectoryCommand(
    ListeningExecutorService httpExecutor,
    ListeningScheduledExecutorService internalExecutor,
    CloudStoreClient client,
    String acl,
    RenameOptions opts)
  {
    if(httpExecutor == null)
      throw new IllegalArgumentException("non-null http executor is required");
    if(internalExecutor == null)
      throw new IllegalArgumentException("non-null internal executor is required");

    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
    _client = client;
    _options = opts;
  }

  public ListenableFuture<List<S3File>> run()
    throws InterruptedException, ExecutionException, IOException
  {
    checkDestExists();
    List<S3File> toRename = queryFiles();
    prepareFutures(toRename);

    if(_options.isDryRun())
    {
      return Futures.immediateFuture(null);
    }
    else
    {
      return Futures.withFallback(
        Futures.allAsList(_futures),
        new FutureFallback<List<S3File>>()
        {
          public ListenableFuture<List<S3File>> create(Throwable t)
          {
            try
            {
              cleanup();
            }
            catch(Exception ex)
            {
              return Futures.immediateFailedFuture(new Exception(
                "Error cleaning up after rename failure:  " + ex.getMessage(), ex));
            }

            return Futures.immediateFailedFuture(new Exception(
              "Error renaming '"
                + getUri(_options.getSourceBucket(), _options.getSourceKey())
                + "'" + ":  " + t.getMessage(), t));
          }
        });
    }
  }


  private void cleanup()
    throws InterruptedException, ExecutionException, IOException
  {
    // stop any rename futures that are still running
    for(ListenableFuture<S3File> f : _futures)
      f.cancel(true);
    _futures.clear();

    // HACK - Occassionally when we're renaming a directory and a failure
    // happens with GCS, one file will be copied but it doesn't appear to
    // exist yet when we try to detect and delete it to clean up.  Inserting
    // a small delay here seems to correct this, but is obviously far less
    // than ideal and may not always work.
//    Thread.currentThread().sleep(1000);

    // move back any files that completed the rename
    for(Map.Entry<String,String> e : _cleanupTable.entrySet())
    {
      String srcKey = e.getKey();
      String destKey = e.getValue();
      replaceFile(srcKey, destKey);
      deleteFile(_options.getDestinationBucket(), destKey);
    }
  }


  private void replaceFile(String srcKey, String destKey)
    throws InterruptedException, ExecutionException, IOException
  {
    String srcBucket = _options.getSourceBucket();
    String destBucket = _options.getDestinationBucket();

    // if dest exists (copy succeeded) and src doesn't (delete succeeded too)
    if((null != _client.exists(destBucket, destKey).get())
        && (null == _client.exists(srcBucket, srcKey).get()))
    {
      CopyOptions copyOpts = new CopyOptionsBuilder()
          .setSourceBucketName(destBucket)
          .setSourceKey(destKey)
          .setDestinationBucketName(srcBucket)
          .setDestinationKey(srcKey)
          .setIgnoreAbortInjection(true)
          .createCopyOptions();
      _client.copy(copyOpts).get();
    }
  }


  private void deleteFile(String bucket, String key)
    throws InterruptedException, ExecutionException, IOException
  {
    if(null != _client.exists(bucket, key).get())
    {
      DeleteOptions deleteOpts = new DeleteOptionsBuilder()
        .setBucket(bucket)
        .setObjectKey(key)
        .setForceDelete(true)
        .setIgnoreAbortInjection(true)
        .createDeleteOptions();
      _client.delete(deleteOpts).get();
    }
  }

  
  private void checkDestExists()
  {
    String bucket = _options.getDestinationBucket();
    String key = _options.getDestinationKey();

    try
    {
      if(_client.exists(bucket, key).get() != null)
        throw new UsageException("Cannot overwrite existing destination object '"
          + getUri(bucket, key));
    }
    catch(InterruptedException | ExecutionException ex)
    {
      throw new UsageException(
        "Error checking object existence: " + ex.getMessage(), ex);
    }
  }


  private void prepareFutures(List<S3File> toRename)
    throws IOException
  {
    String destDir = _options.getDestinationKey();
    if(!destDir.endsWith("/"))
      destDir = destDir + "/";
    int srcIdx = _options.getSourceKey().length();
    for(S3File src : toRename)
    {
      String destKey = destDir + src.getKey().substring(srcIdx);
      if(_options.isDryRun())
      {
        System.out.println("<DRYRUN> renaming '"
          + getUri(src.getBucketName(), src.getKey())
          + "' to '"
          + getUri(_options.getDestinationBucket(), destKey)
          + "'");
      }
      else
      {
        _cleanupTable.put(src.getKey(), destKey);
        RenameOptions opts = new RenameOptionsBuilder()
          .setSourceBucket(src.getBucketName())
          .setSourceKey(src.getKey())
          .setDestinationBucket(_options.getDestinationBucket())
          .setDestinationKey(destKey)
          .setRecursive(false)
          .createRenameOptions();
        _futures.add(_client.rename(opts));
      }
    }
  }


  private List<S3File> queryFiles()
    throws InterruptedException, ExecutionException
  {
    // find all files that need to be renamed
    ListOptions opts = new ListOptionsBuilder()
        .setBucket(_options.getSourceBucket())
        .setObjectKey(_options.getSourceKey())
        .setRecursive(_options.isRecursive())
        .createListOptions();
    List<S3File> matches = new ArrayList<S3File>();
    List<S3File> potential = _client.listObjects(opts).get();
    for(S3File f : potential)
    {
      if(!f.getKey().endsWith("/"))
        matches.add(f);
    }
    if(matches.isEmpty())
    {
      throw new UsageException("No objects found that match '"
        + getUri(_options.getSourceBucket(), _options.getSourceKey()) + "'");
    }
    return matches;
  }
}
