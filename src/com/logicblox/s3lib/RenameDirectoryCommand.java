package com.logicblox.s3lib;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;


public class RenameDirectoryCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  private RenameOptions _options;
  private CloudStoreClient _client;

  public RenameDirectoryCommand(
    ListeningExecutorService httpExecutor,
    ListeningScheduledExecutorService internalExecutor,
    CloudStoreClient client,
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


  public ListenableFuture<List<StoreFile>> run()
    throws InterruptedException, ExecutionException, IOException
  {
    return startCopyThenDelete();
  }


  private ListenableFuture<List<StoreFile>> startCopyThenDelete()
    throws InterruptedException, ExecutionException, IOException
  {
    final String bucket = _options.getDestinationBucket();
    final String key = stripSlash(_options.getDestinationKey());
       // exists command doesn't allow trailing slash
    ListenableFuture<Metadata> destExists = _client.exists(bucket, key);
    return Futures.transform(
      destExists,
      new AsyncFunction<Metadata,List<StoreFile>>()
      {
        public ListenableFuture<List<StoreFile>> apply(Metadata mdata)
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


  private ListenableFuture<List<StoreFile>> copyThenDelete()
    throws InterruptedException, ExecutionException, IOException
  {
    CopyOptions copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_options.getSourceBucket())
       .setSourceKey(_options.getSourceKey())
       .setDestinationBucketName(_options.getDestinationBucket())
       .setDestinationKey(_options.getDestinationKey())
       .setRecursive(_options.isRecursive())
       .setDryRun(_options.isDryRun())
       .setCannedAcl(_options.getCannedAcl().orNull())
       .createCopyOptions();

    // hack -- exceptions are a bit of a mess.  copyToDir throws all sorts of stuff that 
    //         should be collected into an ExecutionException?
    ListenableFuture<List<StoreFile>> copyFuture = null;
    try
    {
      copyFuture = _client.copyToDir(copyOpts);
    }
    catch(URISyntaxException ex)
    {
      throw new ExecutionException("invalid URI in rename dir copy operation", ex);
    }

    return Futures.transform(
      copyFuture,
      new AsyncFunction<List<StoreFile>, List<StoreFile>>()
      {
        public ListenableFuture<List<StoreFile>> apply(final List<StoreFile> destFiles)
          throws InterruptedException, ExecutionException
        {
          DeleteOptions delOpts = new DeleteOptionsBuilder()
            .setBucket(_options.getSourceBucket())
            .setObjectKey(_options.getSourceKey())
            .setRecursive(_options.isRecursive())
            .setDryRun(_options.isDryRun())
            .createDeleteOptions();

          // need to return list of dest files
          return Futures.transform(
            _client.deleteDir(delOpts),
            new Function<List<StoreFile>, List<StoreFile>>()
            {
              public List<StoreFile> apply(List<StoreFile> deletedFiles)
              {
                return destFiles;
              }
            });
        }
      });
  }

}
