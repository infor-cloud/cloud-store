package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.DeleteObjectRequest;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;


public class RenameCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  private CloudStoreClient _client;
  private RenameOptions _options;
  private String _acl;


  public RenameCommand(
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
    _acl = acl;
  }


  public ListenableFuture<S3File> run()
    throws IOException
  {
    checkSourceExists();
    checkDestExists();

    final String destKey = getDestKey();
    final String srcUri =
      getUri(_options.getSourceBucket(), _options.getSourceKey());

    if(_options.isDryRun())
    {
      System.out.println("<DRYRUN> renaming '"
        + srcUri
	+ "' to '"
        + getUri(_options.getDestinationBucket(), destKey)
	+ "'");
      return Futures.immediateFuture(null);
    }
    else
    {
      final DeleteOptions deleteOpts = new DeleteOptionsBuilder()
        .setBucket(_options.getSourceBucket())
	.setObjectKey(_options.getSourceKey())
        .createDeleteOptions();
      ListenableFuture<S3File> copyAndDelete = Futures.transform(
        getCopyOp(),
	new AsyncFunction<S3File,S3File>()
	{
	  public ListenableFuture<S3File> apply(S3File srcFile)
	  {
	    return _client.delete(deleteOpts);
	  }
	}
      );

      // return S3File with the dest
      ListenableFuture<S3File> result = Futures.transform(
        copyAndDelete,
	new Function<S3File, S3File>()
	{
	  public S3File apply(S3File deletedFile)
	  {
	    return new S3File(
	      _options.getDestinationBucket(), destKey);
	  }
	}
      );

      // try to clean up if a failure occurs.  just have to worry
      // about failure during a delete phase and remove the copied
      // file
      return Futures.withFallback(
        result,
	new FutureFallback<S3File>()
	{
	  public ListenableFuture<S3File> create(Throwable t)
	  {
            DeleteOptions deleteOpts = new DeleteOptionsBuilder()
              .setBucket(_options.getDestinationBucket())
              .setObjectKey(_options.getDestinationKey())
	      .setForceDelete(true)
	      .setIgnoreAbortInjection(true)
              .createDeleteOptions();
	    try
	    {
              _client.delete(deleteOpts).get();
	    }
	    catch(InterruptedException | ExecutionException ex)
	    {
	      return Futures.immediateFailedFuture(new Exception(
	        "Error cleaning up after rename failure:  " + ex.getMessage(), ex));
	    }
	    
            if (t instanceof UsageException)
              return Futures.immediateFailedFuture(t);

            return Futures.immediateFailedFuture(new Exception("Error " +
              "renaming " + srcUri + ":  " + t.getMessage(), t));
          }
        });
    }
  }


  private ListenableFuture<S3File> getCopyOp()
    throws IOException
  {
    final CopyOptions copyOpts = new CopyOptionsBuilder()
      .setSourceBucketName(_options.getSourceBucket())
      .setSourceKey(_options.getSourceKey())
      .setDestinationBucketName(_options.getDestinationBucket())
      .setDestinationKey(getDestKey())
      .setCannedAcl(_acl)
      .createCopyOptions();

    return _client.copy(copyOpts);
  }


  private void checkSourceExists()
  {
    String bucket = _options.getSourceBucket();
    String key = _options.getSourceKey();

    try
    {
      if(_client.exists(bucket, key).get() == null)
        throw new UsageException("Source object '" + getUri(bucket, key) + "' does not exist");
    }
    catch(InterruptedException | ExecutionException ex)
    {
      throw new UsageException(
        "Error checking object existence: " + ex.getMessage(), ex);
    }
  }

  
  private String getDestKey()
  {
    String key = _options.getDestinationKey();
    if(key.endsWith("/"))
    {
      // moving a file into a folder....
      String src = _options.getSourceKey();
      int idx = src.lastIndexOf("/");
      if(-1 != idx)
        key = key + src.substring(idx + 1);
    }
    return key;
  }

  
  private void checkDestExists()
  {
    String bucket = _options.getDestinationBucket();
    String key = getDestKey();
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
  
}
