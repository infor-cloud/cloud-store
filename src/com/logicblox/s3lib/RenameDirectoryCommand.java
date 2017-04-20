package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
// FIXME - This could be a little problemmatic since we don't have an atomic rename
// function in the store interfaces.  The way this is currently written, we do copy/delete
// in pairs for each object to be renamed.  If some failure happens somewhere, we could
// end up with objects that have been copied and deleted, copied but not deleted, or not
// copied at all.  It could be better here to do all the copies first, followed by all the
// deletes, with fallbacks so that if any one copy failed we would delete any copied files
// that succeeded.  We could still end up in an inconsistent state if we made it to the
// delete phase and some deletes succeeded and others failed, leaving original files laying
// around.  Backing out problems in the phase would put us back into more copy/delete pairs....

    checkDestExists();
    List<S3File> toRename = queryFiles();
    List<ListenableFuture<S3File>> futures = prepareFutures(toRename);

    if(_options.isDryRun())
    {
      List<S3File> dummy = new ArrayList<S3File>();
      return Futures.immediateFuture(dummy);
    }
    else
    {
      return Futures.allAsList(futures);
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


  private List<ListenableFuture<S3File>> prepareFutures(List<S3File> toRename)
    throws IOException
  {
    String destDir = _options.getDestinationKey();
    if(!destDir.endsWith("/"))
      destDir = destDir + "/";
    int srcIdx = _options.getSourceKey().length();
    List<ListenableFuture<S3File>> futures = new ArrayList<ListenableFuture<S3File>>();
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
        RenameOptions opts = new RenameOptionsBuilder()
          .setSourceBucket(src.getBucketName())
          .setSourceKey(src.getKey())
	  .setDestinationBucket(_options.getDestinationBucket())
	  .setDestinationKey(destKey)
	  .setRecursive(false)
          .createRenameOptions();
        futures.add(_client.rename(opts));
      }
    }
    return futures;
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
