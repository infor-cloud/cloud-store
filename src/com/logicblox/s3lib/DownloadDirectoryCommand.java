package com.logicblox.s3lib;


import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class DownloadDirectoryCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  private CloudStoreClient _client;
  private List<ListenableFuture<S3File>> _futures;
  private java.util.Set<File> _filesToCleanup;
  private List<File> _dirsToCleanup;
  private boolean _dryRun = false;
  

  public DownloadDirectoryCommand(
          ListeningExecutorService httpExecutor,
          ListeningScheduledExecutorService internalExecutor,
          CloudStoreClient client)
  {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
    _client = client;
    _futures = new ArrayList<ListenableFuture<S3File>>();
    _filesToCleanup = new java.util.HashSet<File>();
    _dirsToCleanup = new ArrayList<File>();
  }

  public ListenableFuture<List<S3File>> run(
    File destination,
    String bucket,
    String key,
    boolean recursive,
    boolean overwrite,
    boolean dryRun,
    OverallProgressListenerFactory progressFactory)
      throws ExecutionException, InterruptedException, IOException
  {
    _dryRun = dryRun;
    _futures.clear();
    _filesToCleanup.clear();
    _dirsToCleanup.clear();

    try
    {
      checkDestination(destination, overwrite);
    }
    catch(UsageException ex)
    {
      cleanup();
      throw ex;
    }

    ListenableFuture<List<S3File>> listObjs = querySourceFiles(bucket, key, recursive);
    ListenableFuture<List<S3File>> result = Futures.transform(
      listObjs,
      new AsyncFunction<List<S3File>, List<S3File>>()
      {
        public ListenableFuture<List<S3File>> apply(List<S3File> srcFiles)
          throws IOException
        {
          prepareFutures(srcFiles, destination, bucket, key, overwrite, progressFactory);
          if(srcFiles.isEmpty())
            throw new UsageException("No objects found for '" + getUri(bucket, key) + "'");

          if(dryRun)
            return Futures.immediateFuture(null);
          else
            return scheduleExecution();
        }
      });
    return result;
  }


  private ListenableFuture<List<S3File>> scheduleExecution()
  {
    // Don't see a way to have all peer futures in the list fail and clean up if any
    // one fails, even if explicitly cancelled.  This seems to be the only way
    // to clean up all the newly created files reliably.
    ListenableFuture<List<S3File>> futureList = Futures.allAsList(_futures);
    return Futures.withFallback(
      futureList,
      new FutureFallback<List<S3File>>()
      {
        public ListenableFuture<List<S3File>> create(Throwable t)
        {
           cleanup();
           return Futures.immediateFailedFuture(t);
        }
      });
  }


  private ListenableFuture<List<S3File>> querySourceFiles(
    String bucket, String key, boolean recursive)
  {
    // find all files that need to be downloaded
    ListOptionsBuilder lob = new ListOptionsBuilder()
        .setBucket(bucket)
        .setObjectKey(key)
        .setRecursive(recursive)
        .setIncludeVersions(false)
        .setExcludeDirs(false);
    return _client.listObjects(lob.createListOptions());
  }


  private void checkDestination(File dest, boolean overwrite)
  {
    // the destination must be a directory if it exists.  if doesn't exist, create it.
    // overwrite flag is only checked on a file by file basis, not for the destination
    // directory
    if(dest.exists())
    {
      if(!dest.isDirectory())
      {
        if(overwrite)
        {
          if(_dryRun)
            System.out.println("<DRYRUN> overwriting existing file '" + dest.getAbsolutePath()
              + "' with new directory");
          else
            dest.delete();
        }
        else
        {
          throw new UsageException("Existing destination '" + dest + "' must be a directory");
        }
      }
    }
    else
    {
      try
      {
        updateDirsToCleanup(Utils.mkdirs(dest, _dryRun));
      }
      catch(IOException ex)
      {
        throw new UsageException("Could not create directory '" + dest + "': "
          + ex.getMessage());
      }
    }
  }


  private void updateDirsToCleanup(List<File> newDirs)
  {
    if(_dryRun)
    {
      for(File f : newDirs)
      {
        if(!_dirsToCleanup.contains(f))
          System.out.println("<DRYRUN> creating missing directory '"
            + f.getAbsolutePath() + "'");
      }
    }
    _dirsToCleanup.addAll(newDirs);
  }
  
  
  private void prepareFutures(
    List<S3File> potentialFiles, File dest, String bucket, String srcKey, boolean overwrite,
    OverallProgressListenerFactory progressFactory)
      throws IOException
  {
    File destAbs = dest.getAbsoluteFile();
    for(S3File src : potentialFiles)
    {
      String relFile = src.getKey().substring(srcKey.length());
      File outputFile = new File(destAbs, relFile);
      File outputPath = new File(outputFile.getParent());

      if(!outputPath.exists())
      {
        try
        {
          updateDirsToCleanup(Utils.mkdirs(outputPath, _dryRun));
        }
        catch(IOException ex)
        {
          throw new UsageException("Could not create directory '" + outputPath + "': "
            + ex.getMessage());
        }
      }

      if(!src.getKey().endsWith("/"))
      {
        if(outputFile.exists())
        {
          if(overwrite)
          {
            if(_dryRun)
            {
              System.out.println("<DRYRUN> overwrite existing file '" + outputFile.getAbsolutePath() + "'");
            }
            else
            {
              if(!outputFile.delete())
                throw new UsageException("Could not overwrite existing file '" + outputFile
                  + "'");
            }
          }
          else
          {
            throw new UsageException(
              "File '" + outputFile + "' already exists. Please delete or use --overwrite");
          }
        }
        if(_dryRun)
        {
          System.out.println("<DRYRUN> downloading '" + getUri(bucket, src.getKey())
            + "' to '" + outputFile.getAbsolutePath() + "'");
        }
        else
        {
          _filesToCleanup.add(outputFile);

          DownloadOptions options = new DownloadOptionsBuilder()
            .setFile(outputFile)
            .setBucket(bucket)
            .setObjectKey(src.getKey())
            .setOverallProgressListenerFactory(progressFactory)
            .createDownloadOptions();

          _futures.add(_client.download(options));
        }
      }
    }
  }
  

  private void cleanup()
  {
    // cancel any futures that may still be trying to run
    for(ListenableFuture<S3File> f : _futures)
      f.cancel(true);
    _futures.clear();

    // delete any files we created
    for(File f : _filesToCleanup)
    {
      if(f.exists())
        f.delete();
    }
    _filesToCleanup.clear();

    // delete any directories we created
    //   - assume these were created topdown, so we can unravel them bottom up
    for(int i = _dirsToCleanup.size() - 1; i >= 0; --i)
    {
      _dirsToCleanup.get(i).delete();
    }
    _dirsToCleanup.clear();
  }

}
