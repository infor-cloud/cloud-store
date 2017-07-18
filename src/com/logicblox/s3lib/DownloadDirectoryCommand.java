package com.logicblox.s3lib;


import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class DownloadDirectoryCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  private CloudStoreClient _client;

  public DownloadDirectoryCommand(
          ListeningExecutorService httpExecutor,
          ListeningScheduledExecutorService internalExecutor,
          CloudStoreClient client)
  {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
    _client = client;
  }

  public ListenableFuture<List<S3File>> run(
    final File file,
    final String bucket,
    final String key,
    final boolean recursive,
    final boolean overwrite,
    final OverallProgressListenerFactory progressListenerFactory)
  throws ExecutionException, InterruptedException, IOException
  {
    ListOptionsBuilder lob = new ListOptionsBuilder()
        .setBucket(bucket)
        .setObjectKey(key)
        .setRecursive(recursive)
        .setIncludeVersions(false)
        .setExcludeDirs(false);
    List<S3File> lst = _client.listObjects(lob.createListOptions()).get();

    final List<File> dirsToCleanup = new ArrayList<File>();
    if (lst.size() > 1)
    {
      if(!file.exists())
      {
        try
        {
          dirsToCleanup.addAll(Utils.mkdirs(file));
        }
        catch(IOException ex)
        {
          throw new UsageException("Could not create directory '" + file + "': " + ex.getMessage());
        }
      }
    }

    final List<ListenableFuture<S3File>> files = new ArrayList<ListenableFuture<S3File>>();
    final java.util.Set<File> filesToCleanup = new java.util.HashSet<File>();

    for (S3File obj : lst)
    {
      String relFile = obj.getKey().substring(key.length());
      File outputFile = new File(file.getAbsoluteFile(), relFile);
      File outputPath = new File(outputFile.getParent());

      if(!outputPath.exists())
      {
        try
        {
          dirsToCleanup.addAll(Utils.mkdirs(outputPath));
        }
        catch(IOException ex)
        {
          throw new UsageException("Could not create directory '" + file + "': " + ex.getMessage());
        }
      }

      if (!obj.getKey().endsWith("/"))
      {
        if(outputFile.exists())
        {
          if(overwrite)
          {
            if(!outputFile.delete())
              throw new UsageException("Could not overwrite existing file '" + file + "'");
          }
          else
            throw new UsageException(
              "File '" + outputFile + "' already exists. Please delete or use --overwrite");
        }
        filesToCleanup.add(outputFile);

        DownloadOptions options = new DownloadOptionsBuilder()
            .setFile(outputFile)
            .setBucket(bucket)
            .setObjectKey(obj.getKey())
            .setOverallProgressListenerFactory(progressListenerFactory)
            .createDownloadOptions();

        ListenableFuture<S3File> result = _client.download(options);
        files.add(result);
      }
    }

    // don't see a way to have all peer futures in the list fail and clean up if any
    // one fails, even if i explicitly cancel them.  this seems to be the only way
    // to clean up all the newly created files and directories reliably.
    ListenableFuture<List<S3File>> futureList = Futures.allAsList(files);
    return Futures.withFallback(
      futureList,
      new FutureFallback<List<S3File>>()
      {
        public ListenableFuture<List<S3File>> create(Throwable t)
        {
           // cancel any futures that may still be trying to run
           for(ListenableFuture<S3File> f : files)
             f.cancel(true);

           // delete any files we created
           for(File f : filesToCleanup)
           {
             if(f.exists())
               f.delete();
           }

           // delete any directories we created
           for(int i = dirsToCleanup.size() - 1; i >= 0; --i)
           {
             dirsToCleanup.get(i).delete();
           }
           
           return Futures.immediateFailedFuture(t);
        }
      });
  }

}
