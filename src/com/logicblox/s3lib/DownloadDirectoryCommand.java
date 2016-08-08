package com.logicblox.s3lib;


import com.google.common.util.concurrent.Futures;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.amazonaws.services.s3.model.ObjectMetadata;
import java.util.concurrent.Callable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

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

    if (lst.size() > 1)
      if(!file.exists())
        if(!file.mkdirs())
          throw new UsageException("Could not create directory '" + file + "'");

    List<ListenableFuture<S3File>> files = new ArrayList<ListenableFuture<S3File>>();
    for (S3File obj : lst)
    {
      String relFile = obj.getKey().substring(key.length());
      File outputFile = new File(file.getAbsoluteFile(), relFile);
      File outputPath = new File(outputFile.getParent());
      if(!outputPath.exists())
        if(!outputPath.mkdirs())
          throw new UsageException("Could not create directory '"+file+"'");
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
        DownloadOptions options = new DownloadOptionsBuilder()
            .setFile(outputFile)
            .setBucket(bucket)
            .setObjectKey(obj.getKey())
            .setOverallProgressListenerFactory(progressListenerFactory)
            .createDownloadOptions();
        ListenableFuture<S3File> result = _client.download(options);
        files.add(result);
      }else if(outputFile.mkdir()){
        // create empty folder
        final File finaloutputFile  = outputFile;
        final String  finalbucket = bucket;
        final String finalKey = obj.getKey();
      ListenableFuture<S3File> future =
          executeWithRetry(_executor, new Callable<ListenableFuture<S3File>>() {
            public ListenableFuture<S3File> call() {
              return getMetadata( finaloutputFile,finalbucket ,finalKey );
              
            }
          });
        files.add(future);
      }
    }
    return Futures.allAsList(files);
  }
    
    private ListenableFuture<S3File> getMetadata(final File outputFile, final String bucket, final  String key ) {
      return _httpExecutor.submit(new Callable<S3File>() {

        public S3File call() {
          ObjectMetadata meta = getAmazonS3Client().getObjectMetadata(bucket, key);
          outputFile.setLastModified(meta.getLastModified().getTime());
          S3File f = new S3File();
          f.setLocalFile(outputFile);
          f.setETag(meta.getETag());
          f.setBucketName(bucket);
          f.setKey(key);
          return f;
        }
      });
    }

}
