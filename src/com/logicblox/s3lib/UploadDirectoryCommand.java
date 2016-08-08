package com.logicblox.s3lib;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class UploadDirectoryCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  private CloudStoreClient _client;

  public UploadDirectoryCommand(
          ListeningExecutorService httpExecutor,
          ListeningScheduledExecutorService internalExecutor,
          CloudStoreClient client)
  {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
    _client = client;
  }

  public ListenableFuture<List<S3File>> run(final File dir,
                                            final String bucket,
                                            final String object,
                                            final String encKey,
                                            final String acl,
                                            OverallProgressListenerFactory
                                                progressListenerFactory)
  throws ExecutionException, InterruptedException, IOException
  {
    final IOFileFilter noSymlinks = new IOFileFilter()
    {
      @Override
      public boolean accept(File file, String s)
      {
        return isSymlink(file);
      }

      private boolean isSymlink(File file)
      {
        try
        {
          boolean res = ! FileUtils.isSymlink(file);
          return res;
        }
        catch (FileNotFoundException e)
        {
          return false;
        }
        catch (IOException e)
        {
          return false;
        }
      }

      @Override
      public boolean accept(File file)
      {
        return isSymlink(file);
      }
    };

    Collection<File> found = FileUtils.listFiles(dir, noSymlinks, noSymlinks);
    
    List<ListenableFuture<S3File>> files = new ArrayList<ListenableFuture<S3File>>();
    if (files.size() == 0) {
      final File finalFolder = dir;
      final String finalbucket = bucket;
      final String finalKey = object;
      ListenableFuture<S3File> future =
          executeWithRetry(_executor, new Callable<ListenableFuture<S3File>>() {
            
            public ListenableFuture<S3File> call() {
              return createFolder(finalFolder, finalbucket, finalKey);
              
            }
          });
      files.add(future);
    }
    for (File file : found) {
      String relPath = file.getPath().substring(dir.getPath().length() + 1);
      String key = Paths.get(object, relPath).toString();
      
      UploadOptions options = new UploadOptionsBuilder()
          .setFile(file)
          .setBucket(bucket)
          .setObjectKey(key)
          .setEncKey(encKey)
          .setAcl(acl)
          .setOverallProgressListenerFactory(progressListenerFactory)
          .createUploadOptions();
      
      files.add(_client.upload(options));
    }
    
    return Futures.allAsList(files);
  }
  
  private ListenableFuture<S3File> createFolder(
      final File outputFile,
      final String bucket,
      final String key) {
    return _httpExecutor.submit(new Callable<S3File>() {

      public S3File call() {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setLastModified(new Date(outputFile.lastModified()));
        metadata.setContentLength(0);
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
        PutObjectRequest putObjectRequest =
            new PutObjectRequest(bucket, key, emptyContent, metadata);
        getAmazonS3Client().putObject(putObjectRequest);
        S3File f = new S3File();
        f.setLocalFile(outputFile);
        f.setBucketName(bucket);
        f.setKey(key);
        return f;
      }
    });
  }
  
}
