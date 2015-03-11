package com.logicblox.s3lib;

import com.amazonaws.event.ProgressListener;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
                                            S3ProgressListenerFactory
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
    for (File file : found)
    {
      String relPath = file.getPath().substring(dir.getPath().length()+1);
      String key = object + "/" + relPath;

      UploadOptions options = new UploadOptionsBuilder()
          .setFile(file)
          .setBucket(bucket)
          .setObjectKey(key)
          .setEncKey(encKey)
          .setAcl(acl)
          .setS3ProgressListenerFactory(progressListenerFactory)
          .createUploadOptions();

      files.add(_client.upload(options));
    }

    return Futures.allAsList(files);
  }

}
