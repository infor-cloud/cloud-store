package com.logicblox.s3lib;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class UploadDirectoryCommand extends Command
{
  private UploadOptions _options;
  private CloudStoreClient _client;
  private boolean _dryRun;

  public UploadDirectoryCommand(UploadOptions options)
  {
    _options = options;
    _client = _options.getCloudStoreClient();
  }

  public ListenableFuture<List<S3File>> run()
  throws ExecutionException, InterruptedException, IOException
  {
    _dryRun = _options.isDryRun();
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

    Collection<File> found = FileUtils.listFiles(_options.getFile(), noSymlinks, noSymlinks);

    List<ListenableFuture<S3File>> files = new ArrayList<ListenableFuture<S3File>>();
    for (File file : found)
    {
      String relPath = file.getPath().substring(_options.getFile().getPath().length()+1);
      String key = Paths.get(_options.getObjectKey(), relPath).toString();

      UploadOptions options = new UploadOptionsBuilder()
          .setCloudStoreClient(_options.getCloudStoreClient())
          .setFile(file)
          .setBucketName(_options.getBucketName())
          .setObjectKey(key)
          .setChunkSize(_options.getChunkSize())
          .setEncKey(_options.getEncKey().orElse(null))
          .setCannedACL(_options.getCannedACL().orElse(null))
          .setOverallProgressListenerFactory(_options
            .getOverallProgressListenerFactory().orElse(null))
          .createUploadOptions();

      if(_dryRun)
      {
        System.out.println("<DRYRUN> uploading '" + file.getAbsolutePath()
                           + "' to '" + getUri(_options.getBucketName(), key) + "'");
      }
      else
      {
        files.add(_client.upload(options));
      }
    }

    if(_dryRun)
    {
      return Futures.immediateFuture(null);
    }
    else
    {
      return Futures.allAsList(files);
    }
  }

}
