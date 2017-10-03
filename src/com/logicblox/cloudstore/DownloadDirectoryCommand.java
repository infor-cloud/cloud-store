/*
  Copyright 2017, Infor Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.logicblox.cloudstore;


import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

class DownloadDirectoryCommand
  extends Command
{
  private DownloadOptions _options;
  private File _destination;
  private List<ListenableFuture<StoreFile>> _futures;
  private java.util.Set<File> _filesToCleanup;
  private List<File> _dirsToCleanup;
  private boolean _dryRun = false;


  public DownloadDirectoryCommand(DownloadOptions options)
  {
    super(options);
    _options = options;
    _destination = _options.getFile();
    _futures = new ArrayList<>();
    _filesToCleanup = new java.util.HashSet<>();
    _dirsToCleanup = new ArrayList<>();
    _dryRun = _options.isDryRun();

  }

  public ListenableFuture<List<StoreFile>> run()
    throws ExecutionException, InterruptedException, IOException
  {
    _futures.clear();
    _filesToCleanup.clear();
    _dirsToCleanup.clear();

    try
    {
      checkDestination();
    }
    catch(UsageException ex)
    {
      cleanup();
      throw ex;
    }

    ListenableFuture<List<StoreFile>> listObjs = querySourceFiles();
    ListenableFuture<List<StoreFile>> result = Futures.transform(listObjs,
      new AsyncFunction<List<StoreFile>, List<StoreFile>>()
      {
        public ListenableFuture<List<StoreFile>> apply(List<StoreFile> srcFiles)
          throws IOException
        {
          prepareFutures(srcFiles);
          if(srcFiles.isEmpty())
          {
            throw new UsageException(
              "No objects found for '" + getUri(_options.getBucketName(), _options.getObjectKey()) +
                "'");
          }

          if(_options.isDryRun())
          {
            return Futures.immediateFuture(null);
          }
          else
          {
            return scheduleExecution();
          }
        }
      });
    return result;
  }


  private ListenableFuture<List<StoreFile>> scheduleExecution()
  {
    // Don't see a way to have all peer futures in the list fail and clean up if any
    // one fails, even if explicitly cancelled.  This seems to be the only way
    // to clean up all the newly created files reliably.
    ListenableFuture<List<StoreFile>> futureList = Futures.allAsList(_futures);
    return Futures.withFallback(futureList, new FutureFallback<List<StoreFile>>()
    {
      public ListenableFuture<List<StoreFile>> create(Throwable t)
      {
        cleanup();
        return Futures.immediateFailedFuture(t);
      }
    });
  }


  private ListenableFuture<List<StoreFile>> querySourceFiles()
  {
    // find all files that need to be downloaded
    ListOptionsBuilder lob = _client.getOptionsBuilderFactory()
      .newListOptionsBuilder()
      .setBucketName(_options.getBucketName())
      .setObjectKey(_options.getObjectKey())
      .setRecursive(_options.isRecursive())
      .setIncludeVersions(false)
      .setExcludeDirs(false);
    return _client.listObjects(lob.createOptions());
  }


  private void checkDestination()
  {
    // the destination must be a directory if it exists.  if doesn't exist, create it.
    // overwrite flag is only checked on a file by file basis, not for the destination
    // directory
    if(_destination.exists())
    {
      if(!_destination.isDirectory())
      {
        if(_options.doesOverwrite())
        {
          if(_dryRun)
          {
            System.out.println(
              "<DRYRUN> overwriting existing file '" + _destination.getAbsolutePath() +
                "' with new directory");
          }
          else
          {
            _destination.delete();
          }
        }
        else
        {
          throw new UsageException(
            "Existing destination '" + _destination + "' must be a directory");
        }
      }
    }
    else
    {
      try
      {
        updateDirsToCleanup(Utils.mkdirs(_destination, _dryRun));
      }
      catch(IOException ex)
      {
        throw new UsageException(
          "Could not create directory '" + _destination + "': " + ex.getMessage());
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
        {
          System.out.println("<DRYRUN> creating missing directory '" + f.getAbsolutePath() + "'");
        }
      }
    }
    _dirsToCleanup.addAll(newDirs);
  }


  private void prepareFutures(List<StoreFile> potentialFiles)
    throws IOException
  {
    File destAbs = _destination.getAbsoluteFile();
    for(StoreFile src : potentialFiles)
    {
      String relFile = src.getObjectKey().substring(_options.getObjectKey().length());
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
          throw new UsageException(
            "Could not create directory '" + outputPath + "': " + ex.getMessage());
        }
      }

      if(!src.getObjectKey().endsWith("/"))
      {
        if(outputFile.exists())
        {
          if(_options.doesOverwrite())
          {
            if(_dryRun)
            {
              System.out.println(
                "<DRYRUN> overwrite existing file '" + outputFile.getAbsolutePath() + "'");
            }
            else
            {
              if(!outputFile.delete())
              {
                throw new UsageException("Could not overwrite existing file '" + outputFile + "'");
              }
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
          System.out.println(
            "<DRYRUN> downloading '" + getUri(_options.getBucketName(), src.getObjectKey()) + "' to '" +
              outputFile.getAbsolutePath() + "'");
        }
        else
        {
          _filesToCleanup.add(outputFile);

          DownloadOptions options = _client.getOptionsBuilderFactory()
            .newDownloadOptionsBuilder()
            .setFile(outputFile)
            .setBucketName(_options.getBucketName())
            .setObjectKey(src.getObjectKey())
            .setOverallProgressListenerFactory(
              _options.getOverallProgressListenerFactory().orElse(null))
            .createOptions();

          _futures.add(_client.download(options));
        }
      }
    }
  }


  private void cleanup()
  {
    // cancel any futures that may still be trying to run
    for(ListenableFuture<StoreFile> f : _futures)
      f.cancel(true);
    _futures.clear();

    // delete any files we created
    for(File f : _filesToCleanup)
    {
      if(f.exists())
      {
        f.delete();
      }
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
