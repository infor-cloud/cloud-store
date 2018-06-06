/*
  Copyright 2018, Infor Inc.

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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

class UploadRecursivelyCommand
  extends Command
{
  private UploadOptions _options;

  public UploadRecursivelyCommand(UploadOptions options)
  {
    super(options);
    _options = options;
  }

  public ListenableFuture<List<StoreFile>> run()
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
          boolean res = !FileUtils.isSymlink(file);
          return res;
        }
        catch(IOException e)
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

    List<ListenableFuture<StoreFile>> files = new ArrayList<ListenableFuture<StoreFile>>();
    if(_options.getFile().isDirectory())
    {
      Collection<File> found = FileUtils.listFiles(_options.getFile(), noSymlinks, noSymlinks);

      for(File file : found)
      {
        String relPath = file.getPath().substring(_options.getFile().getPath().length() + 1);
        String key = Paths.get(_options.getObjectKey(), relPath).toString();
        uploadFile(files, file, key);
      }
    }
    else
    {
      String key;

      if(!_options.getObjectKey().endsWith("/"))
      {
        throw new UsageException("A directory-like destination URI is expected: '" +
          getUri(_options.getBucketName(), _options.getObjectKey()));
      }

      key = Paths.get(_options.getObjectKey(), _options.getFile().getName()).toString();

      uploadFile(files, _options.getFile(), key);
    }

    if(_options.isDryRun())
    {
      return Futures.immediateFuture(null);
    }
    else
    {
      return Futures.allAsList(files);
    }
  }

  private void uploadFile(List<ListenableFuture<StoreFile>> files, File file, String key)
    throws IOException
  {
    UploadOptions options = _client.getOptionsBuilderFactory()
      .newUploadOptionsBuilder()
      .setFile(file)
      .setBucketName(_options.getBucketName())
      .setObjectKey(key)
      .setChunkSize(_options.getChunkSize())
      .setEncKey(_options.getEncKey().orElse(null))
      .setCannedAcl(_options.getCannedAcl())
      .setOverallProgressListenerFactory(_options.getOverallProgressListenerFactory().orElse(null))
      .createOptions();

    if(_options.isDryRun())
    {
      System.out.println("<DRYRUN> uploading '" + file.getAbsolutePath() + "' to '" +
        getUri(_options.getBucketName(), key) + "'");
    }
    else
    {
      files.add(_client.upload(options));
    }
  }

}
