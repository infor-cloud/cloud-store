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

import com.google.api.client.googleapis.media.MediaHttpDownloaderProgressListener;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

class GCSDownload
  implements Download
{
  private DownloadOptions _options;
  private Storage _client;
  private ListeningExecutorService _apiExecutor;
  private ListeningExecutorService _internalExecutor;
  private StorageObject _storageObject;
  private ConcurrentMap<Integer, Crc32cInputStream> _partInputStreams
    = new ConcurrentSkipListMap<>();
  private ConcurrentMap<Integer, Long> _partLengths = new ConcurrentSkipListMap<>();

  public GCSDownload(
    DownloadOptions options, Storage client, ListeningExecutorService apiExecutor,
    ListeningExecutorService internalExecutor, StorageObject storageObject)
  {
    _options = options;
    _client = client;
    _apiExecutor = apiExecutor;
    _internalExecutor = internalExecutor;
    _storageObject = storageObject;
  }

  @Override
  public ListenableFuture<InputStream> downloadPart(
    int partNumber, long start, long end, OverallProgressListener progressListener)
  {
    return _apiExecutor.submit(new DownloadCallable(partNumber, start, end, progressListener));
  }

  public ListenableFuture<Download> completeDownload()
  {
    return _internalExecutor.submit(new GCSDownload.CompleteCallable());
  }

  @Override
  public Map<String, String> getMetadata()
  {
    return _storageObject.getMetadata();
  }

  @Override
  public long getLength()
  {
    return _storageObject.getSize().longValue();
  }

  @Override
  public String getETag()
  {
    return _storageObject.getEtag();
  }

  public String getCrc32c()
  {
    return _storageObject.getCrc32c();
  }

  @Override
  public String getObjectKey()
  {
    return _options.getObjectKey();
  }

  @Override
  public String getBucketName()
  {
    return _options.getBucketName();
  }

  private class DownloadCallable
    implements Callable<InputStream>
  {
    private int _partNumber;
    private long _start;
    private long _end;
    private OverallProgressListener _progressListener;

    public DownloadCallable(
      int partNumber, long start, long end, OverallProgressListener progressListener)
    {
      _partNumber = partNumber;
      _start = start;
      _end = end;
      _progressListener = progressListener;
    }

    public InputStream call()
      throws Exception
    {
      Storage.Objects.Get getObject = _client.objects().get(getBucketName(), getObjectKey());
      getObject.getMediaHttpDownloader().setContentRange(_start, _end);

      if(_progressListener != null)
      {
        PartProgressEvent ppe = new PartProgressEvent(Integer.toString(_partNumber));
        MediaHttpDownloaderProgressListener gcspl = new GCSDownloaderProgressListener(
          _progressListener, ppe);
        getObject.getMediaHttpDownloader().setProgressListener(gcspl);
      }
      // take the copy of the stream and re-write it to an InputStream
      PipedInputStream inp = new PipedInputStream();
      final PipedOutputStream outp = new PipedOutputStream(inp);
      _apiExecutor.submit(new Callable<Void>()
      {
        public Void call()
          throws IOException
        {
          // PipedOutputStream writes should take place in a different thread
          // than PipedInputStream reads. It's safe to do the writes here
          // (apiExecutor thread) because reads happen in an internalExecutor
          // thread.
          getObject.executeMediaAndDownloadTo(outp);
          outp.close();
          return null;
        }
      });

      Crc32cInputStream in = new Crc32cInputStream(inp);
      _partInputStreams.put(_partNumber, in);
      _partLengths.put(_partNumber, _end - _start + 1);

      return in;
    }
  }

  private class CompleteCallable
    implements Callable<Download>
  {
    public Download call()
      throws Exception
    {
      String fn = "'gs://" + getBucketName() + "/" + getObjectKey() + "'";

      String remoteCrc32c = getCrc32c();
      if(null == remoteCrc32c)
      {
        System.err.println(
          "Warning: Skipped checksum validation for " + fn + ".  No CRC32C attached to object.");
        return GCSDownload.this;
      }

      long localCrc32cL = _partInputStreams.get(0).getValue();
      for(int partNumber : Iterables.skip(_partInputStreams.keySet(), 1))
      {
        localCrc32cL = Crc32c.combine(localCrc32cL, _partInputStreams.get(partNumber).getValue(),
          _partLengths.get(partNumber));
      }

      String localCrc32c = new String(
        Base64.encodeBase64(Crc32c.longToBytesBigEndian(localCrc32cL)));
      if(remoteCrc32c.equals(localCrc32c))
      {
        return GCSDownload.this;
      }
      else
      {
        throw new BadHashException(
          "Failed checksum validation for " + getBucketName() + "/" + getObjectKey() + ". " +
            "Calculated CRC32C: " + localCrc32c + ", Expected CRC32C: " + remoteCrc32c);
      }
    }
  }
}