package com.logicblox.s3lib;


import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.BufferedInputStream;
import java.io.RandomAccessFile;
import java.io.ObjectInputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;

import java.security.Key;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.InvalidAlgorithmParameterException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import com.amazonaws.services.s3.AmazonS3Client;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.base.Functions;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

public class DownloadCommand extends Command
{
  private ListeningExecutorService _downloadExecutor;
  private ListeningExecutorService _executor;
  private KeyProvider _encKeyProvider;

  public DownloadCommand(
    ListeningExecutorService downloadExecutor,
    ListeningExecutorService internalExecutor,
    File file,
    KeyProvider encKeyProvider)
  throws IOException
  {
    _downloadExecutor = downloadExecutor;
    _executor = internalExecutor;
    _encKeyProvider = encKeyProvider;

    this.file = file;
    createNewFile();
  }

  private void createNewFile() throws IOException
  {
    file = file.getAbsoluteFile();
    File dir = file.getParentFile();
    if(!dir.exists())
    {
      if(!dir.mkdirs())
        throw new IOException("Could not create directory '" + dir + "'");
    }

    if(file.exists() && !file.delete())
      throw new IOException("Could not delete existing file '" + file + "'");

    if(!file.createNewFile())
      throw new IOException("File '" + file + "' already exists");
  }

  public ListenableFuture<Object> run(final String bucket, final String key)
  {
    ListenableFuture<Download> download = startDownload(bucket, key, 0);
    download = Futures.transform(download, startPartsAsyncFunction());
    return Futures.transform(download, Functions.constant(null));
  }
  
  /**
   * Step 1: Start download and fetch metadata.
   */
  private ListenableFuture<Download> startDownload(final String bucket, final String key, final int retryCount)
  {
    DownloadFactory factory = new AmazonDownloadFactory(getAmazonS3Client(), _downloadExecutor);

    ListenableFuture<Download> startDownloadFuture = factory.startDownload(bucket, key);
    
    FutureFallback<Download> startAgain = new FutureFallback<Download>()
      {
        public ListenableFuture<Download> create(Throwable thrown) throws Exception
        {
          rethrowOnMaxRetry(thrown, retryCount);

            System.err.println("Error starting download: " + thrown.getMessage());
          return DownloadCommand.this.startDownload(bucket, key, retryCount + 1);
        }
      };
    return withFallback(startDownloadFuture, startAgain);
  }

  /**
   * Step 2: Start downloading parts
   */
  private AsyncFunction<Download, Download> startPartsAsyncFunction()
  {
    return new AsyncFunction<Download, Download>()
    {
      public ListenableFuture<Download> apply(Download download) throws Exception
      {
        return startParts(download);
      }
    };
  }

  private ListenableFuture<Download> startParts(Download download)
  throws IOException, UsageException
  {
    Map<String,String> meta = download.getMeta();

    if (meta.containsKey("s3tool-version")) {
      String objectVersion = meta.get("s3tool-version");

      if (!String.valueOf(Version.CURRENT).equals(objectVersion))
        throw new UsageException("file uploaded with unsupported version: " + objectVersion + ", should be " + Version.CURRENT);

      if (meta.containsKey("s3tool-key-name")) {
        String keyName = meta.get("s3tool-key-name");
        Key privKey;
        try {
          privKey = _encKeyProvider.getPrivateKey(keyName);
        } catch (NoSuchKeyException e) {
          throw new UsageException("private key '" + keyName + "' is not available to decrypt");
        }

        Cipher cipher;
        try
        {
          cipher = Cipher.getInstance("RSA");
        } catch (NoSuchAlgorithmException e) {
          throw new RuntimeException(e);
        } catch (NoSuchPaddingException e) {
          throw new RuntimeException(e);
        }
        try
        {
          cipher.init(Cipher.DECRYPT_MODE, privKey);
        } catch (InvalidKeyException e) {
          throw new RuntimeException(e);
        }
        byte[] encKeyBytes;
        try {
          encKeyBytes = cipher.doFinal(DatatypeConverter.parseBase64Binary(meta.get("s3tool-symmetric-key")));
        } catch (IllegalBlockSizeException e) {
          throw new RuntimeException(e);
        } catch (BadPaddingException e) {
          throw new RuntimeException(e);
        }
        encKey = new SecretKeySpec(encKeyBytes, "AES");
      }

      setChunkSize(Long.valueOf(meta.get("s3tool-chunk-size")));
      fileLength = Long.valueOf(meta.get("s3tool-file-length"));
    } else {
      fileLength = download.getLength();
      if (chunkSize == 0) {
        setChunkSize(fileLength);
      }
    }

    List<ListenableFuture<Integer>> parts = new ArrayList<ListenableFuture<Integer>>();
    for (long position = 0; position < fileLength; position += chunkSize)
    {
      parts.add(startPartDownload(download, position, 0));
    }

    return Futures.transform(Futures.allAsList(parts), Functions.constant(download));
  }

  private ListenableFuture<Integer> startPartDownload(final Download download, final long position, final int retryCount)
  {
    final int partNumber = (int) (position / chunkSize);
    long start;
    long partSize;

    if (encKey != null) {
      long blockSize;
      try {
        blockSize = Cipher.getInstance("AES/CBC/PKCS5Padding").getBlockSize();
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      } catch (NoSuchPaddingException e) {
        throw new RuntimeException(e);
      }

      long postCryptSize = Math.min(fileLength - position, chunkSize);
      start = partNumber * blockSize * (chunkSize/blockSize + 2);
      partSize = blockSize * (postCryptSize/blockSize + 2);
    } else {
      start = position;
      partSize = Math.min(fileLength - position, chunkSize);
    }

    System.err.println("Downloading part " + partNumber);    
    ListenableFuture<InputStream> getPartFuture = download.getPart(start, start + partSize - 1);

    FutureFallback<Integer> getPartAgain = new FutureFallback<Integer>()
      {
        public ListenableFuture<Integer> create(Throwable thrown) throws Exception
        {
          System.err.println("Error downloading part " + partNumber + ": " + thrown.getMessage());
          rethrowOnMaxRetry(thrown, retryCount);
          return DownloadCommand.this.startPartDownload(download, position, retryCount + 1);
        }
      };

    AsyncFunction<InputStream, Integer> readDownloadFunction = new AsyncFunction<InputStream, Integer>()
      {
        public ListenableFuture<Integer> apply(InputStream stream) throws Exception
        {
          readDownload(download, stream, position, partNumber, retryCount);
          return Futures.immediateFuture(partNumber);
        }
      };

    return withFallback(
      Futures.transform(getPartFuture, readDownloadFunction),
      getPartAgain);
  }
  
  private void readDownload(Download download, InputStream stream, long position, int partNumber, int retryCount) throws Exception
  {
    RandomAccessFile out = new RandomAccessFile(file, "rw");
    out.seek(position);

    Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");

    InputStream in;
    if (encKey != null) {
      try {
        in = new CipherWithInlineIVInputStream(stream, cipher, Cipher.DECRYPT_MODE, encKey);
      } catch (IOException e) {
        System.err.println("Error initiating cipher stream for part " + partNumber + ": " + e.getMessage());
        startPartDownload(download, position, retryCount + 1);
        return;
      } catch (InvalidKeyException e) {
        throw new RuntimeException(e);
      } catch (InvalidAlgorithmParameterException e) {
        System.err.println("Error downloading part " + partNumber + ": " + e.getMessage());
        startPartDownload(download, position, retryCount + 1);
        return;
      }
    } else {
      in = stream;
    }
    int postCryptSize = (int) Math.min(fileLength - position, chunkSize);
    int offset = 0;
    byte[] buf = new byte[postCryptSize];
    while (offset < postCryptSize) {
      int result;
      try {
        result = in.read(buf, offset, postCryptSize - offset);
      } catch (IOException e) {
        System.err.println("Error reading part " + partNumber + ": " + e.getMessage());
        try {
          out.close();
          stream.close();
        } catch (IOException ignored) {
        }
        startPartDownload(download, position, retryCount + 1);
        return;
      }
      if (result == -1) {
        System.err.println("Error downloading part " + partNumber + ": unexpected EOF");
        try {
          out.close();
          stream.close();
        } catch (IOException e) {
        }
        startPartDownload(download, position, retryCount + 1);
        return;
      }
      try {
        out.write(buf, offset, result);
      } catch (IOException e) {
        System.err.println("Error writing part " + partNumber + ": " + e.getMessage());
        try {
          out.close();
          stream.close();
        } catch (IOException ignored) {
        }
        startPartDownload(download, position, retryCount + 1);
        return;
      }
      offset += result;
    }

    try {
      out.close();
      stream.close();
    } catch (IOException e) {
    }

    System.err.println("Finished part " + partNumber);
  }
}
