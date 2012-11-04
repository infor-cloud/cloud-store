package com.logicblox.s3lib;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.BufferedInputStream;
import java.io.RandomAccessFile;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.security.Key;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.InvalidAlgorithmParameterException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.Cipher;

import com.amazonaws.services.s3.AmazonS3Client;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;


  public class DownloadCommand extends Command {
    private Download download;

    public DownloadCommand(File file) {
      this.file = file;
      try {
        file.createNewFile();
      } catch (IOException e) {
        System.err.println("Error creating download file: " + e.getMessage());
      }
      completedParts = new HashSet<Integer>();
    }

    public void run(final String bucket, final String key, final int maxConcurrentConnections, final File encKeyFile) {
      ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(maxConcurrentConnections));

      DownloadFactory factory = new AmazonDownloadFactory(new AmazonS3Client(), executor);

      ListenableFuture<Download> startDownloadFuture = factory.startDownload(bucket, key);
      final DownloadCommand that = this;
      System.out.println("Getting file metadata");
      FutureCallback<Download> startDownloadCallback = new FutureCallback<Download>() {
        public void onSuccess(Download download) {
          that.startParts(download, encKeyFile);
        }
        public void onFailure(Throwable thrown) {
          System.err.println("Error starting download: " + thrown.getMessage());
          that.run(bucket, key, maxConcurrentConnections, encKeyFile);
        }
      };
      Futures.addCallback(startDownloadFuture, startDownloadCallback);
    }

    public void startParts(Download download, File encKeyFile) {
      this.download = download;

      Map<String,String> meta = download.getMeta();
      if (!meta.containsKey("s3tool-version")) {
        System.err.println("File not uploaded with s3tool");
        System.exit(1);
      }
      if (Float.valueOf(meta.get("s3tool-version")) > 0.0) {
        System.err.println("File uploaded with unsupported version");
        System.exit(1);
      }
      try {
        encKey = readKeyFromFile(meta.get("s3tool-key-name"), encKeyFile);
      } catch (IOException e) {
        System.err.println("IO Error reading key file: " + e.getMessage());
        System.exit(1);
      } catch (ClassNotFoundException e) {
        System.err.println("Error: Key file is not in the right format " + e.getMessage());
        System.exit(1);
      }
      if (encKey == null) {
        System.err.println("Missing encryption key");
        System.exit(1);
      }
      chunkSize = Long.valueOf(meta.get("s3tool-chunk-size"));
      fileLength = Long.valueOf(meta.get("s3tool-file-length"));

      for (long position = 0; position < fileLength; position += chunkSize) {
        startPartDownload(position);
      }
    }

    public void startPartDownload(final long position) {
      final int partNumber = (int) (position / chunkSize);
      long blockSize = 0;
      try {
        blockSize = Cipher.getInstance("AES/CBC/PKCS5Padding").getBlockSize();
      } catch (NoSuchAlgorithmException e) {
        System.err.println("No AES/CBC/PKCS5Padding!");
        System.exit(1);
      } catch (NoSuchPaddingException e) {
        System.err.println("No AES/CBC/PKCS5Padding!");
        System.exit(1);
      }
      long postCryptSize = Math.min(fileLength - position, chunkSize);
      long start = partNumber * blockSize * (chunkSize/blockSize + 2);
      long partSize = blockSize * (postCryptSize/blockSize + 2);

      ListenableFuture<InputStream> getPartFuture = download.getPart(start, start + partSize - 1);
      final DownloadCommand that = this;
      System.out.println("Downloading part " + partNumber);
      Futures.addCallback(getPartFuture, new FutureCallback<InputStream>() {
        public void onSuccess(InputStream stream) {
          that.readDownload(stream, position, partNumber);
        }
        public void onFailure(Throwable thrown) {
          System.err.println("Error downloading part " + partNumber + ": " + thrown.getMessage());
          that.startPartDownload(position);
        }
      });
    }

    public void readDownload(InputStream stream, long position, int partNumber) {
      RandomAccessFile out = null;
      try {
        out = new RandomAccessFile(file, "rw");
      } catch (FileNotFoundException e) {
        System.err.println("Error: Download file location deleted.");
        System.exit(1);
      }
      try {
        out.seek(position);
      } catch (IOException e) {
        System.err.println("Error: IO exception when seeking in output file: " + e.getMessage());
        System.exit(1);
      }
      Cipher cipher = null;
      try {
        cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
      } catch (NoSuchAlgorithmException e) {
        System.err.println("No AES/CBC/PKCS5Padding!");
        System.exit(1);
      } catch (NoSuchPaddingException e) {
        System.err.println("No AES/CBC/PKCS5Padding!");
        System.exit(1);
      }
      CipherWithInlineIVInputStream in = null;
      try {
        in = new CipherWithInlineIVInputStream(stream, cipher, Cipher.DECRYPT_MODE, encKey);
      } catch (IOException e) {
        System.err.println("Error initiating cipher stream for part " + partNumber + ": " + e.getMessage());
        startPartDownload(position);
        return;
      } catch (InvalidKeyException e) {
        System.err.println("Error: Invalid key");
        System.exit(1);
      } catch (InvalidAlgorithmParameterException e) {
        System.err.println("Error downloading part " + partNumber + ": " + e.getMessage());
        startPartDownload(position);
        return;
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
          startPartDownload(position);
          return;
        }
        if (result == -1) {
          System.err.println("Error downloading part " + partNumber + ": unexpected EOF");
          try {
            out.close();
            stream.close();
          } catch (IOException e) {
          }
          startPartDownload(position);
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
          startPartDownload(position);
          return;
        }
        offset += result;
      }
      try {
        out.close();
        stream.close();
      } catch (IOException e) {
      }
      System.out.println("Finished part " + partNumber);
      boolean allDone = false;
      synchronized(this) {
        completedParts.add(partNumber);
        if (completedParts.size() == (fileLength + chunkSize - 1)/chunkSize) {
          allDone = true;
        }
      }

      if (allDone) {
        System.exit(0);
      }
    }
  }
