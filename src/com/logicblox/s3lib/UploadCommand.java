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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import com.amazonaws.services.s3.AmazonS3Client;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

public class UploadCommand extends Command
{
  private Upload upload;
  private String encKeyName;
  
  public UploadCommand(File file, long chunkSize, String encKeyName, File encKeyFile) {
    this.file = file;
    this.chunkSize = chunkSize;
    this.fileLength = file.length();
    this.encKeyName = encKeyName;
    try {
      this.encKey = readKeyFromFile(encKeyName, encKeyFile);
    } catch (IOException e) {
      System.err.println("IO Error reading key file: " + e.getMessage());
      System.exit(1);
    } catch (ClassNotFoundException e) {
      System.err.println("Error: Key file is not in the right format " + e.getMessage());
      System.exit(1);
    }
    completedParts = new HashSet<Integer>();
  }
  
  public void run(final String bucket, final String key, final int maxConcurrentConnections) {
    if (this.encKey == null) {
      System.err.println("Missing encryption key");
      System.exit(1);
    }
    
    if (this.fileLength == 0) {
      throw new UsageException("File does not exist or is a special file");
    }
    
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(maxConcurrentConnections));
    
    UploadFactory factory = new MultipartAmazonUploadFactory(new AmazonS3Client(), executor);
    
    Map<String,String> meta = new HashMap<String,String>();
    meta.put("s3tool-version", "0.0");
    meta.put("s3tool-key-name", encKeyName);
    meta.put("s3tool-chunk-size", Long.toString(chunkSize));
    meta.put("s3tool-file-length", Long.toString(fileLength));
    
    ListenableFuture<Upload> startUploadFuture = factory.startUpload(bucket, key, meta);
    
    System.out.println("Initiating upload");
    final UploadCommand that = this;
    FutureCallback<Upload> startUploadCallback = new FutureCallback<Upload>()
      {
        public void onSuccess(Upload upload) {
          that.startParts(upload);
        }
          
        public void onFailure(Throwable thrown)
        {
          System.err.println("Error starting upload: " + thrown.getMessage());
            that.run(bucket, key, maxConcurrentConnections);
        }
      };
    Futures.addCallback(startUploadFuture, startUploadCallback);
  }
  
  public void startParts(Upload upload) {
    this.upload = upload;
    for (long position = 0; position < fileLength; position += chunkSize) {
      startPartUploadThread(position);
    }
  }
  
  public void startPartUploadThread(final long position) {
    final UploadCommand that = this;
    Executors.newSingleThreadExecutor().submit(new Runnable() {
      public void run() {
        that.startPartUpload(position);
      }
    });
  }
  
  public void startPartUpload(final long position) {
    final int partNumber = (int) (position / chunkSize);
    final FileInputStream fs;
    try {
      fs = new FileInputStream(file);
    } catch (FileNotFoundException e) {
      System.err.println(file.getPath());
      System.err.println("Error: File not found");
      System.exit(1);
      return;
    }
    try {
      long skipped = fs.skip(position);
      while (skipped < position) {
        skipped += fs.skip(position - skipped);
      }
    } catch (IOException e) {
      System.err.println("Error uploading part " + partNumber + ": " + e.getMessage());
      try {
        fs.close();
      } catch (IOException ignored) {
      }
      startPartUpload(position);
      return;
    }
    BufferedInputStream bs = new BufferedInputStream(fs);
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
      in = new CipherWithInlineIVInputStream(bs, cipher, Cipher.ENCRYPT_MODE, encKey);
    } catch (IOException e) {
      System.err.println("Error uploading part " + partNumber + ": " + e.getMessage());
      try {
        fs.close();
      } catch (IOException ignored) {
      }
      startPartUpload(position);
      return;
    } catch (InvalidKeyException e) {
      System.err.println("Error: Invalid key");
      System.exit(1);
    } catch (InvalidAlgorithmParameterException e) {
      System.err.println("Error uploading part " + partNumber + ": " + e.getMessage());
      try {
        fs.close();
      } catch (IOException ignored) {
      }
      startPartUpload(position);
      return;
    }

    long preCryptSize = Math.min(fileLength - position, chunkSize);
    long blockSize = cipher.getBlockSize();
    long partSize = blockSize * (preCryptSize/blockSize + 2);
    System.out.println("Uploading part " + partNumber);
    ListenableFuture<Void> uploadPartFuture = upload.uploadPart(partNumber, in, partSize);
    final UploadCommand that = this;
    Futures.addCallback(uploadPartFuture, new FutureCallback<Void>() {
        public void onSuccess(Void ignored) {
          that.finishPart(partNumber);
          try {
            fs.close();
          } catch (IOException e) {
          }
        }
        public void onFailure(Throwable thrown) {
          System.err.println("Error uploading part " + partNumber + ": " + thrown.getMessage());
          try {
            fs.close();
          } catch (IOException e) {
          }
          that.startPartUpload(position);
        }
      });
  }

  public void finishPart(final int partNumber) {
    System.out.println("Finished part " + partNumber);
    boolean allDone = false;
    synchronized(this) {
      completedParts.add(partNumber);
      if (completedParts.size() == (fileLength + chunkSize - 1)/chunkSize) {
        allDone = true;
      }
    }
    
    if (allDone) {
      ListenableFuture<String> completeUploadFuture = upload.completeUpload();
      final FutureCallback<String> completeUploadCallback = new FutureCallback<String>() {
        public void onSuccess(String etag) {
          System.out.println("File uploaded with etag " + etag);
            System.exit(0);
        }
          public void onFailure(Throwable thrown) {
            System.err.println("Error completing upload: " + thrown);
              finishPart(partNumber);
          }
      };
      Futures.addCallback(completeUploadFuture, completeUploadCallback);
    }
  }
}
