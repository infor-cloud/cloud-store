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

import joptsimple.OptionParser;
import joptsimple.OptionSpec;
import joptsimple.OptionSet;

import com.amazonaws.services.s3.AmazonS3Client;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

public class Main {
  public static void main(String[] args)
  {
    Logger root = Logger.getRootLogger();
    root.setLevel(Level.INFO);
    File defaultEncKeyFile = new File(System.getProperty("user.home") + File.separator + ".s3lib-enc-keys");
    OptionParser parser = new OptionParser();
    OptionSpec<File> fileSpec = parser.accepts("file", "The file to upload/download to/from").withRequiredArg().ofType(File.class).describedAs("Required");
    OptionSpec<String> keySpec = parser.accepts("key", "The name of the file in s3").withRequiredArg().describedAs("Required");
    OptionSpec<String> bucketSpec = parser.accepts("bucket", "The s3 bucket").withRequiredArg().describedAs("Required");
    OptionSpec<Integer> maxConcurrentConnectionsSpec = parser.accepts("max-concurrent-connections", "The maximum number of concurrent HTTP connections to s3").withRequiredArg().ofType(Integer.class).defaultsTo(Integer.valueOf(10)).describedAs("Optional");
    OptionSpec<Long> chunkSizeSpec = parser.accepts("chunk-size", "The size of each chunk read from the file").withRequiredArg().ofType(Long.class).describedAs("Required for upload");
    OptionSpec<String> encKeyNameSpec = parser.accepts("enc-key-name", "The key to use for encryption").withRequiredArg().describedAs("Required for upload");
    OptionSpec<File> encKeyFileSpec = parser.accepts("enc-key-file", "The file where the encryption keys (represented as a serialization of java.util.HashMap<java.lang.String,java.security.Key>) are found").withRequiredArg().ofType(File.class).defaultsTo(defaultEncKeyFile).describedAs("Optional");

    OptionSet options = parser.parse(args);

    List<String> commands = options.nonOptionArguments();

    if (commands.size() != 1) {
      showUsage(parser);
    }

    String command = commands.get(0);

    if (!options.has(fileSpec)) {
      showUsage(parser);
    }
    File file = options.valueOf(fileSpec);

    if (!options.has(keySpec)) {
      showUsage(parser);
    }
    String key = options.valueOf(keySpec);

    if (!options.has(bucketSpec)) {
      showUsage(parser);
    }
    String bucket = options.valueOf(bucketSpec);

    int maxConcurrentConnections = options.valueOf(maxConcurrentConnectionsSpec).intValue();

    long chunkSize = 0;
    String encKeyName = null;
    if (command.equals("upload")) {
      if (!options.has(chunkSizeSpec)) {
        showUsage(parser);
      }
      chunkSize = options.valueOf(chunkSizeSpec).longValue();

      if (!options.has(encKeyNameSpec)) {
        showUsage(parser);
      }
      encKeyName = options.valueOf(encKeyNameSpec);
    }

    File encKeyFile = options.valueOf(encKeyFileSpec);

    if (command.equals("upload")) {
      new UploadCommand(file, chunkSize, encKeyName, encKeyFile).run(bucket, key, maxConcurrentConnections);
    } else if (command.equals("download")) {
      new DownloadCommand(file).run(bucket, key, maxConcurrentConnections, encKeyFile);
    } else {
      showUsage(parser);
    }
  }

  private static void showUsage(OptionParser parser) {
    System.err.println("Usage: s3tool <command> <args>");
    System.err.println("Commands: upload download");
    try {
      parser.printHelpOn(System.err);
    } catch (IOException e) {
    }
    System.exit(1);
  }

  private static Key readKeyFromFile(String encKeyName, File encKeyFile) throws IOException, ClassNotFoundException {
    FileInputStream fs = new FileInputStream(encKeyFile);
    ObjectInputStream in = new ObjectInputStream(fs);
    Map<String,Key> keys = (HashMap<String,Key>) in.readObject();
    in.close();
    if (keys.containsKey(encKeyName)) {
      return keys.get(encKeyName);
    }
    return null;
  }

  private static class Command {
    protected File file;
    protected long chunkSize;
    protected Key encKey;
    protected Set<Integer> completedParts;
    protected long fileLength;
  }

  private static class DownloadCommand extends Command {
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

  private static class UploadCommand extends Command {
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
        System.err.println("File does not exist or is a special file");
        System.exit(1);
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
      FutureCallback<Upload> startUploadCallback = new FutureCallback<Upload>() {
        public void onSuccess(Upload upload) {
          that.startParts(upload);
        }
        public void onFailure(Throwable thrown) {
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
}
