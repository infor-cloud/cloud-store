package com.logicblox;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.logicblox.s3lib.DirectoryKeyProvider;
import com.logicblox.s3lib.KeyProvider;
import com.logicblox.s3lib.S3Client;
import com.logicblox.s3lib.UsageException;
import com.logicblox.s3lib.Utils;

public class S3uploader {
   private static ListeningExecutorService getHttpExecutor() {
        int maxConcurrentConnections = 10;
        return MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(maxConcurrentConnections));
    }

    private static ListeningScheduledExecutorService getInternalExecutor() {
        return MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(50));
    }

    static String encKeyName = "kiabi-encryption-key";
    static String cannedAcl = "bucket-owner-full-control";

    private static KeyProvider getKeyProvider(String encKeyDirectory) {
        File dir = new File(encKeyDirectory);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new UsageException(String.format("specified key directory '%s' does not exist", encKeyDirectory));
        }
        if (!dir.isDirectory()) {
            throw new UsageException(String.format("specified key directory '%s' is not a directory", encKeyDirectory));
        }
        return new DirectoryKeyProvider(dir);
    }

    protected static URI getURI(List<String> urls) throws URISyntaxException {
        if (urls.size() != 1) {
            throw new UsageException("A single S3 object URL is required");
        }
        return Utils.getURI(urls.get(0));
    }

    protected static String getBucket(List<String> urls) throws URISyntaxException {
        return Utils.getBucket(getURI(urls));
    }

    protected static String getObjectKey(List<String> urls) throws URISyntaxException {
        return Utils.getObjectKey(getURI(urls));
    }

    private static CannedAccessControlList getAcl(String value)
    {
      for (CannedAccessControlList acl : CannedAccessControlList.values())
      {
        if (acl.toString().equals(value))
        {
          return acl;
        }
      }
      return null;
    }

    public static void  upload(S3Client client, String file, List<String> urls) throws Exception
    {
      CannedAccessControlList acl = getAcl(cannedAcl);
      if(acl == null)
      {
        throw new UsageException("Unknown canned ACL '"+cannedAcl+"'");
      }

      if (getObjectKey(urls).endsWith("/")) {
        throw new UsageException("Destination key " + getBucket(urls) + "/" + getObjectKey(urls) +
            " should be fully qualified. No trailing '/' is permitted.");
      }

      File f = new File(file);
      if(f.isFile()) {
        client.upload(f, getBucket(urls), getObjectKey(urls), encKeyName, acl).get();
      } else if(f.isDirectory()) {
        client.uploadDirectory(f, getURI(urls), encKeyName, acl).get();
      } else {
        throw new UsageException("File '"+file+"' is not a file or a directory.");
      }
      client.shutdown();
    }

    public static void main(String[] args) throws Exception {
        ClientConfiguration clientCfg = new ClientConfiguration();
        clientCfg.setProtocol(Protocol.HTTPS);
        // Defining the client configuration 
        // You can define a proxy connection here if needed as follow : 
        //    clientCfg.setProxyHost("localhost");
        //    clientCfg.setProxyPort(8118);
        
        Map<String, String> env = System.getenv();

        // System.out.format("%s=%s%n","AWS_ACCESS_KEY_ID",env.get("AWS_ACCESS_KEY_ID"));
        // System.out.format("%s=%s%n","AWS_SECRET_KEY",env.get("AWS_SECRET_KEY"));

        AmazonS3Client s3Client = new AmazonS3Client(clientCfg);

        long chunkSize = Utils.getDefaultChunkSize();
        
        // This specify the encryption key directory. It's setup to be $HOME/.s3lib-keys here
        String key_dir=String.format("%s/.s3lib-keys",env.get("HOME") );
        
        // Define the s3 client
        S3Client client = new S3Client(s3Client, getHttpExecutor(), getInternalExecutor(), chunkSize, getKeyProvider(key_dir));
        List<String> urls = new ArrayList<String>();
        
        // upload the file to this url
        String target = "s3://kiabi-s3-poc/test/test.dat.gz";
        urls.add(target);
        String file="test.dat.gz";

        System.out.println(String.format("Encryption key is '%s'.", encKeyName));
        System.out.println(String.format("Uploading '%s' to '%s'.", file, target));
        upload(client, file, urls);
        client.shutdown();
    }


}
