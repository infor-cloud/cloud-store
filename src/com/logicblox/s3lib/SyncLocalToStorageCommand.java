package com.logicblox.s3lib;

import java.io.File;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;

public class SyncLocalToStorageCommand extends Command {
  
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  
  public SyncLocalToStorageCommand(ListeningExecutorService httpExecutor,
      ListeningScheduledExecutorService internalExecutor) {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
  }
  
  public ListenableFuture<List<SyncFile>> run(final SyncCommandOptions syncOptions)
      throws FileNotFoundException {
    ListenableFuture<List<SyncFile>> future =
        executeWithRetry(_executor, new Callable<ListenableFuture<List<SyncFile>>>() {
          
          
          public ListenableFuture<List<SyncFile>> call() throws FileNotFoundException {
            return runActual(syncOptions);
          }
          
          public String toString() {
            return "listing objects and directories for "
                + getUri(syncOptions.getDestinationBucket(), syncOptions.getDestinatioKey());
          }
        });
    return future;
  }
  
  private ListenableFuture<List<SyncFile>> runActual(final SyncCommandOptions syncOptions)
      throws FileNotFoundException {
    return _httpExecutor.submit(new Callable<List<SyncFile>>() {
      
      
      public List<SyncFile> call() throws FileNotFoundException {
        System.out.println("Syncing " + syncOptions.getSourceFilePath() + " to "
            + syncOptions.getDestinationBucket() + "/" + syncOptions.getDestinatioKey());
        
        Map<String, File> fileMap = new HashMap<String, File>();
        File file = new File(syncOptions.getSourceFilePath());
        if (! file.exists()) {
          throw new FileNotFoundException(file.getPath());
        }
        List<SyncFile> all = new ArrayList<SyncFile>();
        Map<String, Long[]> localFiles = getLocalFiles(file, fileMap);
        Map<String, Long[]> S3Files =
            getS3Files(syncOptions.getDestinationBucket(), syncOptions.getDestinatioKey());
        for (Map.Entry<String, Long[]> entry : localFiles.entrySet()) {
          if (S3Files.containsKey(entry.getKey())
              && S3Files.get(entry.getKey())[0].equals(entry.getValue()[0])
              && S3Files.get(entry.getKey())[1].equals(entry.getValue()[1])) {
            // file is up to date
            
          } else {// I need to sync the file by uploading it to S3
            // Skip uploading entire folder and just upload sub files of the folder
            if (entry.getKey().endsWith("/")) {
              continue;
            }
            // Skip files that are up to date
            if (S3Files.containsKey(entry.getKey())
                && upTodate(entry.getValue()[0], S3Files.get(entry.getKey())[0])) {
              continue;
            }
            
            SyncFile syncfile = new SyncFile();
            syncfile.set_destination_bucket(syncOptions.getDestinationBucket());
            syncfile.set_destination_key(entry.getKey());
            syncfile.setSyncAction(SyncFile.SyncAction.UPLOAD);
            syncfile.setLocalFile(fileMap.get(entry.getKey()));
            all.add(syncfile);
            
          }
        }
        for (Map.Entry<String, Long[]> entry : S3Files.entrySet()) {
          if (! localFiles.containsKey(entry.getKey())) {
            SyncFile syncfile = new SyncFile();
            syncfile.setSyncAction(SyncFile.SyncAction.DELETEREMOTE);
            syncfile.set_destination_bucket(syncOptions.getDestinationBucket());
            syncfile.set_destination_key(entry.getKey());
            all.add(syncfile);
          }
        }
        
        return all;
      }
    });
    
  }
  
  public boolean upTodate(long sourceLastModified, long destinationLastModified) {
    
    long delta = destinationLastModified - sourceLastModified;
    if (delta >= 0)
      return true;
    return false;
    
  }
  
  public Map<String, Long[]> getLocalFiles(File root, Map<String, File> fileMap) {
    
    Map<String, Long[]> files = new TreeMap<String, Long[]>();
    
    getLocalFilesRecursive(root, files, fileMap, root.getName() + "/");
    
    return files;
    
  }
  
  public void getLocalFilesRecursive(
      File dir,
      Map<String, Long[]> list,
      Map<String, File> fileMap,
      String prefix) {
    
    for (File file : dir.listFiles()) {
      
      if (file.isFile()) {
        if (file.getName().equals("Thumbs.db"))
          continue;
        if (file.getName().startsWith("."))
          continue;
        String key = prefix + file.getName();
        key = key.replaceFirst("\\.noex\\.[^\\.]+$", "");
        Long[] data = {
            file.lastModified(), file.length()
        };
        list.put(key, data);
        fileMap.put(key, file);
      } else if (file.isDirectory()) {
        String key = prefix + file.getName() + "/";
        Long[] data = {
            file.lastModified(), file.length()
        };
        list.put(key, data);
        fileMap.put(key, file);
        getLocalFilesRecursive(file, list, fileMap, prefix + file.getName() + "/");
      }
      
    }
  }
  
  public Map<String, Long[]> getS3Files(String bucketName, String prefix) {
    
    Map<String, Long[]> files = new TreeMap<String, Long[]>();
    
    ListObjectsRequest req = new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix);
    
    ObjectListing current;
    
    do {
      current = getAmazonS3Client().listObjects(req);
      for (S3ObjectSummary summary : current.getObjectSummaries()) {
        GetObjectMetadataRequest metadataReq =
            new GetObjectMetadataRequest(bucketName, summary.getKey());
        ObjectMetadata metadata = getAmazonS3Client().getObjectMetadata(metadataReq);
        Map<String, String> userMetadata = metadata.getUserMetadata();
        Long actualSize = userMetadata.get("s3tool-file-length") != null
            ? Long.valueOf(userMetadata.get("s3tool-file-length")) : summary.getSize();
        Long[] data = {
            summary.getLastModified().getTime(), actualSize
        };
        files.put(summary.getKey(), data);
      }
    } while (current.isTruncated());
    return files;
    
  }
  
}
