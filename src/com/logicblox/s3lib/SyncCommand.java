package com.logicblox.s3lib;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;

import com.logicblox.s3lib.SyncFile.UrlType;

public class SyncCommand extends Command {
  
  
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  private CloudStoreClient _client;
  
  public SyncCommand(ListeningExecutorService httpExecutor,
      ListeningScheduledExecutorService internalExecutor,
      CloudStoreClient client) {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
    _client = client;
  }
  
  public ListenableFuture<List<SyncFile>> run(final SyncCommandOptions syncOptions)
      throws FileNotFoundException, ExecutionException, InterruptedException, IOException {
    ListenableFuture<List<SyncFile>> future =
        executeWithRetry(_executor, new Callable<ListenableFuture<List<SyncFile>>>() {
          
          
          public ListenableFuture<List<SyncFile>> call()
              throws FileNotFoundException, ExecutionException, InterruptedException, IOException {
            return runActual(syncOptions);
          }
          
          public String toString() {
            return "Sync Source URL with Destination URL";
            
          }
        });
    return future;
  }
  
  private ListenableFuture<List<SyncFile>> runActual(final SyncCommandOptions syncOptions)
      throws FileNotFoundException, ExecutionException, InterruptedException, IOException {
    return _httpExecutor.submit(new Callable<List<SyncFile>>() {
      
      
      public List<SyncFile> call()
          throws FileNotFoundException, ExecutionException, InterruptedException, IOException {
        Map<String, File> fileMap = new TreeMap<String, File>();
        List<SyncFile> all = new ArrayList<SyncFile>();
        Map<String, Long[]> sourceFiles = new TreeMap<String, Long[]>();
        Map<String, Long[]> destinationFiles = new TreeMap<String, Long[]>();
        String sourceType = null;
        String destinationType = null;
        Boolean toStorage = null;
        File file = null;
        
        if (syncOptions.getDestinationBucket() != null && syncOptions.getSourceFilePath() != null) {
          System.out.println("Syncing " + syncOptions.getSourceFilePath() + " to "
              + syncOptions.getDestinationBucket() + "/" + syncOptions.getDestinationKey());
          file = new File(syncOptions.getSourceFilePath());
          if (! file.exists()) {
            throw new FileNotFoundException(file.getPath());
          }
          sourceFiles = getLocalFiles(file, fileMap);
          destinationFiles =
              getS3Files(syncOptions.getDestinationBucket(), syncOptions.getDestinationKey());
          sourceType = UrlType.Local.name();
          destinationType = UrlType.Storage.name();
          toStorage = true;
          
        } else if (syncOptions.getSourceBucket() != null
            && syncOptions.getDestinationFilePath() != null) {
          System.out.println("Syncing " + syncOptions.getSourceBucket()
              + syncOptions.getSourceKey() + " to " + syncOptions.getDestinationFilePath());
          sourceFiles = getS3Files(syncOptions.getSourceBucket(), syncOptions.getSourceKey());
          file = new File(syncOptions.getDestinationFilePath());
          if (! file.exists()) {
            throw new FileNotFoundException(file.getPath());
          }
          destinationFiles = getLocalFiles(file, fileMap);
          sourceType = UrlType.Storage.name();
          destinationType = UrlType.Local.name();
          toStorage = false;
          
        } else if (syncOptions.getSourceBucket() != null
            && syncOptions.getDestinationBucket() != null) {
          System.out.println("Syncing " + syncOptions.getSourceBucket() + "/"
              + syncOptions.getSourceKey() + " to " + syncOptions.getDestinationBucket() + "/"
              + syncOptions.getDestinationKey());
          sourceFiles = getS3Files(syncOptions.getSourceBucket(), syncOptions.getSourceKey());
          destinationFiles =
              getS3Files(syncOptions.getDestinationBucket(), syncOptions.getDestinationKey());
          sourceType = UrlType.Storage.name();
          destinationType = UrlType.Storage.name();
          toStorage = true;
          
        }
        for (Map.Entry<String, Long[]> entry : sourceFiles.entrySet()) {
          if (destinationFiles.containsKey(entry.getKey())
              && destinationFiles.get(entry.getKey())[0].equals(entry.getValue()[0])
              && destinationFiles.get(entry.getKey())[1].equals(entry.getValue()[1])) {
            // file is up to date
          } else {// I need to sync the file by downloading it to local path
            // Skip downloading existing folders
            if (entry.getKey().endsWith("/")
                && (entry.getValue()[1] > - 1 || destinationFiles.containsKey(entry.getKey()))) {
              continue;
            }
            // Skip files that are up to date
            if (destinationFiles.containsKey(entry.getKey()) && upTodate(entry.getValue()[0],
                destinationFiles.get(entry.getKey())[0], toStorage)) {
              continue;
            }
            SyncFile syncfile = new SyncFile();
            if (sourceType.equals(UrlType.Storage.name())) {
              syncfile.setSourceBucket(syncOptions.getSourceBucket());
              syncfile.setSourcKey(entry.getKey());
              if (destinationType.equals(UrlType.Local.name())) {
                syncfile.setSyncAction(SyncFile.SyncAction.DOWNLOAD);
              } else {
                syncfile.setSyncAction(SyncFile.SyncAction.COPY);
              }
            } else {
              syncfile.setSyncAction(SyncFile.SyncAction.UPLOAD);
              syncfile.setLocalFile(fileMap.get(entry.getKey()));
            }
            if (destinationType.equals(UrlType.Storage.name())) {
              syncfile.setDestinationBucket(syncOptions.getDestinationBucket());
              syncfile.setDestinationkey(entry.getKey());
            } else {
              if (destinationFiles.containsKey(entry.getKey())) {
                syncfile.setLocalFile(fileMap.get(entry.getKey()));
              } else {
                syncfile.setLocalFile(new File(file.getParent() + "/" + entry.getKey()));
              }
            }
            all.add(syncfile);
          }
        }
        for (Map.Entry<String, Long[]> entry : destinationFiles.entrySet()) {
          if (! sourceFiles.containsKey(entry.getKey())) {
            SyncFile syncfile = new SyncFile();
            if (entry.getKey().endsWith("/") && sourceType.equals(UrlType.Storage.name())) {
              ListOptionsBuilder lob = new ListOptionsBuilder()
                  .setBucket(syncOptions.getSourceBucket())
                  .setObjectKey(entry.getKey())
                  .setRecursive(true)
                  .setIncludeVersions(false)
                  .setExcludeDirs(false);
              List<S3File> results = _client.listObjects(lob.createListOptions()).get();
              if (results.size() > 0) {
                continue;
              }
            }
            if (destinationType.equals(UrlType.Local.name())) {
              syncfile.setLocalFile(fileMap.get(entry.getKey()));
            } else {
              syncfile.setDestinationBucket(syncOptions.getDestinationBucket());
              syncfile.setDestinationkey(entry.getKey());
            }
            syncfile.setSyncAction(SyncFile.SyncAction.DELETE);
            all.add(syncfile);
          }
        }
        
        return all;
      }
    });
    
  }
  void listFiles(Path path, ArrayList<Path> files) throws IOException {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
      for (Path entry : stream) {
        if (Files.isDirectory(entry)) {
          listFiles(entry, files);
        }
        files.add(entry);
      }
    }
  }
  
  public Map<String, Long[]> getLocalFiles(File root, Map<String, File> fileMap)
      throws IOException {
    
    Map<String, Long[]> files = new TreeMap<String, Long[]>();
    
    getLocalFiles(root, files, fileMap);
    
    return files;
    
  }
  
  public void getLocalFiles(File dir, Map<String, Long[]> list, Map<String, File> fileMap)
      throws IOException {
    ArrayList<Path> LocalFiles = new ArrayList<Path>();
    listFiles(Paths.get(dir.getAbsolutePath()), LocalFiles);
    String root = dir.getName() + "/";
    for (Path path : LocalFiles) {
      File file = path.toFile();
      String key = root + dir.toURI().relativize(file.toURI()).getPath();
      if (file.isFile()) {
        if (file.getName().equals("Thumbs.db"))
          continue;
        if (file.getName().startsWith("."))
          continue;
        Long[] data = {
            file.lastModified(), file.length()
        };
        list.put(key, data);
        fileMap.put(key, file);
      } else if (file.isDirectory()) {
        long actualSize = - 1;
        if (file.list().length > 1) {
          actualSize = file.length();
        }
        Long[] data = {
            file.lastModified(), actualSize
        };
        list.put(key, data);
        fileMap.put(key, file);
        
      }
    }
    
  }
  
  public Map<String, Long[]> getS3Files(String bucketName, String prefix)
      throws ExecutionException, InterruptedException {
    Map<String, Long[]> files = new TreeMap<String, Long[]>();
    ListOptionsBuilder lob = new ListOptionsBuilder()
        .setBucket(bucketName)
        .setObjectKey(prefix)
        .setRecursive(true)
        .setIncludeVersions(false)
        .setExcludeDirs(false);
    List<S3File> listCommandResults = _client.listObjects(lob.createListOptions()).get();
    for (S3File obj : listCommandResults) {
      long actualSize = 0;
      if (obj.getKey().endsWith("/")) {
        ListOptionsBuilder inlob = new ListOptionsBuilder()
            .setBucket(bucketName)
            .setObjectKey(obj.getKey())
            .setRecursive(true)
            .setIncludeVersions(false)
            .setExcludeDirs(false);
        List<S3File> results = _client.listObjects(inlob.createListOptions()).get();
        if (! (results.size() > 1))
          actualSize = - 1;
      } else {
        ObjectMetadata metadata = _client.exists(bucketName, obj.getKey()).get();
        Map<String, String> userMetadata = metadata.getUserMetadata();
        actualSize = userMetadata.get("s3tool-file-length") != null
            ? Long.valueOf(userMetadata.get("s3tool-file-length")) : obj.getSize().get();
      }
      Long[] data = {
          obj.getTimestamp().get().getTime(), actualSize
      };
      files.put(obj.getKey(), data);
    }
    return files;
  }
  
  public boolean upTodate(
      long sourceLastModified,
      long destinationLastModified,
      boolean toStorage) {
    long delta = destinationLastModified - sourceLastModified;
    if (toStorage && delta >= 0) {
      return true;
      
    } else if (! toStorage && delta <= 0) {
      return true;
    }
    
    return false;
  }
  
}
