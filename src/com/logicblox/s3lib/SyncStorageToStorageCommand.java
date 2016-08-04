
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

public class SyncStorageToStorageCommand extends Command {
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  
  public SyncStorageToStorageCommand(ListeningExecutorService httpExecutor,
      ListeningScheduledExecutorService internalExecutor) {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
  }
  
  public ListenableFuture<List<SyncFile>> run(final SyncCommandOptions syncOptions) throws FileNotFoundException {
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
  
  private ListenableFuture<List<SyncFile>> runActual(final SyncCommandOptions syncOptions) throws FileNotFoundException{
    return _httpExecutor.submit(new Callable<List<SyncFile>>() {
      
      public List<SyncFile> call() throws FileNotFoundException {
        System.out.println("Syncing " + syncOptions.getSourcebucket() + "/"+syncOptions.getSourceoKey() +" to "
            + syncOptions.getDestinationBucket()+"/"+syncOptions.getDestinatioKey());
        
        List<SyncFile> all = new ArrayList<SyncFile>();
        Map<String, Long[]> S3SourceFiles =
            getS3Files(syncOptions.getSourcebucket(), syncOptions.getSourceoKey());
        Map<String, Long[]> S3DestinationFiles =
            getS3Files(syncOptions.getDestinationBucket(), syncOptions.getDestinatioKey());
        
        for (Map.Entry<String, Long[]> entry : S3SourceFiles.entrySet()) {
          if (S3DestinationFiles.containsKey(entry.getKey())
              && S3DestinationFiles.get(entry.getKey())[0].equals(entry.getValue()[0])
              && S3DestinationFiles.get(entry.getKey())[1].equals(entry.getValue()[1])) {
            // file is up to date
            
          } else {// I need to sync the file by copying to destination it to S3
            //Skip copying entire folder and just copy sub files of the folder
           if(entry.getKey().endsWith("/")){
              continue;
           }
           //Skip files that are up to date 
           if(S3DestinationFiles.containsKey(entry.getKey()) && upTodate(entry.getValue()[0],S3DestinationFiles.get(entry.getKey())[0])) {
             continue ;
           }
           
            SyncFile syncfile = new SyncFile();
            syncfile.set_destination_bucket(syncOptions.getDestinationBucket());
            syncfile.set_destination_key(entry.getKey());
            syncfile.set_source_bucket(syncOptions.getSourcebucket());
            syncfile.set_source_key(entry.getKey());
            syncfile.setSyncAction(SyncFile.SyncAction.COPY);
            all.add(syncfile);
            
          }
        }
        // Delete files that are not in the remote source, skip directories
        for (Map.Entry<String, Long[]> entry : S3DestinationFiles.entrySet()) {
          if (!S3SourceFiles.containsKey(entry.getKey()) && !entry.getKey().endsWith("/")) {
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
  
  public boolean upTodate(long sourceLastModified, long destinationLastModified){
    
    long delta = destinationLastModified - sourceLastModified;
    if (delta>=0) return true;
    return false ;
    
  }
  
  public Map<String, Long[]> getS3Files(String bucketName, String prefix) {
    
    Map<String, Long[]> files = new TreeMap<String, Long[]>();
    
    ListObjectsRequest req = new ListObjectsRequest()
        .withBucketName(bucketName)
        .withPrefix(prefix);
    
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
