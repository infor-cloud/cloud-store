package com.logicblox.s3lib;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import com.google.common.util.concurrent.ListenableFuture;

import com.amazonaws.AmazonServiceException;

class Main
{
  JCommander _commander = new JCommander();

  public static void main(String[] args)
  {
    Utils.initLogging();

    try
    {
      Main main = new Main();
      main.execute(args);
    }
    catch(Exception exc)
    {
      exc.printStackTrace();
      System.exit(1);
    }
  }


  public Main()
  {
    _commander = new JCommander(new MainCommand());
    _commander.setProgramName("cloud-store");
    _commander.addCommand("upload", new UploadCommandOptions());
    _commander.addCommand("download", new DownloadCommandOptions());
    _commander.addCommand("copy", new CopyCommandOptions());
    _commander.addCommand("rename", new RenameCommandOptions());
    _commander.addCommand("delete", new DeleteCommandOptions());
    _commander.addCommand("ls", new ListCommandOptions());
    _commander.addCommand("du", new DiskUsageCommandOptions());
    _commander.addCommand("list-pending-uploads", new
        ListPendingUploadsCommandOptions());
    _commander.addCommand("abort-pending-uploads", new
        AbortPendingUploadsCommandOptions());
    _commander.addCommand("exists", new ExistsCommandOptions());
    _commander.addCommand("list-buckets", new ListBucketsCommandOptions());
    _commander.addCommand("add-encryption-key", new
      AddEncryptionKeyCommandOptions());
    _commander.addCommand("remove-encryption-key", new
      RemoveEncryptionKeyCommandOptions());
    _commander.addCommand("keygen", new KeyGenCommandOptions());
    _commander.addCommand("version", new VersionCommand());
    _commander.addCommand("help", new HelpCommand());
  }

  class MainCommand
  {
    @Parameter(names = { "-h", "--help" }, description = "Print usage information", help = true)
    boolean help = false;
  }

  abstract class CommandOptions
  {
    @Parameter(names = { "-h", "--help" }, description = "Print usage information", help = true)
    boolean help = false;

    public abstract void invoke() throws Exception;
  }

  /**
   * Abstraction for all storage service commands
   */
  abstract class S3CommandOptions extends CommandOptions
  {
    @Parameter(names = {"--max-concurrent-connections"}, description = "The " +
        "maximum number of concurrent HTTP connections to the storage service")
    int maxConcurrentConnections = Utils.getDefaultMaxConcurrentConnections();

    @Parameter(names = "--endpoint", description = "Endpoint")
    String endpoint = null;

    @Parameter(names = "--keydir", description = "Directory where encryption keys are found")
    String encKeyDirectory = Utils.getDefaultKeyDirectory();

    @Parameter(names = "--stubborn", description = "Retry client exceptions (e.g. file not found and authentication errors)")
    boolean _stubborn = false;

    @Parameter(names = "--retry", description = "Number of retries on failures")
    int _retryCount = Utils.getDefaultRetryCount();

    @Parameter(names = {"--credential-providers-s3"}, description = "The " +
        "order of the credential providers that should be checked for S3. The" +
        " default order is: \"env-vars\", " + "\"system-properties\", " +
        "\"credentials-profile\", \"ec2-metadata-service\". Choices should be" +
        " comma-separated without any spaces, e.g. \"env-vars," +
        "ec2-metadata-service\"." ,
        validateValueWith = CredentialProvidersValidator.class)
    List<String> credentialProvidersS3;

    protected URI getURI() throws URISyntaxException
    {
      return null;
    }

    protected String getScheme() throws URISyntaxException
    {
      return null;
    }

    protected CloudStoreClient createCloudStoreClient()
        throws URISyntaxException, IOException, GeneralSecurityException
    {
      return Utils.createCloudStoreClient(
        getScheme(), endpoint, maxConcurrentConnections, 
        encKeyDirectory, credentialProvidersS3, _stubborn, _retryCount);
    }
  }

  public static class CredentialProvidersValidator implements IValueValidator<List<String>>
  {
    @Override
    public void validate(String name, List<String> credentialsProvidersS3) throws ParameterException
    {
      for (String cp : credentialsProvidersS3)
        if (!Utils.defaultCredentialProvidersS3.contains(cp))
          throw new ParameterException("Credential providers should be a " +
              "subset of " + Arrays.toString(Utils.defaultCredentialProvidersS3.toArray()));
    }
  }

  /**
   * Abstraction for commands that deal with storage service objects
   */
  abstract class S3ObjectCommandOptions extends S3CommandOptions
  {
    @Parameter(description = "storage-service-url", required = true)
    List<String> urls;

    protected URI getURI() throws URISyntaxException
    {
      if(urls.size() != 1)
        throw new UsageException("A single storage service object URL is " +
            "required");

      return Utils.getURI(urls.get(0));
    }

    protected String getBucket() throws URISyntaxException
    {
      return Utils.getBucket(getURI());
    }

    protected String getObjectKey() throws URISyntaxException
    {
      return Utils.getObjectKey(getURI());
    }

    protected String getScheme() throws URISyntaxException
    {
      return getURI().getScheme();
    }
  }

  /**
   * Abstraction for commands that deal with two object/prefix URLs
   */
  abstract class TwoObjectsCommandOptions extends S3CommandOptions
  {
    // Upgrade JCommander to support following lines
    // @Parameter(description = "source-url", required = true)
    // String sourceURL;
    //
    // @Parameter(description = "destination-url", required = true)
    // String destinationURL;

    @Parameter(description = "source-url destination-url", required = true)
    List<String> urls;

    protected URI getSourceURI() throws URISyntaxException
    {
      if(urls.size() != 2)
        throw new UsageException("Two object URLs are required");

      return Utils.getURI(urls.get(0));
    }

    protected String getSourceBucket() throws URISyntaxException
    {
      if(urls.size() != 2)
        throw new UsageException("Two object URLs are required");

      return Utils.getBucket(getSourceURI());
    }

    protected String getSourceObjectKey() throws URISyntaxException
    {
      return Utils.getObjectKey(getSourceURI());
    }

    protected String getScheme() throws URISyntaxException
    {
      return getSourceURI().getScheme();
    }

    protected URI getDestinationURI() throws URISyntaxException
    {
      return Utils.getURI(urls.get(1));
    }

    protected String getDestinationBucket() throws URISyntaxException
    {
      return Utils.getBucket(getDestinationURI());
    }

    protected String getDestinationObjectKey() throws URISyntaxException
    {
      return Utils.getObjectKey(getDestinationURI());
    }
  }

  @Parameters(commandDescription = "List storage service buckets")
  class ListBucketsCommandOptions extends S3CommandOptions
  {
    @Parameter(description = "service", required = true)
    List<String> services;

    protected String getScheme()
    {
      if(null == services)
      {
        if(null == endpoint)
          throw new UsageException("Either 's3' or 'gs' service is required");
        return null;
      }

      if(services.size() != 1)
        throw new UsageException("Only one service name may be specified");

      String service = services.get(0);
      if(!service.equals("s3") && !service.equals("gs"))
        throw new UsageException("Either 's3' or 'gs' service is required");
      return service;
    }

    public void invoke() throws Exception
    {
      CloudStoreClient client = createCloudStoreClient();
      ListenableFuture<List<Bucket>> result = client.listBuckets();

      List<Bucket> buckets = result.get();
      Collections.sort(buckets, new Comparator<Bucket>(){
          public int compare(Bucket b1, Bucket b2) {
            return b1.getName().toLowerCase().compareTo(b2.getName().toLowerCase());
          }});

      String[][] table = new String[buckets.size()][3];
      int[] max = new int[3];

      DateFormat df = Utils.getDefaultDateFormat();

      for(int i = 0; i < buckets.size(); i++)
      {
        Bucket b = buckets.get(i);
        table[i][0] = b.getName();
        table[i][1] = df.format(b.getCreationDate());
        String ownerName = b.getOwner().getDisplayName();
        table[i][2] = (ownerName != null) ? ownerName : b.getOwner().getId();

        for(int j = 0; j < 3; j++)
          max[j] = Math.max(table[i][j].length(), max[j]);
      }

      for (final String[] row : table)
      {
        System.out.format("%-" + (max[2] + 3) + "s%-" + (max[1] + 3) + "s%-" + (max[0] + 3) + "s\n", row[2], row[1], row[0]);
      }

      client.shutdown();
    }
  }

  @Parameters(commandDescription = "Check if a file exists in the storage service")
  class ExistsCommandOptions extends S3ObjectCommandOptions
  {
    @Parameter(names = "--verbose", description = "Print information about success/failure and metadata if object exists")
    boolean _verbose = false;

    public void invoke() throws Exception
    {
      CloudStoreClient client = createCloudStoreClient();
      String bucket = getBucket();
      String key = getObjectKey();

      ExistsOptions opts = client.getOptionsBuilderFactory()
        .newExistsOptionsBuilder()
        .setBucketName(bucket)
        .setObjectKey(key)
        .createOptions();

      ListenableFuture<Metadata> result = client.exists(opts);

      boolean exists = false;
      Metadata metadata = result.get();
      if(metadata == null)
      {
        if(_verbose)
          System.err.println("Object " + Utils.getURI(client.getScheme(), bucket, key) +
                             " does not exist.");
      }
      else
      {
        exists = true;

        if(_verbose)
          metadata.print(System.out);
      }

      client.shutdown();

      if (!exists)
        System.exit(1);
    }

  }

  @Parameters(commandDescription = "Copy prefix/object. If destination URI is" +
      " a directory (ends with '/'), then source URI acts as a prefix and " +
      "this operation will copy all keys that would be returned by the list " +
      "operation on the same prefix. Otherwise, we go for a direct key-to-key" +
      " copy.")
  class CopyCommandOptions extends TwoObjectsCommandOptions
  {
    @Parameter(names = "--canned-acl", description = "The canned ACL to use. "
        + S3Client.CANNED_ACLS_DESC_CONST)
    String cannedAcl;

    @Parameter(names = "--storage-class", description = "The storage class to" +
        " use. Source object's storage class will be used by default. " +
        S3Client.STORAGE_CLASSES_DESC_CONST)
    String storageClass;

    @Parameter(names = {"-r", "--recursive"}, description = "Copy recursively")
    boolean recursive = false;

    @Parameter(names = "--dry-run", description = "Display operations but do not execute them")
    boolean dryRun = false;

    public void invoke() throws Exception
    {
      CloudStoreClient client = createCloudStoreClient();

      CopyOptions options = client.getOptionsBuilderFactory()
          .newCopyOptionsBuilder()
          .setSourceBucketName(getSourceBucket())
          .setSourceObjectKey(getSourceObjectKey())
          .setDestinationBucketName(getDestinationBucket())
          .setDestinationObjectKey(getDestinationObjectKey())
          .setCannedAcl(cannedAcl)
          .setStorageClass(storageClass)
          .setRecursive(recursive)
          .setDryRun(dryRun)
          .createOptions();

      try
      {
        ExistsOptions opts = client.getOptionsBuilderFactory()
          .newExistsOptionsBuilder()
          .setBucketName(getDestinationBucket())
          .setObjectKey("")
          .createOptions();

        // Check if destination bucket exists
        if (client.exists(opts).get() == null)
        {
          throw new UsageException("Bucket not found at " +
              Utils.getURI(client.getScheme(), getDestinationBucket(), ""));
        }

        if(getDestinationObjectKey().endsWith("/") ||
            getDestinationObjectKey().equals(""))
        {
          // If destination URI is a directory (ends with "/") or a bucket
          // (object URI is empty), then source URI acts as a prefix and this
          // operation will copy all keys that would be returned by the list
          // operation on the same prefix.
          client.copyToDir(options).get();
        }
        else
        {
          opts = client.getOptionsBuilderFactory()
            .newExistsOptionsBuilder()
            .setBucketName(getSourceBucket())
            .setObjectKey(getSourceObjectKey())
            .createOptions();

          // We go for a direct key-to-key copy, so source object has
          // to be there.
          if (client.exists(opts).get() == null)
          {
            throw new UsageException("Object not found at " + getSourceURI());
          }
          client.copy(options).get();
        }
      }
      catch(ExecutionException exc)
      {
        rethrow(exc.getCause());
      }
      finally
      {
        client.shutdown();
      }
    }
  }

  
  @Parameters(commandDescription = "Rename an object or object prefix. If the " +
      "source URI ends with '/', then it acts as a prefix and " +
      "this operation will rename all objects that would be returned by the list " +
      "operation on the same prefix.")
  class RenameCommandOptions extends TwoObjectsCommandOptions
  {
    @Parameter(names = "--canned-acl", description = "The canned ACL to use. "
        + S3Client.CANNED_ACLS_DESC_CONST)
    String cannedAcl;

    @Parameter(names = {"-r", "--recursive"}, description = "Rename recursively")
    boolean recursive = false;

    @Parameter(names = "--dry-run", description = "Display operations but do not execute them")
    boolean dryRun = false;

    public void invoke() throws Exception
    {
      CloudStoreClient client = createCloudStoreClient();

      RenameOptions options = client.getOptionsBuilderFactory()
          .newRenameOptionsBuilder()
          .setSourceBucketName(getSourceBucket())
          .setSourceObjectKey(getSourceObjectKey())
          .setDestinationBucketName(getDestinationBucket())
          .setDestinationObjectKey(getDestinationObjectKey())
          .setCannedAcl(cannedAcl)
          .setRecursive(recursive)
          .setDryRun(dryRun)
          .createOptions();

      try
      {
        if(getSourceObjectKey().endsWith("/"))
        {
          client.renameDirectory(options).get();
        }
        else
        {
          client.rename(options).get();
        }
      }
      catch(ExecutionException exc)
      {
        rethrow(exc.getCause());
      }
      finally
      {
        client.shutdown();
      }
    }
  }


  @Parameters(commandDescription = "Upload a file or directory to the storage service")
  class UploadCommandOptions extends S3ObjectCommandOptions
  {
    @Parameter(names = "-i", description = "File or directory to upload", required = true)
    String file;

    @Parameter(names = "--key", description = "The name of the encryption key to use")
    String encKeyName = null;

    @Parameter(names = "--progress", description = "Enable progress indicator")
    boolean progress = false;

    @Parameter(names = "--dry-run", description = "Display operations but do not execute them")
    boolean dryRun = false;

    @Parameter(names = "--canned-acl", description = "The canned ACL to use. "
        + S3Client.CANNED_ACLS_DESC_CONST + " " + GCSClient.CANNED_ACLS_DESC_CONST)
    String cannedAcl;

    @Parameter(names = {"--chunk-size"},
      description = "The size of each chunk read from the file. Determined " +
                    "automatically if not set.")
    long chunkSize = -1;

    public void invoke() throws Exception
    {
      CloudStoreClient client = createCloudStoreClient();
      File f = new File(file);

      if (f.isDirectory() && !getObjectKey().endsWith("/")) {
        throw new UsageException("Destination key " +
            Utils.getURI(client.getScheme(), getBucket(), getObjectKey()) +
            " should end with '/', since a directory is uploaded.");
      }

      UploadOptionsBuilder uob = client.getOptionsBuilderFactory()
          .newUploadOptionsBuilder()
          .setFile(f)
          .setBucketName(getBucket())
          .setObjectKey(getObjectKey())
          .setChunkSize(chunkSize)
          .setEncKey(encKeyName)
          .setCannedAcl(cannedAcl)
          .setDryRun(dryRun);

      if (progress) {
        OverallProgressListenerFactory cplf = new
            ConsoleProgressListenerFactory();
        uob.setOverallProgressListenerFactory(cplf);
      }

      if(f.isFile()) {
        if (getObjectKey().endsWith("/"))
          uob.setObjectKey(getObjectKey() + f.getName());
        client.upload(uob.createOptions()).get();
      } else if(f.isDirectory()) {
        client.uploadDirectory(uob.createOptions()).get();
      } else {
        throw new UsageException("File '" + file + "' is not a file or a " +
            "directory.");
      }
      client.shutdown();
    }
  }

  @Parameters(commandDescription = "List objects in storage service")
  class ListCommandOptions extends S3ObjectCommandOptions
  {
    @Parameter(names = {"-r", "--recursive"}, description = "List all objects" +
        " that match the provided storage service URL prefix.")
    boolean recursive = false;

    @Parameter(names = {"--exclude-dirs"}, description = "List only objects " +
        "(excluding first-level directories) that match the provided storage " +
        "service URL prefix")
    boolean excludeDirs = false;

    @Parameter(names = {"--include-versions"}, description = "List objects versions" +
        "that match the provided storage " +
        "service URL prefix")
    boolean includeVersions = false;
    
    @Override
    public void invoke() throws Exception {
      CloudStoreClient client = createCloudStoreClient();
      ListOptionsBuilder lob = client.getOptionsBuilderFactory()
          .newListOptionsBuilder()
          .setBucketName(getBucket())
          .setObjectKey(getObjectKey())
          .setRecursive(recursive)
          .setIncludeVersions(includeVersions)
          .setExcludeDirs(excludeDirs);
      try {
        List<StoreFile> listCommandResults = client.listObjects(lob.createOptions()).get();
        if (includeVersions) {
          String[][] table = new String[listCommandResults.size()][4];
          int[] max = new int[4];
          DateFormat df = Utils.getDefaultDateFormat();
          for (int i = 0; i < listCommandResults.size(); i++) {
            StoreFile obj = listCommandResults.get(i);
            table[i][0] = Utils.getURI(client.getScheme(), obj.getBucketName(), "") + obj.getKey();
            table[i][1] = obj.getVersionId().orElse("No Version Id");
            if (obj.getTimestamp().isPresent()) {
              table[i][2] = df.format(obj.getTimestamp().get());
            } else {
              table[i][2] = "Not applicable";
            }
            if (obj.getSize().isPresent()) {
              table[i][3] = obj.getSize().get().toString();
            } else {
              table[i][3] = "0";
            }
            for (int j = 0; j < 4; j++)
              max[j] = Math.max(table[i][j].length(), max[j]);
          }
          for (final String[] row : table) {
            System.out.format("%-" + (max[0] + 4) + "s%-" + (max[1] + 4) + "s%-" + (max[2] + 3)
                + "s%-" + (max[3] + 3) + "s\n", row[0], row[1], row[2], row[3]);
          }
        } else {
          for (StoreFile obj : listCommandResults) {
            System.out.println(Utils.getURI(client.getScheme(), obj.getBucketName(), "") + obj.getKey());
          }
        }
      } catch (ExecutionException exc) {
        rethrow(exc.getCause());
      }
      client.shutdown();
    }
  }
  
  @Parameters(commandDescription = "Delete objects from a storage service")
  class DeleteCommandOptions extends S3ObjectCommandOptions
  {
    @Parameter(names = {"-r", "--recursive"}, description = "Delete all objects" +
        " that match the provided storage service URL prefix.")
    boolean recursive = false;

    @Parameter(names = {"-f", "--force"}, description = "Do not report an error if the object does not exist")
    boolean forceDelete = false;

    @Parameter(names = "--dry-run", description = "Display operations but do not execute them")
    boolean dryRun = false;
    
    @Override
    public void invoke()
      throws Exception
    {
      if(recursive && !getObjectKey().endsWith("/"))
        throw new UsageException("Object key should end with / to recursively delete a directory structure");

      CloudStoreClient client = createCloudStoreClient();
      DeleteOptions opts = client.getOptionsBuilderFactory()
          .newDeleteOptionsBuilder()
          .setBucketName(getBucket())
          .setObjectKey(getObjectKey())
          .setRecursive(recursive)
          .setDryRun(dryRun)
          .setForceDelete(forceDelete)
          .createOptions();

      try
      {
        if(getObjectKey().endsWith("/"))
          client.deleteDir(opts).get();
        else
          client.delete(opts).get();
      }
      catch(ExecutionException exc)
      {
        // if UsageException is thrown from the command, rethrow that instead of the
        // wrapper exception to get cleaner error logging
        rethrow(exc.getCause());
      }
      client.shutdown();
    }
  }
  
  @Parameters(commandDescription = "List objects sizes in storage service")
  class DiskUsageCommandOptions extends S3ObjectCommandOptions {
    
    
    @Parameter(names = {
        "-d", "--max-depth"
    }, description = "Print sizes total for directories " + "with depth N")
    int maxDepth = 0;
    
    @Parameter(names = {
        "--all"
    }, description = "Print all files in directories " + "with depth N")
    boolean all = false;
    
    @Parameter(names = {
        "-H", "--human-readable-sizes"
    }, description = "Print sizes in human readable form " + "(eg 1kB instead of 1234)")
    boolean humanReadble = false;
    
    @Override
    public void invoke() throws Exception {
      CloudStoreClient client = createCloudStoreClient();
      ListOptionsBuilder lob = client.getOptionsBuilderFactory()
          .newListOptionsBuilder()
          .setBucketName(getBucket())
          .setObjectKey(getObjectKey())
          .setRecursive(true)
          .setIncludeVersions(false)
          .setExcludeDirs(false);
      long numberOfFiles = 0;
      long totalSize = 0;
      int baseDepth = getObjectKey().equals("") ? 1 : getObjectKey().split("/").length + 1;
      String du = null;
      TreeMap<String, DirectoryNode> dirs = new TreeMap<String, DirectoryNode>();
      try {
        List<StoreFile> result = client.listObjects(lob.createOptions()).get();
        for (StoreFile obj : result) {
          numberOfFiles += 1;
          totalSize += obj.getSize().orElse((long)0);
          if (maxDepth > 0) {
            String current = obj.getKey();
            String parent = findParent(current);
            long depth = current.split("/").length - baseDepth + 1;
            // add size to the parent Node if parent Node to be displayed
            if (0 <= depth - 1 && depth - 1 <= maxDepth) {
              DirectoryNode parentNode = dirs.get(parent);
              if (parentNode != null) {
                parentNode.size = parentNode.size + obj.getSize().orElse((long)0);
              } else {
                parentNode = new DirectoryNode(obj.getSize().orElse((long)0), parent);
                dirs.put(parent, parentNode);
              }
              // handle children if they were to be displayed
              if (depth <= maxDepth) {
                // if child node was a directory add them to the map of directories
                if (current.endsWith("/")) {
                  if (! dirs.containsKey(current)) {
                    current = current.substring(0, current.length() - 1);
                    DirectoryNode currentNode = new DirectoryNode(obj.getSize().orElse((long)0), current);
                    parentNode.childs.add(currentNode);
                    dirs.put(current, currentNode);
                  }
                }
                // else add file Node to children if all was enabled to be displayed
                else if (all) {
                  DirectoryNode currentNode = new DirectoryNode(obj.getSize().orElse((long)0), current);
                  parentNode.childs.add(currentNode);
                }
              }
            }
            // add size of current to all great Parents who will be displayed
            while (parent.length() > 0 && parent.split("/").length - baseDepth < maxDepth
                && 0 < parent.split("/").length - baseDepth) {
              parent = findParent(parent);
              DirectoryNode parentNode = dirs.get(parent);
              if (parentNode != null) {
                parentNode.size = parentNode.size + obj.getSize().orElse((long)0);
              } else {
                parentNode = new DirectoryNode(obj.getSize().orElse((long)0), parent);
                dirs.put(parent, parentNode);
              }
              depth--;
            }
          }
        }
        if (humanReadble) {
          du = getReadableString(totalSize);
        } else {
          du = Long.toString(totalSize);
        }
        System.out.format("%-15s %d objects %s %n", du, numberOfFiles, getURI().toString());
        if (dirs.size() > 0) {
          printTree(dirs, humanReadble, all, getObjectKey());
        }
      } catch (ExecutionException exc) {
        rethrow(exc.getCause());
      } finally {
        client.shutdown();
      }
    }
  }

  public String findParent(String current)
  {
    if (current.endsWith("/")) {
      current = current.substring(0, current.length() - 1);
    }
    int endIndex = current.lastIndexOf('/');
    return current.substring(0, endIndex == -1 ? 0 : endIndex);
  }
  
  public void printTree( Map<String, DirectoryNode> map, boolean humanReadble,
      boolean all, String root) {
    ArrayList <String[]> table = new ArrayList <String[]>();
    int[] max = new int[2];
    int tableCounter =0;
    for (Map.Entry<String, DirectoryNode> entry : map.entrySet()) {
      String size = "";
      if (! entry.getKey().equals(root)) {// skip root info
        if (humanReadble) {
          size = getReadableString(entry.getValue().size);
        } else {
          size = Long.toString(entry.getValue().size);
        }
        String [] row = {size, "/" + entry.getValue().fileName + "/"};
        table.add(tableCounter,row);
        for (int j = 0; j < 2; j++)
          max[j] = Math.max(table.get(tableCounter)[j].length(), max[j]);
        tableCounter ++;
      }
      if (all) {
        for (DirectoryNode n : entry.getValue().childs) {
          if (map.containsKey(n.fileName))
            continue;
          if (humanReadble) {
            size = getReadableString(n.size);
          } else {
            size = Long.toString(n.size);
          }
          String [] row = {size, "/" + n.fileName};
          table.add(tableCounter,row );
          for (int j = 0; j < 2; j++)
            max[j] = Math.max(table.get(tableCounter)[j].length(), max[j]);
          tableCounter ++;
        }
      }
    }
    for (final String[] row : table) {
      System.out.format("%-" + (max[0] + 4) + "s%-" + (max[1] + 4) + "s\n", row[0], row[1]);
    }
  }
  
  private static final String[] units = new String[] {
      "", "K", "M", "G", "T", "P", "E"
  };
  
  public String getReadableString(long bytes) {
    for (int i = 6; i > 0; i--) {
      double step = Math.pow(1024, i);
      if (bytes > step) {
        return new DecimalFormat("#,##0.##").format(bytes / step) + units[i];
      }
    }
    return Long.toString(bytes);
  }
  
  @Parameters(commandDescription = "List pending uploads")
  class ListPendingUploadsCommandOptions extends S3ObjectCommandOptions
  {
    @Override
    public void invoke() throws Exception
    {
      CloudStoreClient client = createCloudStoreClient();

      try
      {
        PendingUploadsOptions options = client.getOptionsBuilderFactory()
          .newPendingUploadsOptionsBuilder()
          .setBucketName(getBucket())
          .setObjectKey(getObjectKey())
          .createOptions();
        List<Upload> pendingUploads = client.listPendingUploads(options).get();

        Collections.sort(pendingUploads, new Comparator<Upload>(){
          public int compare(Upload u1, Upload u2) {
            return (u1.getBucket() + '/' + u1.getKey()).toLowerCase()
                .compareTo((u2.getBucket() + '/' + u2.getKey()).toLowerCase());
          }});

        String[][] table = new String[pendingUploads.size()][3];
        int[] max = new int[3];

        DateFormat df = Utils.getDefaultDateFormat();

        for(int i = 0; i < pendingUploads.size(); i++)
        {
          Upload u = pendingUploads.get(i);
          table[i][0] = Utils.getURI(client.getScheme(), u.getBucket(), u.getKey()).toString();
          table[i][1] = u.getId();
          table[i][2] = df.format(u.getInitiationDate());

          for(int j = 0; j < 3; j++)
            max[j] = Math.max(table[i][j].length(), max[j]);
        }

        for (final String[] row : table)
        {
          System.out.format("%-" + (max[0] + 3) + "s%-" + (max[1] + 3) +
              "s%-" + (max[2] + 3) + "s\n", row[0], row[1], row[2]);
        }
      }
      catch(ExecutionException exc)
      {
        rethrow(exc.getCause());
      }
     finally
      {
        client.shutdown();
      }
    }
  }

  @Parameters(commandDescription = "Abort pending uploads, either by id or " +
      "date/datetime. When date/datetime is specified the URL can be a prefix.")
  class AbortPendingUploadsCommandOptions extends S3ObjectCommandOptions
  {
    @Parameter(names = "--id", description = "Id of the pending upload to " +
        "abort")
    String id;

    @Parameter(names = "--date", description = "Pending uploads older " +
        "than this date are going to be aborted. Date has to be in ISO " +
        "8601 format \"yyyy-MM-dd\", UTC. Example: " +
        "\"2015-02-20\"")
    String dateStr;

    @Parameter(names = "--datetime", description = "Pending uploads older " +
        "than this datetime are going to be aborted. Datetime has to be in " +
        "ISO 8601 format \"yyyy-MM-dd HH:mm\", UTC. Example: " +
        "\"2015-02-20 19:31:51\"")
    String dateTimeStr;

    @Override
    public void invoke() throws Exception
    {
      CloudStoreClient client = createCloudStoreClient();

      try
      {
        if (id != null && dateTimeStr != null && dateStr != null)
        {
          throw new UsageException("id and/or date/datetime should be specified");
        }
        if (dateTimeStr != null && dateStr != null)
        {
          throw new UsageException("Only one of date and datetime " +
              "options can be specified");
        }

        Date date = null;
        if (dateStr != null) {
          DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
          df.setTimeZone(TimeZone.getTimeZone("UTC"));
          date = df.parse(dateStr);
        }
        else if (dateTimeStr != null) {
          DateFormat df = Utils.getDefaultDateFormat();
          date = df.parse(dateTimeStr);
        }
        PendingUploadsOptions options = client.getOptionsBuilderFactory()
          .newPendingUploadsOptionsBuilder()
          .setBucketName(getBucket())
          .setObjectKey(getObjectKey())
          .setUploadId(id)
          .setDate(date)
          .createOptions();
        client.abortPendingUploads(options).get();
      }
      catch(ExecutionException exc)
      {
        rethrow(exc.getCause());
      }
      finally
      {
        client.shutdown();
      }
    }
  }

  @Parameters(commandDescription = "Generates a public/private keypair in PEM format")
  class KeyGenCommandOptions extends CommandOptions
  {
    @Parameter(names = {"-n", "--name"}, description = "Name of the PEM file", required = true)
    String name = null;

    @Parameter(names = "--keydir", description = "Directory where PEM file will be stored")
    String encKeyDirectory = Utils.getDefaultKeyDirectory();

    @Override
    public void invoke() throws Exception
    {
      try
      {
        String pemfp = name + ".pem";
        File pemf = new File(encKeyDirectory, pemfp);
        if(pemf.exists()) {
          System.err.println("File " + pemf.getPath() + " already exists.");
          System.exit(1);
        }

        KeyGenCommand kgc = new KeyGenCommand("RSA", 2048);
        kgc.savePemKeypair(pemf);
      }
      catch(Exception exc)
      {
        rethrow(exc.getCause());
      }
    }
  }

  @Parameters(commandDescription = "Download a file, or a set of files from the storage service")
  class DownloadCommandOptions extends S3ObjectCommandOptions
  {
    @Parameter(names = "-o", description = "Write output to file, or directory")
    String file = System.getProperty("user.dir");

    @Parameter(names = "--overwrite", description = "Overwrite existing file(s) if existing")
    boolean overwrite = false;

    @Parameter(names = {"-r", "--recursive"}, description = "Download recursively")
    boolean recursive = false;
    
    @Parameter(names = {"--version-id"}, description = "Download a specific version of a file")
    String version = null;

    @Parameter(names = "--progress", description = "Enable progress indication")
    boolean progress = false;

    @Parameter(names = "--dry-run", description = "Display operations but do not execute them")
    boolean dryRun = false;

    @Override
    public void invoke() throws Exception
    {
      CloudStoreClient client = createCloudStoreClient();

      File output = new File(file);
      ListenableFuture<?> result;

      DownloadOptionsBuilder dob = client.getOptionsBuilderFactory()
          .newDownloadOptionsBuilder()
          .setFile(output)
          .setBucketName(getBucket())
          .setObjectKey(getObjectKey())
          .setRecursive(recursive)
          .setVersion(version)
          .setOverwrite(overwrite)
          .setDryRun(dryRun);

      if (progress) {
          OverallProgressListenerFactory cplf = new
              ConsoleProgressListenerFactory();
          dob.setOverallProgressListenerFactory(cplf);
      }

      if(getObjectKey().endsWith("/") || getObjectKey().equals("")) {
        result = client.downloadDirectory(dob.createOptions());
      } else {
        if (output.isDirectory())
          output = new File(output,
              getObjectKey().substring(getObjectKey().lastIndexOf("/")+1));
        dob.setFile(output);
        result = client.download(dob.createOptions());
      }

      try
      {
        result.get();
      }
      catch(ExecutionException exc)
      {
        rethrow(exc.getCause());
      }

      client.shutdown();
    }
  }

  @Parameters(commandDescription = "Add new encryption key")
  class AddEncryptionKeyCommandOptions extends S3ObjectCommandOptions
  {
    @Parameter(names = "--key",
      description = "The name of the encryption key to add",
      required = true)
    String encKeyName = null;

    public void invoke() throws Exception
    {
      CloudStoreClient client = createCloudStoreClient();
      try
      {
        if(getObjectKey().endsWith("/") || getObjectKey().equals(""))
        {
          throw new UsageException("Invalid object key " + getURI());
        }
        else
        {
          ExistsOptions opts = client.getOptionsBuilderFactory()
            .newExistsOptionsBuilder()
            .setBucketName(getBucket())
            .setObjectKey(getObjectKey())
            .createOptions();

          if (client.exists(opts).get() == null)
          {
            throw new UsageException("Object not found at " + getURI());
          }
          EncryptionKeyOptions options = client.getOptionsBuilderFactory()
            .newEncryptionKeyOptionsBuilder()
            .setBucketName(getBucket())
            .setObjectKey(getObjectKey())
            .setEncryptionKey(encKeyName)
            .createOptions();

          client.addEncryptionKey(options).get();
        }
      }
      catch(ExecutionException exc)
      {
        rethrow(exc.getCause());
      }
      finally
      {
        client.shutdown();
      }
    }
  }

  @Parameters(commandDescription = "Remove existing encryption key")
  class RemoveEncryptionKeyCommandOptions extends S3ObjectCommandOptions
  {
    @Parameter(names = "--key",
      description = "The name of the encryption key to remove",
      required = true)
    String encKeyName = null;

    public void invoke() throws Exception
    {
      CloudStoreClient client = createCloudStoreClient();
      try
      {
        if(getObjectKey().endsWith("/") || getObjectKey().equals(""))
        {
          throw new UsageException("Invalid object key " + getURI());
        }
        else
        {
          ExistsOptions opts = client.getOptionsBuilderFactory()
            .newExistsOptionsBuilder()
            .setBucketName(getBucket())
            .setObjectKey(getObjectKey())
            .createOptions();

          if (client.exists(opts).get() == null)
          {
            throw new UsageException("Object not found at " + getURI());
          }
          EncryptionKeyOptions options = client.getOptionsBuilderFactory()
            .newEncryptionKeyOptionsBuilder()
            .setBucketName(getBucket())
            .setObjectKey(getObjectKey())
            .setEncryptionKey(encKeyName)
            .createOptions();

          client.removeEncryptionKey(options).get();
        }
      }
      catch(ExecutionException exc)
      {
        rethrow(exc.getCause());
      }
      finally
      {
        client.shutdown();
      }
    }
  }

  /**
   * Version
   */
  @Parameters(commandDescription = "Print version")
  class VersionCommand extends CommandOptions
  {
    public void invoke()
    {
      System.out.println(S3Client.version());
    }
  }
  
  /**
   * Help
   */
  @Parameters(commandDescription = "Print usage")
  class HelpCommand extends CommandOptions
  {
    @Parameter(description = "Commands")
    List<String> _commands;

    public void invoke()
    {
      if(_commands == null)
        printUsage();
      else
      {
        for(String cmd : _commands)
        {
          printCommandUsage(cmd);
        }
      }
    }
  }

  public void execute(String[] args)
  {
    try
    {
      _commander.parse(args);
      String command = _commander.getParsedCommand();
      if(command != null)
      {
        CommandOptions cmd = (CommandOptions) _commander.getCommands().get(command).getObjects().get(0);
        if(cmd.help)
        {
          printCommandUsage(command);
          System.exit(1);
        }

        cmd.invoke();
      }
      else
      {
        printUsage();
      }
    }
    catch(ParameterException exc)
    {
      System.err.println("error: " + exc.getMessage());
      System.err.println("");
      printUsage();
      System.exit(1);
    }
    catch(UsageException exc)
    {
      System.err.println("error: " + exc.getMessage());
      System.exit(1);
    }
    catch(AmazonServiceException exc)
    {
      if(exc.getStatusCode() == 404)
      {
        System.err.println("error: Storage service object not found: " +
            exc.getMessage());
      }
      else if(exc.getStatusCode() == 403)
      {
        System.err.println("error: Access to storage service object denied " +
            "with current credentials: " + exc.getMessage());
      }
      else
      {
        System.err.println("error: " + exc.getMessage());
        exc.printStackTrace();
      }

      System.exit(1);
    }
    catch(UnsupportedOperationException exc)
    {
      System.err.println("error: " + exc.getMessage());
      System.exit(1);
    }
    catch(Exception exc)
    {
      System.err.println("error: " + exc.getMessage());
      System.err.println("");
      exc.printStackTrace();
      System.exit(1);
    }
  }

  private void printOptions()
  {
    // Hack to avoid printing the commands, which are not formatted
    // correctly.
    JCommander tmp = new JCommander(new MainCommand());
    tmp.setProgramName("cloud-store");

    // Hack to avoid printing the usage line, which is not correct in
    // this incomplete commander object.
    StringBuilder builder = new StringBuilder();
    tmp.usage(builder);
    String usage = builder.toString();
    String options = usage.substring(usage.indexOf('\n'));
    System.err.println(options);
  }

  private void printUsage()
  {
    System.err.println("Usage: cloud-store [options] command [command options]");
    printOptions();

    System.err.println("   Commands: ");
    for(String cmd : _commander.getCommands().keySet())
    {
      String indentStr = "     ";
      int padding = 23;
      int column = 79;

      String wrapDesc = wrapDescription(indentStr.length() + padding,
          _commander.getCommandDescription(cmd), column);
      System.out.println(indentStr + padRight(padding, ' ', cmd) + wrapDesc);
    }
  }

  private String wrapDescription(int indent, String description, int
      columnSize) {
    StringBuilder out = new StringBuilder();
    String[] words = description.split(" ");
    int current = indent;
    int i = 0;
    while (i < words.length) {
      String word = words[i];
      if (word.length() > columnSize || current + word.length() < columnSize) {
        out.append(" ").append(word);
        current += word.length() + 1;
      } else {
        out.append("\n").append(spaces(indent + 1)).append(word);
        current = indent + 1 + word.length();
      }
      i++;
    }
    return out.toString();
  }

  private String spaces(int indent) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < indent; i++) sb.append(" ");
    return sb.toString();
  }

  private static String padRight(int width, char c, String s)
  {
    StringBuffer buf = new StringBuffer(width);
    buf.append(s);
    for(int i = 0; i < width - s.length(); i++)
      buf.append(c);
    return buf.toString();
  }

  private void printCommandUsage(String command)
  {
    StringBuilder builder = new StringBuilder();
    _commander.usage(command, builder);
    System.err.println(builder.toString());
  }

  private static void rethrow(Throwable thrown) throws Exception
  {
    if(thrown instanceof Exception)
      throw (Exception) thrown;
    if(thrown instanceof Error)
      throw (Error) thrown;
    else
      throw new RuntimeException(thrown);
  }
}
