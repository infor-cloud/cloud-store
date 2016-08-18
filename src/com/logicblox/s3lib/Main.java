package com.logicblox.s3lib;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import com.amazonaws.AmazonServiceException;

class Main
{
  JCommander _commander = new JCommander();

  public static void main(String[] args)
  {
    initLogging();

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

  private static void initLogging() {
    Logger root = Logger.getRootLogger();
    root.setLevel(Level.INFO);

    ConsoleAppender console = new ConsoleAppender();
    String PATTERN = "%d [%p|%c|%C{1}] %m%n";
    console.setLayout(new PatternLayout(PATTERN));
    console.setThreshold(Level.ERROR);
    console.activateOptions();

    Logger s3libLogger = Logger.getLogger("com.logicblox.s3lib");
    s3libLogger.addAppender(console);
    Logger awsLogger = Logger.getLogger("com.amazonaws");
    awsLogger.addAppender(console);
    Logger apacheLogger = Logger.getLogger("org.apache.http");
    apacheLogger.addAppender(console);
  }

  public Main()
  {
    _commander = new JCommander(new MainCommand());
    _commander.setProgramName("cloud-store");
    _commander.addCommand("upload", new UploadCommandOptions());
    _commander.addCommand("download", new DownloadCommandOptions());
    _commander.addCommand("copy", new CopyCommandOptions());
    _commander.addCommand("ls", new ListCommandOptions());
    _commander.addCommand("list-pending-uploads", new
        ListPendingUploadsCommandOptions());
    _commander.addCommand("abort-pending-uploads", new
        AbortPendingUploadsCommandOptions());
    _commander.addCommand("exists", new ExistsCommandOptions());
    _commander.addCommand("list-buckets", new ListBucketsCommandOptions());
    _commander.addCommand("add-encrypted-key", new
      AddEncryptedKeyCommandOptions());
    _commander.addCommand("remove-encrypted-key", new
      RemoveEncryptedKeyCommandOptions());
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
    int maxConcurrentConnections = 10;

    @Parameter(names = "--endpoint", description = "Endpoint")
    String endpoint = null;

    @Parameter(names = "--keydir", description = "Directory where encryption keys are found")
    String encKeyDirectory = Utils.getDefaultKeyDirectory();

    @Parameter(names = "--stubborn", description = "Retry client exceptions (e.g. file not found and authentication errors)")
    boolean _stubborn = false;

    @Parameter(names = "--retry", description = "Number of retries on failures")
    int _retryCount = 10;

    @Parameter(names = {"--chunk-size"}, description = "The size of each chunk read from the file")
    long chunkSize = Utils.getDefaultChunkSize();

    @Parameter(names = {"--credential-providers-s3"}, description = "The " +
        "order of the credential providers that should be checked for S3. The" +
        " default order is: \"env-vars\", " + "\"system-properties\", " +
        "\"credentials-profile\", \"ec2-metadata-service\". Choices should be" +
        " comma-separated without any spaces, e.g. \"env-vars," +
        "ec2-metadata-service\"." ,
        validateValueWith = CredentialProvidersValidator.class)
    List<String> credentialProvidersS3;

    protected Utils.StorageService detectStorageService() throws URISyntaxException
    {
      return Utils.detectStorageService(endpoint, null);
    }

    protected CloudStoreClient createCloudStoreClient()
        throws URISyntaxException, IOException, GeneralSecurityException {
      ListeningExecutorService uploadExecutor = Utils.getHttpExecutor
          (maxConcurrentConnections);

      Utils.StorageService service = detectStorageService();

      CloudStoreClient client;
      if (service == Utils.StorageService.GCS) {
        AWSCredentialsProvider gcsXMLProvider =
            Utils.getGCSXMLEnvironmentVariableCredentialsProvider();
        AmazonS3ClientForGCS s3Client = new AmazonS3ClientForGCS(gcsXMLProvider);

        client = new GCSClientBuilder()
            .setInternalS3Client(s3Client)
            .setApiExecutor(uploadExecutor)
            .setChunkSize(chunkSize)
            .setKeyProvider(Utils.getKeyProvider(encKeyDirectory))
            .createGCSClient();
      }
      else {
        ClientConfiguration clientCfg = new ClientConfiguration();
        clientCfg = Utils.setProxy(clientCfg);
        AWSCredentialsProvider credsProvider =
            Utils.getCredentialsProviderS3(credentialProvidersS3);
        AmazonS3Client s3Client = new AmazonS3Client(credsProvider, clientCfg);

        client = new S3ClientBuilder()
            .setInternalS3Client(s3Client)
            .setApiExecutor(uploadExecutor)
            .setChunkSize(chunkSize)
            .setKeyProvider(Utils.getKeyProvider(encKeyDirectory))
            .createS3Client();
      }

      client.setRetryClientException(_stubborn);
      client.setRetryCount(_retryCount);
      if(endpoint != null)
      {
        client.setEndpoint(endpoint);
      }

      return client;
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

    protected Utils.StorageService detectStorageService() throws URISyntaxException
    {
      return Utils.detectStorageService(endpoint, getURI());
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

    protected Utils.StorageService detectStorageService() throws URISyntaxException
    {
      return Utils.detectStorageService(endpoint, getSourceURI());
    }
  }

  @Parameters(commandDescription = "List storage service buckets")
  class ListBucketsCommandOptions extends S3CommandOptions
  {
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
      ListenableFuture<ObjectMetadata> result = client.exists(bucket, key);

      boolean exists = false;
      ObjectMetadata metadata = result.get();
      if(metadata == null)
      {
        if(_verbose)
          System.err.println("Object " + client.getUri(bucket, key) + " does " +
              "not exist.");
      }
      else
      {
        exists = true;

        if(_verbose)
          Utils.print(metadata);
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
        + S3Client.cannedACLsDescConst)
    String cannedAcl;

    @Parameter(names = {"-r", "--recursive"}, description = "Copy recursively")
    boolean recursive = false;

    public void invoke() throws Exception
    {
      if (cannedAcl == null)
      {
        cannedAcl = Utils.getDefaultCannedACLFor(detectStorageService());
      }

      if(!Utils.isValidCannedACLFor(detectStorageService(), cannedAcl))
      {
        throw new UsageException("Unknown canned ACL '" + cannedAcl + "'");
      }

      CloudStoreClient client = createCloudStoreClient();

      CopyOptions options = new CopyOptionsBuilder()
          .setSourceBucketName(getSourceBucket())
          .setSourceKey(getSourceObjectKey())
          .setDestinationBucketName(getDestinationBucket())
          .setDestinationKey(getDestinationObjectKey())
          .setCannedAcl(cannedAcl)
          .setRecursive(recursive)
          .createCopyOptions();

      try
      {
        // Check if destination bucket exists
        if (client.exists(getDestinationBucket(), "").get() == null)
        {
          throw new UsageException("Bucket not found at " +
              client.getUri(getDestinationBucket(), ""));
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
          // We go for a direct key-to-key copy, so source object has
          // to be there.
          if (client.exists(getSourceBucket(), getSourceObjectKey()).get() == null)
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

  @Parameters(commandDescription = "Upload a file or directory to the storage service")
  class UploadCommandOptions extends S3ObjectCommandOptions
  {
    @Parameter(names = "-i", description = "File or directory to upload", required = true)
    String file;

    @Parameter(names = "--key", description = "The name of the encryption key to use")
    String encKeyName = null;

    @Parameter(names = "--progress", description = "Enable progress indicator")
    boolean progress = false;

    @Parameter(names = "--canned-acl", description = "The canned ACL to use. "
        + S3Client.cannedACLsDescConst + " " + GCSClient.cannedACLsDescConst)
    String cannedAcl;

    public void invoke() throws Exception
    {
      if (cannedAcl == null)
      {
        cannedAcl = Utils.getDefaultCannedACLFor(detectStorageService());
      }

      if(!Utils.isValidCannedACLFor(detectStorageService(), cannedAcl))
      {
        throw new UsageException("Unknown canned ACL '" + cannedAcl + "'");
      }

      CloudStoreClient client = createCloudStoreClient();
      File f = new File(file);

      if (f.isDirectory() && !getObjectKey().endsWith("/")) {
        throw new UsageException("Destination key " +
            client.getUri(getBucket(), getObjectKey()) +
            " should end with '/', since a directory is uploaded.");
      }

      UploadOptionsBuilder uob = new UploadOptionsBuilder();
      uob.setFile(f)
          .setBucket(getBucket())
          .setObjectKey(getObjectKey())
          .setEncKey(encKeyName)
          .setAcl(cannedAcl);
      if (progress) {
        OverallProgressListenerFactory cplf = new
            ConsoleProgressListenerFactory();
        uob.setOverallProgressListenerFactory(cplf);
      }

      if(f.isFile()) {
        if (getObjectKey().endsWith("/"))
          uob.setObjectKey(getObjectKey() + f.getName());
        client.upload(uob.createUploadOptions()).get();
      } else if(f.isDirectory()) {
        client.uploadDirectory(uob.createUploadOptions()).get();
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
      ListOptionsBuilder lob = new ListOptionsBuilder()
          .setBucket(getBucket())
          .setObjectKey(getObjectKey())
          .setRecursive(recursive)
          .setIncludeVersions(includeVersions)
          .setExcludeDirs(excludeDirs);
      try {
        List<S3File> result = client.listObjects(lob.createListOptions()).get();
        for (S3File obj : result)
          System.out.println(client.getUri(obj.getBucketName(), obj.getKey()));
      } catch (ExecutionException exc) {
        rethrow(exc.getCause());
      }
      client.shutdown();
    }
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
        List<Upload> pendingUploads = client.listPendingUploads(getBucket(),
            getObjectKey()).get();

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
          table[i][0] = client.getUri(u.getBucket(), u.getKey()).toString();
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
        if (id != null && (dateTimeStr != null || dateStr != null)) {
          throw new UsageException("You can abort pending uploads either by " +
              "id or by date/datetime parameters");
        }
        if (dateTimeStr != null && dateStr != null) {
          throw new UsageException("Only one of date and datetime " +
              "parameters can be specified");
        }

        if (id != null) {
          client.abortPendingUpload(getBucket(), getObjectKey(), id).get();
        }
        else if (dateStr != null) {
          DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
          df.setTimeZone(TimeZone.getTimeZone("UTC"));
          Date date = df.parse(dateStr);

          client.abortOldPendingUploads(getBucket(), getObjectKey(), date).get();
        }
        else if (dateTimeStr != null) {
          DateFormat df = Utils.getDefaultDateFormat();
          Date date = df.parse(dateTimeStr);

          client.abortOldPendingUploads(getBucket(), getObjectKey(), date).get();
        }
        else {
          throw new UsageException("Either id or date/datetime parameters " +
              "should be specified");
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

    @Override
    public void invoke() throws Exception
    {
      CloudStoreClient client = createCloudStoreClient();

      File output = new File(file);
      ListenableFuture<?> result;

      DownloadOptionsBuilder dob = new DownloadOptionsBuilder()
          .setFile(output)
          .setBucket(getBucket())
          .setObjectKey(getObjectKey())
          .setRecursive(recursive)
          .setVersion(version)
          .setOverwrite(overwrite);

      if (progress) {
          OverallProgressListenerFactory cplf = new
              ConsoleProgressListenerFactory();
          dob.setOverallProgressListenerFactory(cplf);
      }

      if(getObjectKey().endsWith("/") || getObjectKey().equals("")) {
        result = client.downloadDirectory(dob.createDownloadOptions());
      } else {
        // Test if storage service url exists.
        if(client.exists(getBucket(), getObjectKey()).get() == null) {
          throw new UsageException("Object not found at "+getURI());
        }
        if (output.isDirectory())
          output = new File(output,
              getObjectKey().substring(getObjectKey().lastIndexOf("/")+1));
        dob.setFile(output);
        result = client.download(dob.createDownloadOptions());
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

  @Parameters(commandDescription = "Add new encrypted key")
  class AddEncryptedKeyCommandOptions extends S3ObjectCommandOptions
  {
    @Parameter(names = "--key",
      description = "The name of the encrypted key to add",
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
          if (client.exists(getBucket(), getObjectKey()).get() == null)
          {
            throw new UsageException("Object not found at " + getURI());
          }
          client.addEncryptedKey(getBucket(), getObjectKey(), encKeyName).get();
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

  @Parameters(commandDescription = "Remove existing encrypted key")
  class RemoveEncryptedKeyCommandOptions extends S3ObjectCommandOptions
  {
    @Parameter(names = "--key",
      description = "The name of the encrypted key to remove",
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
          if (client.exists(getBucket(), getObjectKey()).get() == null)
          {
            throw new UsageException("Object not found at " + getURI());
          }
          client.removeEncryptedKey(getBucket(), getObjectKey(),
            encKeyName).get();
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
