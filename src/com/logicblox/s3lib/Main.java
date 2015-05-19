package com.logicblox.s3lib;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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
    console.setThreshold(Level.INFO);
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
    _commander.addCommand("exists", new ExistsCommandOptions());
    _commander.addCommand("list-buckets", new ListBucketsCommandOptions());
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
   * Abstraction for all S3 commands
   */
  abstract class S3CommandOptions extends CommandOptions
  {
    @Parameter(names = {"--max-concurrent-connections"}, description = "The maximum number of concurrent HTTP connections to S3")
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
        AWSCredentialsProvider gcsXMLProvider = Utils
            .getGCSXMLEnvironmentVariableCredentialsProvider();
        AmazonS3Client s3Client = new AmazonS3Client(gcsXMLProvider);

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
        AmazonS3Client s3Client = new AmazonS3Client(clientCfg);

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


  /**
   * Abstraction for commands that deal with S3 objects
   */
  abstract class S3ObjectCommandOptions extends S3CommandOptions
  {
    @Parameter(description = "S3URL", required = true)
    List<String> urls;

    protected URI getURI() throws URISyntaxException
    {
      if(urls.size() != 1)
        throw new UsageException("A single S3 object URL is required");

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

  @Parameters(commandDescription = "List S3 buckets")
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

      TimeZone tz = TimeZone.getTimeZone("UTC");
      DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
      df.setTimeZone(tz);

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

  @Parameters(commandDescription = "Check if a file exists in S3")
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

  @Parameters(commandDescription = "Copy object")
  class CopyCommandOptions extends TwoObjectsCommandOptions
  {
    @Parameter(names = "--progress", description = "Enable progress indicator")
    boolean progress = false;

    @Parameter(names = "--canned-acl", description = "The canned ACL to use. "
        + S3Client.cannedACLsDescConst)
    String cannedAcl;

    // @Parameter(names = {"-r", "--recursive"}, description = "Copy
    // recursively")
    //boolean recursive = false;

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

      CopyOptionsBuilder cob = new CopyOptionsBuilder();
      cob.setSourceBucketName(getSourceBucket())
          .setSourceKey(getSourceObjectKey())
          .setDestinationBucketName(getDestinationBucket())
          .setDestinationKey(getDestinationObjectKey())
          .setCannedAcl(cannedAcl)
          .setRecursive(false);
      if (progress) {
        OverallProgressListenerFactory cplf = new
            ConsoleProgressListenerFactory();
        cob.setOverallProgressListenerFactory(cplf);
      }
      CopyOptions options = cob.createCopyOptions();

      try
      {
        // TODO(geokollias): Add support for prefix/directory copy
        if(getSourceObjectKey().endsWith("/"))
        {
          throw new UsageException("Copy of prefixes is not supported yet: " +
              getSourceURI());
        }
        if(client.exists(getSourceBucket(), getSourceObjectKey()).get() == null)
        {
          throw new UsageException("Object not found at " + getSourceURI());
        }

        client.copy(options).get();
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

  @Parameters(commandDescription = "Upload a file or directory to S3")
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

      if (getObjectKey().endsWith("/")) {
        throw new UsageException("Destination key " + getBucket() + "/" +
            getObjectKey() +
            " should be fully qualified. No trailing '/' is permitted.");
      }

      CloudStoreClient client = createCloudStoreClient();
      File f = new File(file);

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
      UploadOptions options = uob.createUploadOptions();

      if(f.isFile()) {
        client.upload(options).get();
      } else if(f.isDirectory()) {
        client.uploadDirectory(options).get();
      } else {
        throw new UsageException("File '" + file + "' is not a file or a " +
            "directory.");
      }
      client.shutdown();
    }
  }

  @Parameters(commandDescription = "List objects in S3")
  class ListCommandOptions extends S3ObjectCommandOptions
  {
    @Parameter(names = {"-r", "--recursive"}, description = "List all objects that match the provided S3 URL prefix.")
    boolean recursive = false;

    @Parameter(names = {"--include-dirs"}, description = "List all objects and (first-level) directories that match the provided S3 URL prefix.")
    boolean include_dirs = false;

    @Override
    public void invoke() throws Exception
    {
      CloudStoreClient client = createCloudStoreClient();

      try
      {
        if (include_dirs)
        {
          List<S3File> result = client.listObjectsAndDirs(getBucket(), getObjectKey(), recursive).get();

          for (S3File obj : result)
          {
            // print the full s3 url for each object and (first-level) directory
            System.out.println(client.getUri(obj.getBucketName(), obj.getKey()));
          }
        }
        else
        {
          List<S3ObjectSummary> result = client.listObjects(getBucket(), getObjectKey(), recursive).get();

          for (S3ObjectSummary obj : result)
          {
            // print the full s3 url for each object
            System.out.println(client.getUri(obj.getBucketName(), obj.getKey()));
          }
        }
      }
      catch(ExecutionException exc)
      {
        rethrow(exc.getCause());
      }

      client.shutdown();
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

  @Parameters(commandDescription = "Download a file, or a set of files from S3")
  class DownloadCommandOptions extends S3ObjectCommandOptions
  {
    @Parameter(names = "-o", description = "Write output to file, or directory", required = true)
    String file;

    @Parameter(names = "--overwrite", description = "Overwrite existing file(s) if existing")
    boolean overwrite = false;

    @Parameter(names = {"-r", "--recursive"}, description = "Download recursively")
    boolean recursive = false;

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
          .setOverwrite(overwrite);

      if (progress) {
          OverallProgressListenerFactory cplf = new
              ConsoleProgressListenerFactory();
          dob.setOverallProgressListenerFactory(cplf);
      }

      DownloadOptions options = dob.createDownloadOptions();

      if(getObjectKey().endsWith("/")) {
        result = client.downloadDirectory(options);
      } else {
        // Test if S3 url exists.
        if(client.exists(getBucket(), getObjectKey()).get() == null) {
          throw new UsageException("Object not found at "+getURI());
        }
        result = client.download(options);
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
        System.err.println("error: S3 object not found");
      }
      else if(exc.getStatusCode() == 403)
      {
        System.err.println("error: Access to S3 object denied with current credentials");
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
      System.out.println("     " + padRight(15, ' ', cmd) + _commander.getCommandDescription(cmd));
    }
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
