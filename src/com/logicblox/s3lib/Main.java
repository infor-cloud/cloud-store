package com.logicblox.s3lib;

import java.util.List;
import java.io.File;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Main
{
  JCommander _commander = new JCommander();

  public static void main(String[] args)
  {
    Logger root = Logger.getRootLogger();
    root.setLevel(Level.INFO);

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
    // TODO
    // System.exit(0);
  }

  public Main()
  {
    _commander = new JCommander(new MainCommand());
    _commander.setProgramName("s3tool");
    _commander.addCommand("upload", new UploadCommandOptions());
    _commander.addCommand("download", new DownloadCommandOptions());
    _commander.addCommand("help", new HelpCommand());
  }

  class MainCommand
  {
    @Parameter(names = { "-h", "--help" }, description = "Print usage information")
    boolean help = false;
  }

  abstract class Command
  {
    @Parameter(names = { "-h", "--help" }, description = "Print usage information")
    boolean help = false;

    public abstract void invoke() throws Exception;
  }

  abstract class S3Command extends Command
  {
    @Parameter(names = {"--bucket"}, description = "Name of S3 bucket", required = true)
    String bucket;

    @Parameter(names = {"--key"}, description = "Name of the S3 object (relative to the bucket)", required = true)
    String key;

    @Parameter(names = {"--max-concurrent-connections"}, description = "The maximum number of concurrent HTTP connections to S3")
    int maxConcurrentConnections = 10;

    @Parameter(names = "--enc-key-file", description = "The file where the encryption keys are found")
    String encKeyFile = System.getProperty("user.home") + File.separator + ".s3lib-enc-keys";
  }

  @Parameters(commandDescription = "Upload a file to S3")
  class UploadCommandOptions extends S3Command
  {
    @Parameter(names = {"-f", "--file"}, description = "File to upload", required = true)
    String file;

    @Parameter(names = {"--chunk-size"}, description = "The size of each chunk read from the file")
    long chunkSize = 10 * 1024 * 1024;

    @Parameter(names = "--enc-key-name", description = "The name of the encryption key to use", required = true)
    String encKeyName;

    public void invoke()
    {
      new UploadCommand(new File(file), chunkSize, encKeyName, new File(encKeyFile)).run(bucket, key, maxConcurrentConnections);
    }
  }

  @Parameters(commandDescription = "Download a file from S3")
  class DownloadCommandOptions extends S3Command
  {
    @Parameter(names = {"-f", "--file"}, description = "File to upload", required = true)
    String file;

    @Override
    public void invoke()
    {
      new DownloadCommand(new File(file)).run(bucket, key, maxConcurrentConnections, new File(encKeyFile));
    }
  }

  /**
   * Help
   */
  @Parameters(commandDescription = "Print usage")
  class HelpCommand extends Command
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
        Command cmd = (Command) _commander.getCommands().get(command).getObjects().get(0);
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
    tmp.setProgramName("s3tool");

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
    System.err.println("Usage: s3tool [options] command [command options]");
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
}
