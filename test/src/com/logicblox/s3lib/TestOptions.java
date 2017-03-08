package com.logicblox.s3lib;

import java.net.URL;
import java.net.MalformedURLException;


public class TestOptions
{
  private static String _service = "s3";
  private static String _endpoint = null;


  private static void usage()
  {
    System.out.println("usage:  TestRunner {--help || -h} {--service s3|gs} {--endpoint url}");
  }


  public static void parseArgs(String[] args)
  {
// TODO - add options for port and host
    for(int i = 0; i < args.length; ++i)
    {
      if(args[i].equals("--help") || args[i].equals("-h"))
      {
        usage();
        System.exit(0);
      }
      else if(args[i].equals("--service"))
      {
        ++i;
        _service = args[i];
      }
      else if(args[i].equals("--endpoint"))
      {
        ++i;
        _endpoint = args[i];
      }
      else
      {
        System.out.println("Error:  '" + args[i] + "' unexpected");
        System.exit(1);
      }
    }

    if(!_service.equals("s3") && !_service.equals("gs"))
    {
      System.out.println("Error:  --service must be s3 or gs");
      System.exit(1);
    }
  }


  public static URL getEndpoint()
    throws MalformedURLException
  {
    if(null == _endpoint)
      return null;
    else
      return new URL(_endpoint);
  }


  public static String getEndpointString()
    throws MalformedURLException
  {
    if(null == _endpoint)
      return null;
    else
      return getEndpoint().toString();
  }


  public static String getService()
  {
    return _service;
  }


  public static String getServiceDescription()
  {
    String ep = _endpoint;
    if((null == ep) && _service.equals("s3"))
      ep = "AWS";
    else if((null == ep) && _service.equals("gs"))
      ep = "GCS";
    return _service + " (" + ep + ")";
  }
}
