package com.logicblox.s3lib;

import org.junit.internal.TextListener;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
@Suite.SuiteClasses( { 
  BucketTests.class })
public class TestRunner
{
  public static void main(String[] args)
  {
    Utils.initLogging();
    TestOptions.parseArgs(args);
    JUnitCore core = new JUnitCore();
    core.addListener(new TextListener(System.out));
    core.run(TestRunner.class);
  }

}
