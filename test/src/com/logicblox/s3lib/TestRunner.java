/*
  Copyright 2017, Infor Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.logicblox.s3lib;

import org.junit.internal.TextListener;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
@Suite.SuiteClasses( { 
//  BucketTests.class,
        // these only work with minio and with AWS if you have the right creds
  UploadDownloadTests.class,
  CopyTests.class,
  DeleteTests.class,
  RenameTests.class,
  MultiKeyTests.class
 })
public class TestRunner
{
  public static void main(String[] args)
  {
    Utils.initLogging();
    TestUtils.parseArgs(args);
    System.out.println(
      "Running tests against service " + TestUtils.getServiceDescription());
    JUnitCore core = new JUnitCore();
    core.addListener(new TextListener(System.out));
    Result r = core.run(TestRunner.class);
    System.exit(r.getFailureCount());
  }

}
