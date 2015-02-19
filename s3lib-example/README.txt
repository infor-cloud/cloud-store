Sample program to show how s3lib can be integrated into customer applications.

Project was created by running
   mvn archetype:create -DgroupId=com.logicblox -DartifactId=s3lib-example
and then updating pom.xml with immediate level dependencies.


To load a new s3lib*.jar file into the local repo, do something like:

   wget https://bob.logicblox.com/job/s3lib/default/binary_tarball/latest/download-by-type/file/tgz -O s3lib.tgz
   tar xfz s3lib.tgz
   ln -s s3lib-279_641791076f3c/ s3tool
   mvn install:install-file -Dfile=s3tool/lib/java/s3lib-0.2.jar -DgroupId=com.logicblox -DartifactId=s3lib -Dversion=0.2.272 -Dpackaging=jar -DgeneratePom=true
   (Note that the build number is being included as the revision section of the JAR's version number.)

   # Do this to update the pom.xml file:
   mvn versions:use-latest-versions -Dincludes=com.logicblox:s3lib

   # Then you should regenerate the eclipse classpath as well:
   mvn eclipse:eclipse


Confirm downloads work using the command line utility, s3lib:
  ./s3tool/bin/s3tool download -o test.gz s3://kiabi-fred-dev/test/test.gz

To compile and run at the command line:
   mvn -q clean compile exec:java -Dexec.mainClass=com.logicblox.S3downloader
   zcat test.gz

Note that the s3upload class uses an encryption key called kiabi-dev for now and
expects to find it under /home/.s3lib-keys
  mvn -q clean compile exec:java -Dexec.mainClass=com.logicblox.S3uploader

Get a proxy running in your local machine
   sudo apt-get install Privoxy
   When you install it, it'll bind to 8118 automatically

To setup a proxy connection, check the Proxy example
  These two lines setup the proxy:
    clientCfg.setProxyHost("localhost");
    clientCfg.setProxyPort(8118);
  We can add them to either the s3dowbloader or s3uploader main.


To check dependency versions:
    k

To generate project reports:
   mvn site
   xdg-open target/site/index.html
