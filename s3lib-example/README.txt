Sample program to show how s3lib can be integrated into customer applications.

Project was created by running
   mvn archetype:create -DgroupId=com.logicblox -DartifactId=s3lib-example
and then updating pom.xml with immediate level dependencies.


To load a new s3lib*.jar file into the local repo, something like:

   wget https://bob.logicblox.com/job/s3lib/default/binary_tarball/latest/download-by-type/file/tgz -O s3lib.tgz
   tar xfz s3lib.tgz
   mvn install:install-file -Dfile=s3lib-267_43193082ee6a/lib/java/s3lib-0.2.jar -DgroupId=com.logicblox -DartifactId=s3lib -Dversion=0.2 -Dpackaging=jar -DgeneratePom=true

To check dependency versions:
   mvn versions:display-dependency-updates


To compile and run at the command line:
   mvn -q clean compile exec:java -Dexec.mainClass=com.logicblox.S3downloader

