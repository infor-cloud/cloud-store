Sample program to show how s3lib can be integrated into customer applications.

Project was created by running
   mvn archetype:create -DgroupId=com.logicblox -DartifactId=s3lib-example
and then updating pom.xml with immediate level dependencies.


To load a new s3lib*.jar file into the local repo, something like: 
   mvn install:install-file -Dfile=s3lib-257_5c669378aac4/lib/java/s3lib-0.2.jar -DgroupId=com.logicblox -DartifactId=s3lib -Dversion=0.2 -Dpackaging=jar -DgeneratePom=true

To check dependency versions:
   mvn versions:display-dependency-updates


