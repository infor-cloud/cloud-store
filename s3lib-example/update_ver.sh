#!/usr/bin/env bash

wget https://bob.logicblox.com/job/s3lib/default/binary_tarball/latest/download-by-type/file/tgz -O s3lib.tgz

# Find the directory embedded in the tarball
S3TOOL_DIR=`tar tfz s3lib.tgz | sed  "s/\/.*$//" | sort -u`

BUILD_NUM=`tar tfz s3lib.tgz | sed  "s/\/.*$//" | sort -u | awk -F'[-_]' '{print $2}'`

tar xfz s3lib.tgz

mv s3lib.tgz s3lib-${BUILD_NUM}.tgz

# Create a link for future convenience
ln -sf $S3TOOL_DIR s3tool
mvn install:install-file -Dfile=s3tool/lib/java/s3lib-0.2.jar -DgroupId=com.logicblox -DartifactId=s3lib -Dversion=0.2.${BUILD_NUM} -Dpackaging=jar -DgeneratePom=true

mvn versions:display-dependency-updates

# Do this to update the pom.xml file:
mvn versions:use-latest-versions -Dincludes=com.logicblox:s3lib
 
mvn versions:display-dependency-updates
 
# Then you should regenerate the eclipse classpath as well:
mvn eclipse:eclipse
