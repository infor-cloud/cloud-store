#! /bin/bash

jarsdir='/usr/local/lib/java'
mkdir -p $jarsdir
cd $jarsdir

function download {
    echo "downloading $1"
    wget -q --show-progress --no-directories -r -N -l inf $2 -O $1
}

download "jackson-annotations-2.10.4.jar" https://search.maven.org/remotecontent?filepath=com/fasterxml/jackson/core/jackson-annotations/2.10.4/jackson-annotations-2.10.4.jar
download "commons-codec-1.9.jar" http://search.maven.org/remotecontent?filepath=commons-codec/commons-codec/1.9/commons-codec-1.9.jar
download "jackson-core-2.10.4.jar" https://search.maven.org/remotecontent?filepath=com/fasterxml/jackson/core/jackson-core/2.10.4/jackson-core-2.10.4.jar
download "jackson-databind-2.10.4.jar" https://search.maven.org/remotecontent?filepath=com/fasterxml/jackson/core/jackson-databind/2.10.4/jackson-databind-2.10.4.jar

download "istack-commons-runtime-3.0.11.jar" https://repo1.maven.org/maven2/com/sun/istack/istack-commons-runtime/3.0.11/istack-commons-runtime-3.0.11.jar
download "guava-15.0.jar" http://search.maven.org/remotecontent?filepath=com/google/guava/guava/15.0/guava-15.0.jar
download "jaxb-api-2.3.1.jar" https://repo1.maven.org/maven2/javax/xml/bind/jaxb-api/2.3.1/jaxb-api-2.3.1.jar
download "jaxb-runtime-2.3.1.jar" https://repo1.maven.org/maven2/org/glassfish/jaxb/jaxb-runtime/2.3.1/jaxb-runtime-2.3.1.jar
download "jcommander-1.29.jar" http://search.maven.org/remotecontent?filepath=com/beust/jcommander/1.29/jcommander-1.29.jar
download "junit-4.8.2.jar" https://repo1.maven.org/maven2/junit/junit/4.8.2/junit-4.8.2.jar
download "log4j-core-2.17.1.jar" https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-core/2.17.1/log4j-core-2.17.1.jar
download "log4j-api-2.17.1.jar" https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-api/2.17.1/log4j-api-2.17.1.jar
download "commons-io-2.4.jar" http://search.maven.org/remotecontent?filepath=commons-io/commons-io/2.4/commons-io-2.4.jar

download "google-api-services-storage-v1-rev20190910-1.30.3.jar" http://search.maven.org/remotecontent?filepath=com/google/apis/google-api-services-storage/v1-rev20190910-1.30.3/google-api-services-storage-v1-rev20190910-1.30.3.jar
download "google-api-client-1.30.3.jar" http://search.maven.org/remotecontent?filepath=com/google/api-client/google-api-client/1.30.3/google-api-client-1.30.3.jar
download "google-oauth-client-1.30.2.jar" http://search.maven.org/remotecontent?filepath=com/google/oauth-client/google-oauth-client/1.30.2/google-oauth-client-1.30.2.jar
download "google-http-client-1.32.0.jar" http://search.maven.org/remotecontent?filepath=com/google/http-client/google-http-client/1.32.0/google-http-client-1.32.0.jar
download "google-http-client-jackson2-1.32.0.jar" http://search.maven.org/remotecontent?filepath=com/google/http-client/google-http-client-jackson2/1.32.0/google-http-client-jackson2-1.32.0.jar
download "jsr305-3.0.2.jar" http://search.maven.org/remotecontent?filepath=com/google/code/findbugs/jsr305/3.0.2/jsr305-3.0.2.jar
download "j2objc-annotations-1.3.jar" http://search.maven.org/remotecontent?filepath=com/google/j2objc/j2objc-annotations/1.3/j2objc-annotations-1.3.jar
download "opencensus-api-0.24.0.jar" http://search.maven.org/remotecontent?filepath=io/opencensus/opencensus-api/0.24.0/opencensus-api-0.24.0.jar
download "opencensus-contrib-http-util-0.24.0.jar" http://search.maven.org/remotecontent?filepath=io/opencensus/opencensus-contrib-http-util/0.24.0/opencensus-contrib-http-util-0.24.0.jar
download "grpc-context-1.22.1.jar" http://search.maven.org/remotecontent?filepath=io/grpc/grpc-context/1.22.1/grpc-context-1.22.1.jar

mkdir -p /tmp
cd /tmp
download aws-java-sdk-1.11.102.zip http://sdk-for-java.amazonwebservices.com/aws-java-sdk-1.11.102.zip
unzip -q -o aws-java-sdk-1.11.102.zip
for f in $(find aws-java-sdk-1.11.102/ -name '*.jar'); do
    cp $f $jarsdir
done
cd / && rm -r /tmp