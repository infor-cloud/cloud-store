from lbconfig.api import *

depdir = os.getenv('LB_UNIVERSE_DEPS', '/opt/logicblox/lb-universe-deps')

lbconfig_package(
  's3lib-0.2',
  version='0.2',
  default_prefix='/opt/logicblox/s3lib',
  default_targets=['jars']
)

depends_on(
  guava= depdir,
  jcommander= depdir,
  commons_io= depdir,
  commons_codec= depdir,
  log4j= depdir,
  aws_java_sdk= depdir
)


deps = [
    '$(guava)/lib/java/guava-15.0.jar',
    '$(jcommander)/lib/java/jcommander-1.29.jar',
    '$(log4j)/lib/java/log4j-1.2.13.jar',
    '$(commons_io)/lib/java/commons-io-2.4.jar',
    '$(commons_codec)/lib/java/commons-codec-1.9.jar',
    '$(aws_java_sdk)/lib/java/commons-logging-1.1.1.jar',
    '$(aws_java_sdk)/lib/java/joda-time-2.2.jar',
    '$(aws_java_sdk)/lib/java/httpcore-4.2.jar',
    '$(aws_java_sdk)/lib/java/httpclient-4.2.3.jar',
    '$(aws_java_sdk)/lib/java/jackson-annotations-2.1.1.jar',
    '$(aws_java_sdk)/lib/java/jackson-core-2.1.1.jar',
    '$(aws_java_sdk)/lib/java/jackson-databind-2.1.1.jar',
    '$(aws_java_sdk)/lib/java/aws-java-sdk-1.7.1.jar']


link_libs(deps)

jar(
   name = 's3lib-$(version)',
   srcdirs = ['src'],
   classpath = deps,
   javadoc =
     {'title' : "s3lib $(version) API Documentation" })



bin_program('cloud-store')
bin_program('s3tool')
bin_program('s3lib-keygen')

install_files(deps, 'lib/java')

dist_files(['README'])
