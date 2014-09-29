from lbconfig.api import *

deps='/opt/logicblox/deps'

jetty_version = '7.6.7.v20120910'
openam_version = '10.0.0.0-patched'
lb_web_deps = os.getenv('BLOXWEB_DEPS', '/opt/logicblox/deps')

lbconfig_package(
  's3lib-0.2',
  version='0.2',
  default_prefix='/opt/logicblox/s3lib',
  default_targets=['jars']
)

depends_on(
  guava= deps + '/guava-15.0',
  jcommander= deps + '/jcommander-1.29',
  commons_io= deps + '/commons-io-2.4',
  commons_codec= deps + '/commons-codec-1.9',
  log4j= deps + '/log4j-1.2.13',
  aws_java_sdk= deps + '/aws-java-sdk-1.7.1'
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



bin_program('s3tool')
bin_program('s3lib-keygen')

install_files(deps, 'lib/java')

dist_files(['README'])
