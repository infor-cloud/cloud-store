#! /bin/bash

set -e
set -x
set -u

function md5
{
  md5sum $1 | awk '{print $1}'
}

function exec_no_encryption()
{
  f=$1
  base=$(basename $f)

  cloud-store upload s3://mbravenboer.wagfm.request/$base  -i $f
  rm -f $base.tmp
  cloud-store download s3://mbravenboer.wagfm.request/$base -o $base.tmp
  test $(md5 $base.tmp) = $(md5 $f)
}

function exec_encryption()
{
  f=$1
  base=$(basename $f)

  cloud-store upload s3://mbravenboer.wagfm.request/$base  -i $f --key martin
  rm -f $base.tmp
  cloud-store download s3://mbravenboer.wagfm.request/$base -o $base.tmp
  test $(md5 $base.tmp) = $(md5 $f)
}

exec_encryption $HOME/log.tgz
exec_no_encryption $HOME/log.tgz
