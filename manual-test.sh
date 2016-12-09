#! /usr/bin/env bash

set -e
set -x
set -u

file=$1
dir_uri=$2
enc_key=$3

function md5
{
  md5sum $1 | awk '{print $1}'
}

function exec_no_encryption()
{
  f=$1
  base=$(basename $f)

  echo "cloud-store upload ${dir_uri%%/}/$base -i $f"
  cloud-store upload ${dir_uri%%/}/$base -i $f --endpoint http://localhost:9000/
  rm -f $base.tmp
  echo "cloud-store download ${dir_uri%%/}/$base -o $base.tmp"
  cloud-store download ${dir_uri%%/}/$base -o $base.tmp
  test $(md5 $base.tmp) = $(md5 $f)
  echo "md5 $(md5 $f)"
  diff -q $base.tmp $f
}

function exec_encryption()
{
  f=$1
  base=$(basename $f)

  echo "cloud-store upload ${dir_uri%%/}/$base.enc -i $f --key $enc_key"
  cloud-store upload ${dir_uri%%/}/$base.enc -i $f --key $enc_key
  rm -f $base.tmp
  echo "cloud-store download ${dir_uri%%/}/$base.enc -o $base.tmp"
  cloud-store download ${dir_uri%%/}/$base.enc -o $base.tmp
  test $(md5 $base.tmp) = $(md5 $f)
  echo "md5 $(md5 $f)"
  diff -q $base.tmp $f
}

exec_no_encryption $file
exec_encryption $file
