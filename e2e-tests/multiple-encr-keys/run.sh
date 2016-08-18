#!/usr/bin/env bash
set -e
set -u
# set -x

s3_bucket=$1
s3_path=$2
local_path=$3

function match_stderr()
{
  local regex_file="$1"
  local stderr_file="$2"
  while read l; do grep -Eq "$l" $stderr_file; done < $regex_file
}

function md5
{
  md5sum $1 | awk '{print $1}'
}

function report_success()
{
  echo " - success"
}

function report_expected_failure()
{
  echo " - expected failure"
}

if [ "s3://$s3_bucket/$s3_path" == "s3://s3lib-testing/abcd.txt" ]; then
  echo "Pre-existing \"s3://s3lib-testing/abcd.txt\" should not be overwritten. Choose another S3 path."
  exit 1
fi

echo -n "Download pre-existing unencrypted file uploaded without \"s3tool-pubkey-hash\" header"
cloud-store download s3://s3lib-testing/abcd.txt -o ./abcd.txt.tmp --keydir s3lib-keys-1 --overwrite
report_success

echo -n "Download pre-existing encrypted file uploaded without \"s3tool-pubkey-hash\" header"
cloud-store download s3://s3lib-testing/abcd-encr.txt -o ./abcd-encr.txt.tmp --keydir s3lib-keys-1 --overwrite
report_success

echo -n "Add new encryption key to a pre-existing object without \"s3tool-pubkey-hash\" header"
cloud-store add-encrypted-key s3://s3lib-testing/abcd-encr.txt --key test2 --keydir s3lib-keys-1 --retry 2 2> stderr.tmp || true
match_stderr expected-stderr/1.reg stderr.tmp
report_expected_failure

echo -n "Upload $local_path to s3://$s3_bucket/$s3_path"
cloud-store upload s3://$s3_bucket/$s3_path -i $local_path --key test1 --keydir s3lib-keys-1
report_success

echo -n "Download newly-uploaded encrypted file"
cloud-store download s3://$s3_bucket/$s3_path -o $local_path.tmp --keydir s3lib-keys-1 --overwrite
test $(md5 $local_path.tmp) = $(md5 $local_path)
diff -q $local_path.tmp $local_path
report_success

echo -n "Add 2nd key"
cloud-store add-encrypted-key s3://$s3_bucket/$s3_path --key test2 --keydir s3lib-keys-1 2> stderr.tmp
match_stderr expected-stderr/2.reg stderr.tmp
report_success

echo -n "Add 3rd key with only one test2 being available locally"
rm -rf s3lib-keys-tmp
mkdir s3lib-keys-tmp
cp s3lib-keys-1/test2.pem s3lib-keys-tmp
cp s3lib-keys-1/test3.pem s3lib-keys-tmp
cloud-store add-encrypted-key s3://$s3_bucket/$s3_path --key test3 --keydir s3lib-keys-tmp 2> stderr.tmp
match_stderr expected-stderr/3.reg stderr.tmp
report_success

echo -n "Re-add test2 key"
cloud-store add-encrypted-key s3://$s3_bucket/$s3_path --key test2 --keydir s3lib-keys-1 2> stderr.tmp || true
match_stderr expected-stderr/4.reg stderr.tmp
report_expected_failure

echo -n "Add test4 with none of \"s3tool-key-name\" keys being available locally"
rm -rf s3lib-keys-tmp
mkdir s3lib-keys-tmp
cloud-store add-encrypted-key s3://$s3_bucket/$s3_path --key test4 --keydir s3lib-keys-tmp 2> stderr.tmp || true
match_stderr expected-stderr/5.reg stderr.tmp
report_expected_failure

# TODO: Add support for adding new key (e.g. test4) which is in a different directory than already
# added keys? E.g.:
# cloud-store add-encrypted-key s3://$s3_bucket/$s3_path --key test4 --keydir s3lib-keys-2
echo -n "Add 4th key"
cloud-store add-encrypted-key s3://$s3_bucket/$s3_path --key test4 --keydir s3lib-keys-1 2> stderr.tmp
match_stderr expected-stderr/6.reg stderr.tmp
report_success

echo -n "Add 5th key"
cloud-store add-encrypted-key s3://$s3_bucket/$s3_path --key test5 --keydir s3lib-keys-1 2> stderr.tmp || true
match_stderr expected-stderr/7.reg stderr.tmp
report_expected_failure

echo -n "Download with only one \"s3tool-key-name\" key being available locally"
rm -rf s3lib-keys-tmp
mkdir s3lib-keys-tmp
cp s3lib-keys-1/test3.pem s3lib-keys-tmp
cloud-store download s3://$s3_bucket/$s3_path -o $local_path.tmp --keydir s3lib-keys-tmp --overwrite
test $(md5 $local_path.tmp) = $(md5 $local_path)
diff -q $local_path.tmp $local_path
report_success

echo -n "Remove encryption key of a pre-existing object without \"s3tool-pubkey-hash\" header"
cloud-store remove-encrypted-key s3://s3lib-testing/abcd-encr.txt --key test1 --retry 2 2> stderr.tmp || true
match_stderr expected-stderr/8.reg stderr.tmp
report_expected_failure

echo -n "Removing non-existent encryption key"
cloud-store remove-encrypted-key s3://$s3_bucket/$s3_path --key test5 2> stderr.tmp || true
match_stderr expected-stderr/9.reg stderr.tmp
report_expected_failure

echo -n "Remove 3rd key"
cloud-store remove-encrypted-key s3://$s3_bucket/$s3_path --key test3 2> stderr.tmp
match_stderr expected-stderr/10.reg stderr.tmp
report_success

echo -n "Remove 2nd key"
cloud-store remove-encrypted-key s3://$s3_bucket/$s3_path --key test2 2> stderr.tmp
match_stderr expected-stderr/11.reg stderr.tmp
report_success

echo -n "Remove 1st key"
cloud-store remove-encrypted-key s3://$s3_bucket/$s3_path --key test1 2> stderr.tmp
match_stderr expected-stderr/12.reg stderr.tmp
report_success

echo -n "Remove last key"
cloud-store remove-encrypted-key s3://$s3_bucket/$s3_path --key test4 2> stderr.tmp || true
match_stderr expected-stderr/13.reg stderr.tmp
report_expected_failure

echo -n "Download with only one \"s3tool-key-name\" key being available locally"
rm -rf s3lib-keys-tmp
mkdir s3lib-keys-tmp
cp s3lib-keys-1/test4.pem s3lib-keys-tmp
cloud-store download s3://$s3_bucket/$s3_path -o $local_path.tmp --keydir s3lib-keys-tmp --overwrite
test $(md5 $local_path.tmp) = $(md5 $local_path)
diff -q $local_path.tmp $local_path
report_success
