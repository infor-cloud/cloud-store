# Running cloud-store unit tests

A suite of JUnit tests has been written to exercise the cloud-store implementation. The test 
suite is not complete but does cover many common operations.  The tests can be executed against 
AWS and GCS stores, as well as with a local Minio instance.

## Installing Minio

The Minio server can be used to run all the tests locally without AWS or GCS credentials.
Using Minio does require access keys but you can set the `MINIO_ACCESS_KEY` and 
`MINIO_SECRET_KEY` to any strings you want for local testing. Additional information on Minio can
be found at  https://docs.minio.io/docs/minio-client-complete-guide. Follow these steps to 
download, install, and test the Minio server for 64-bit linux:

    # install Minio server
    mkdir /minio/install/dir
    cd /minio/install/dir
    wget https://dl.minio.io/server/minio/release/linux-amd64/minio
    chmod a+x ./minio
    ./minio -h

    # install Minio client
    wget https://dl.minio.io/client/mc/release/linux-amd64/mc
    chmod a+x ./mc
    ./mc -h

    # start server
    export MINIO_ACCESS_KEY=my_minio_key
    export MINIO_SECRET_KEY=my_minio_secret
    ./minio server /tmp/minio

    # test basic local Minio connection
    export MINIO_ACCESS_KEY=my_minio_key
    export MINIO_SECRET_KEY=my_minio_secret
    echo "hello" > test
    ./mc cp ./test local:/test
    ./mc ls local:/
    ./mc cat local:/test

    # test Minio S3 interface
    #    NOTE:  this requires AWS keys with MINIO_ACCESS_KEY=$AWS_ACCESS_KEY_ID 
    #           and MINIO_SECRET_KEY=$AWS_SECRET_ACCESS_KEY
    aws --endpoint-url http://localhost:9000/ s3 mb s3://minio-test-bucket
    echo "s3 hello" > s3_test
    aws --endpoint-url http://localhost:9000/ s3 cp s3_test s3://minio-test-bucket/
    aws --endpoint-url http://localhost:9000/ s3 ls s3://minio-test-bucket

    # testing cloud-store with minio
    #    NOTE:  this requires AWS keys with MINIO_ACCESS_KEY=$AWS_ACCESS_KEY_ID 
    #           and MINIO_SECRET_KEY=$AWS_SECRET_ACCESS_KEY
    cloud-store ls s3://minio-test-bucket/ --endpoint http://127.0.0.1:9000/
    mkdir zzz
    cd zzz
    cloud-store download s3://minio-test-bucket/s3_test --endpoint http://127.0.0.1:9000/ -o s3_test_dl
    cat s3_test_dl


## Running Tests

To run the full test suite using the Minio server, start the server as outlined and then run:

    # AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY should be set, e.g.:
    #   export AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY 
    #   export AWS_SECRET_ACCESS_KEY=$MINIO_SECRET_KEY
    ./run-tests.sh --endpoint http://127.0.0.1:9000/

Pass a -h or --help option to run-tests.sh to set the options supported by the test script. By 
default, the test framework will try to create a new unique bucket in which to work while 
executing tests. This really only works with the Minio server (may work with AWS if you have the 
right credentials, but unlikely). To run the tests using a GCS or AWS server, it is recommended 
that you specify a existing bucket and folder using the --dest-prefix option as in the following 
examples:

    # run test in an existing GCS bucket, creating the UT_ZZZ "folder" if necessary
    ./run-tests.sh --dest-prefix gs://lb-cloud-store-testing/UT_ZZZ

    # run test in an existing AWS bucket, creating the UT_ZZZ "folder" if necessary
    ./run-tests.sh --dest-prefix s3://cloud-store-test/UT_ZZZ


## Running Tests Within Nix

The cloud-store test suite using Minio as the storage server can be executed within a Nix  
environment as well. See the local-nix-build.sh bash script and its comments for information on 
doing this.
