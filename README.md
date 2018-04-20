Building from source
---------------

All `cloud-store` third-party dependencies can be downloaded from:

    https://bob.logicblox.com/job/logicblox-40/universe/linux.lb_universe_deps/latest/download/1

It is suggested to unpack the archive under `/opt/logicblox`, but it can be put anywhere:

    $ cd /opt/logicblox
    $ tar zxvf /path/to/lb-universe-deps.tar.gz
    $ source lb-universe-deps/env.sh

To build and install:

    $ ./configure
    $ make install

The default installation prefix is `/opt/logicblox/cloudstore`, unless `CLOUDSTORE_HOME`
is set. To configure this, use the `--prefix` option. For example:

    $ ./configure --prefix=$HOME/cloudstore

The binary distribution of `cloud-store` is self-contained.


Usage
---------------

Generate a key pair:

    $ cloud-store keygen -n testkeypair

By default, the key pair is installed under `~/.s3lib-keys`.

Upload:

    $ cloud-store upload s3://bucket/AS400.jpg -i AS400.jpg --key testkeypair

Download:

    $ cloud-store download s3://bucket/AS400.jpg -o AS400-2.jpg

Before you can run the `cloud-store` command, you need to set the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_KEY` to the corresponding values from your AWS credentials.

Authors
---------------

  * Shea Levy
  * Martin Bravenboer
  * Patrick Lee
  * Luke Vanderhart
  * Thiago T. Bartolomei
  * Rob Vermaas
  * George Kollias

