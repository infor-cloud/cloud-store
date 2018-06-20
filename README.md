## Building from source

Currently there are two ways to build `cloud-store`:

### Using Nix

- Just run:


    $ nix-build -A build


### Manually

- Download all the required dependencies mentioned in `deps.nix` in a directory
- Point the `LB_UNIVERSE_DEPS` environment variable to that directory (it's set to 
`/opt/logicblox/lb-universe-deps` by default)
- Build and install:


    $ ./configure
    $ make install


- The default installation prefix is `/opt/logicblox/cloudstore`, unless `CLOUDSTORE_HOME`
is set. To configure this, use the `--prefix` option. For example:


    $ ./configure --prefix=$HOME/cloudstore


## Usage

Generate a key pair:

    $ cloud-store keygen -n testkeypair

By default, the key pair is installed under `~/.cloudstore-keys`.

Upload:

    $ cloud-store upload s3://bucket/AS400.jpg -i AS400.jpg --key testkeypair

Download:

    $ cloud-store download s3://bucket/AS400.jpg -o AS400-2.jpg

Before you can run the `cloud-store` command, you need to set the environment variables 
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_KEY` to the corresponding values from your AWS credentials.

## Authors

  * Shea Levy
  * Martin Bravenboer
  * Patrick Lee
  * Luke Vanderhart
  * Thiago T. Bartolomei
  * Rob Vermaas
  * George Kollias
  * Wes Hunter
