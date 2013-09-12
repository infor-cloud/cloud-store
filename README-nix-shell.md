Building with nix-shell-lb
===========================
nix-shell-lb is a tool to set up build environments for building the LB
platform and LB projects. It can automatically build platform dependencies
from source, use specified pre-built dependencies, or any combination of the
two. In the future, it will additionally be able to fetch dependencies from
builder.logicblox.com if requested.


Build procedure
----------------
Setting up the build environment is as simple as:

  $ nix-shell-lb -A build

Then, building s3lib is just:

  $ ./configure $configureFlags # You can add additional flags to configure here
  $ make

As long as you stay in the nix-shell environment, you can modify and build
incrementally as normal without worrying about setting up your environment.

s3lib depends on logicblox

Dependencies from source
-------------------------
Building dependencies from source requires your nix path to be set up
properly. A nix path can have two types of components:

  * A component of the form "foo=/path/to/bar" means <foo> will resolve to
    /path/to/bar, if the latter exists
  * A component of the form "/path/to/some/directory" where the directory
    contains "baz" and "quux" means <baz> will resolve to
    "/path/to/some/directory/baz" and <quux> to "/path/to/some/directory/quux"

If the path part of a nix path component does not exist, that component is
ignored. The first matching non-ignored nix path component is used when
resolving <> path lookups.

Nix path components can be set in two ways:

  * The NIX_PATH env var is a colon-separated list of components
  * On the nix-shell-lb command line, the argument -I component will
    add component to the list of components (takes precedence over NIX_PATH)

Most dependencies are expected to live in locations with the same name as their
bitbucket repository, e.g. the s3lib source tree can be found in <s3lib>. There
are exceptions, though:

  * The logicblox source tree is expected to be found at <LogicBlox>
  * The builder-config source tree is expected to be found at <config>
  * The lb-config source tree is expected to be found at <lb-config>
    even though the repository is currently named buildlib


Pre-built dependencies
-----------------------
nix-shell-lb respects the FOO_HOME environment variables to specify the
locations of built components. If you do not want to fuss with unsetting them
all and want to build all of your dependencies from source, you can
follow the instructions in the preceding sections replacing "nix-shell-lb"
with "nix-shell"

nix-shell-lb currently knows about:

  * LOGICBLOX_HOME is logicblox
  * BLOXWEB_HOME is bloxweb
  * BLOXWEB_MEASURE_SERVICE_HOME is bloxweb-measure-service
  * LBCONFIG_HOME is lb-config
  * S3LIB_HOME is s3lib
  * LBJACOCO_HOME is lbjacoco

Integration build as dependencies
----------------------------------
You can, of course, set the relevant environment variables to point to any
prefix that contains a built and installed version of the corresponding
package. One common use case is to build against some version of the
integration build. In the future nix-shell-lb will take special arguments
to make this use case easier, but for now you can do this as follows:

On the successful release.tested or release.not-tested build page,
click the detauls tab There should be a row labeled "Output store paths" with
a single path. Run
nix-store -r $thatPath --option binary-caches http://builder.logicblox.com
If that succeeds, open up $thatPath/default.nix. It should contain a set
of component-name: path pairs. Use the paths to set the corresponding env var
for each component you want to use from that integration build.
