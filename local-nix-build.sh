#!/usr/bin/env bash

#
#  Copyright 2017, Infor Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

set -e
set -u

# nixpkgs needs to point to nixpkgs version 17.03,
# e.g. /home/user/dev/nix/nixpkgs-20170404
nixpkgs="$1"

# This wraps use of nix-build to run the test job in default.nix, passing in
# args that point to local source directories.

# clean up stuff that we don't want copied into the nix environment
rm -rf build Makefile buildlib.pyc

# set the "keep" flag if you don't want the test execution directory to be discarded
#keep="-K"

nix-build $keep -I nixpkgs=$nixpkgs -A test default.nix
