'''
  Copyright 2018, Infor Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
'''

import os
import sys
import types
import argparse

g_args = {}
g_libpath = {}

def parse_arguments(prefix, deps = {}):
    global g_args
    parser = argparse.ArgumentParser(description='configure build of package')
    parser.add_argument('--prefix', help='installation prefix', default=prefix)

    for k, v in deps.iteritems():
      if isinstance(v, basestring):
          default_path = v
      else:
          if os.getenv(v[0]) == None:
              default_path = v[1]
          else:
              default_path = '$(' + v[0] + ')'

      parser.add_argument('--with-' + k,
                          help='Installation prefix of ' + k,
                          metavar='DIR',
                          default=default_path)
    g_args = parser.parse_args()

def open_makefile():
    global g_makefile
    g_makefile = open('Makefile', 'w')

    emit('topdir = $(shell pwd)')
    emit('prefix = ' + g_args.prefix)
    emit('build = $(topdir)/build')

    for k ,v in g_args.__dict__.iteritems():
        if k.startswith('with_'):
            dep = k[len('with_'):]
            emit(dep + ' = ' + v)

    emit('rt_version = 3.9')
    emit('bloxcompiler_flags = -runtimeVersion $(rt_version) -progress -explain')

def close_makefile():
    '''Close the makefile'''
    dist_files(['configure', 'buildlib.py'])
    dist_target()
    check_target()
    install_target()
    g_makefile.close()

def phony(target, makefile=None):
    """Declare a target to be a phony target, so that a file with that
    name will not prevent make from executing that rule.

    The target parameter can contain multiple targets as a single
    space-delimited string."""
    emit('.PHONY: ' + target, makefile=makefile)

def emit(line, makefile=None):
    '''Add a line to the makefile'''
    if makefile is None: makefile = g_makefile
    makefile.write(line + '\n')

def flatten(seq):
    """Flattens a nested list or tuple.
    
    Example:

        >>> list(flatten([[1, 2], [3, 4]]))
        [1, 2, 3, 4]
    """
    for v0 in seq:
         # Don't recurse again unless it's a collection
         if isinstance(v0, (list, tuple)):
             for v1 in flatten(v0):
                 yield v1
         else:
             yield v0

def rule(output, input, commands=None, separator=':', makefile=None):
    if not isinstance(output, types.ListType):
        output = [ output ]
    if not isinstance(input, types.ListType):
        input = [ input ]
    if commands is None:
        commands = []
    if not isinstance(commands, types.ListType):
        commands = [ commands ]
    commands = list(flatten(commands))
    last_output = len(output) - 1
    for i, trg in enumerate(output):
        tab = '\t' if i > 0 else ''
        sep = (' %s' % separator) if i == last_output else ''
        cont = ' \\' #if len(output) > 1 and i != last_output else ''
        emit(tab + trg + sep + cont, makefile=makefile)
    last_input = len(input) - 1
    for dep in input:
        tab = '\t'
        cont = ' \\'
        emit(tab + dep + cont, makefile=makefile)
    emit('', makefile=makefile)
    if len(commands):
        for cmd in commands:
            emit('\t%s' % cmd, makefile=makefile)

    # Always add a blank line at the end for readability.
    emit('', makefile=makefile)

def find_files(dirname, extension):
    '''utility to find all files with the given extension (e.g. .java) in a directory.'''
    result = []
    for dirpath, dirnames, filenames in os.walk(dirname):
        for filename in filenames:
            f = os.path.join(dirpath, filename)
            if f.endswith(extension):
                result.append(f)
    return result

def check_jar(name, main, srcdir = None, srcdirs = [], classpath = [], srcgen = [], javadoc = None):
    jar(name, srcdir, srcdirs, classpath, srcgen, javadoc, install = False)

    jar_file = '$(build)/jars/' + name + '.jar'

    rule(
        output = 'check-' + name,
        input = [jar_file],
        commands = [
            'java -cp ' + ':'.join(classpath) + ':' + jar_file + ' ' + main],
        separator = '::')

    rule(
        output = 'check',
        input = ['check-' + name],
        separator = '::')

def jar(name,
        srcdir = None,
        srcdirs = [],
        classpath = [],
        srcgen = [],
        javadoc = None,
        resources = [],
        install = True,
        java_version = "1.6",
        manifest=None):
    '''Build a jar by compiling Java files with javac'''
    java_files = []

    if srcdir != None:
        srcdirs = [srcdir] + srcdirs

    for d in srcdirs:
        java_files.extend(find_files(d, '.java'))

    dist_files(java_files)
    dist_files(resources)
    java_files.extend(srcgen)

    jar_file = '$(build)/jars/' + name + '.jar'
    classes_dir = '$(build)/jars/' + name + '.classes'

    emit(jar_file + ': ' + ' '.join(java_files) + ' ' + ' '.join(resources))
    emit('\tmkdir -p ' + classes_dir)
    emit('\tjavac -Xlint:deprecation -d ' + classes_dir +
         ' -source ' + java_version +
         ' -target ' + java_version +
         ' -cp ' + ':'.join(classpath) + ':' + classes_dir +
         ' ' + 
         ' '.join(java_files))
    for f in resources:
        emit('\tcp ' + f + ' ' + classes_dir)

    if manifest is not None:
        manifest_file = '%s/Manifest.txt' % classes_dir

        # clear out previous file
        emit("\tcat /dev/null > %s" % manifest_file)

        if manifest.get('add_classpath'):
           manifest_classpath = [os.path.split(p)[1] for p in classpath] # gets filename only

           # need to write the classpath this way to avoid a "line to long" error in the jar command
           if len(manifest_classpath) >= 1:
               emit("\techo 'Class-Path: %s' >> %s" % (manifest_classpath.pop(), manifest_file))
               for cp in manifest_classpath:
                   emit("\techo '  %s' >> %s" % (cp, manifest_file))

        if manifest.get('main_class'):
           emit("\techo 'Main-Class: %s' >> %s" % (manifest.get('main_class'), manifest_file))

        if manifest.get('package_version'):
            for header, definition in manifest.get('package_version'):
                emit("\techo '%s: %s' >> %s" % (header, definition, manifest_file))

        # manifest files need empty line at end
        emit("\techo ' ' >> %s" % manifest_file)

        emit('\t(cd ' + classes_dir + '; ' +
             'jar cfm ../' + name + '.jar Manifest.txt .)')
    else:
        emit('\t(cd ' + classes_dir + '; ' +
             'jar cf ../' + name + '.jar' + ' .)')

    emit_clean_file(jar_file)
    emit_clean_dir(classes_dir)

    rule(
        output = 'jars',
        input = jar_file,
        separator = '::')

    if install:
        install_file(jar_file, '$(prefix)/lib/java')

    if javadoc != None:
        javadoc_dir = '$(build)/javadoc/' + name

        emit(javadoc_dir +  ' : ' + ' '.join(java_files))
        emit('\tjavadoc ' + 
             ' -classpath ' + ':'.join(classpath) + ':' + classes_dir +
             ' -windowtitle "' + javadoc['title'] + '"' +
             ' -doctitle "' + javadoc['title'] + '"' +
             ' -public ' +
             ' -linkoffline "http://docs.oracle.com/javase/7/docs/api/" "./package-list/java"' +
             ' -linkoffline "http://docs.guava-libraries.googlecode.com/git/javadoc/" "./package-list/guava"' +
             ' -linkoffline "http://docs.amazonwebservices.com/AWSJavaSDK/latest/javadoc/" "./package-list/aws"' +
             ' -d ' + javadoc_dir + 
             ' ' + ' '.join(java_files))

        if install:
            install_dir(javadoc_dir, '$(prefix)/docs/api/' + name)

def bin_program(name):
    install_file(name, '$(prefix)/bin')
    dist_files([name])

def config_file(name):
    install_file(name, '$(prefix)/config')
    dist_files([name])

def install_files(filenames, destdir):
    for f in filenames:
        install_file(f, destdir)

def install_file(filename, destdir):
    rule(
        output = 'install',
        input = filename,
        commands = 'mkdir -p $(DESTDIR)' + destdir + ' && cp ' + filename + ' $(DESTDIR)' + destdir,
        separator = '::')

def install_dir(dirname, destdir):
    rule(
        output = 'install',
        input = dirname,
        commands = 'mkdir -p $(DESTDIR)' + destdir + ' && cp -R ' + dirname + '/* $(DESTDIR)' + destdir,
        separator = '::')

def dist_files(files):
    '''make a list of files part of the source distribution'''
    emit('dist_files :: dist_dir')
    for f in files:
        emit('\tcp --parents --target-directory $(package_name) ' + f)

def dist_target():
    rule(
        output='dist_dir',
        input=[],
        commands=[
            'rm -rf $(package_name)',
            'mkdir -p $(package_name)'
            ],
        separator=':')
    rule(
        output='dist',
        input=['dist_files'],
        commands=[
            'tar zcvf $(package_name).tar.gz $(package_name)',
            'rm -rf $(package_name)'
            ],
        separator=':')
    rule(
        output='distcheck',
        input=['dist'],
        commands=[
            'rm -rf $(build)/distcheck',
            'rm -rf $(build)/prefix',
            'mkdir -p $(build)/distcheck',
            'cd $(build)/distcheck && ' + \
                'tar zxf ../../$(package_name).tar.gz',
            'cd $(build)/distcheck/$(package_name) &&' + \
                './configure --prefix=$(build)/prefix',
            'cd $(build)/distcheck/$(package_name) && ' \
                '$(MAKE)',
            'cd $(build)/distcheck/$(package_name) && ' \
                '$(MAKE) install',
            ],
        separator=':')

def check_target():
    rule(
        output = 'check',
        input = [],
        separator='::')

def install_target():
    rule(
        output = 'install',
        input = [],
        separator='::')

def emit_clean_dir(dir):
    rule(
        output='clean',
        input=[],
        commands='rm -rf ' + dir,
        separator='::')

def emit_clean_file(file):
    rule(
        output='clean',
        input=[],
        commands='rm -f ' + file,
        separator='::')
