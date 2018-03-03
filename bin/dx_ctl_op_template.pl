# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Copyright (c) 2016,2018 by Delphix. All rights reserved.
#
# Program Name : dx_ctl_op_template.pl
# Description  : Import hooks or hooks templates
# Author       : Marcin Przepiorowski
# Created      : 02 June 2016 (v2.1.0)
#

use strict;
use warnings;
use JSON;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev); #avoids conflicts with ex host and help
use File::Basename;
use Pod::Usage;
use FindBin;
use Data::Dumper;

my $abspath = $FindBin::Bin;

use lib '../lib';
use Engine;
use Formater;
use Databases;
use Op_template_obj;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'name|n=s' => \(my $name), 
  'indir=s' => \(my $indir),
  'filename=s' => \(my $filename),
  'importHook' => \(my $importHook),
  'updateHook' => \(my $updateHook),
  'importHookScript=s' => \(my $importHookScript),
  'debug:i' => \(my $debug), 
  'all' => (\my $all),
  'dever=s' => \(my $dever),
  'version' => \(my $print_version),
  'nohead' => \(my $nohead),
  'format=s' => \(my $format),
  'configfile|c=s' => \(my $config_file)
) or pod2usage(-verbose => 1,  -input=>\*DATA);

pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   

my $engine_obj = new Engine ($dever, $debug);
$engine_obj->load_config($config_file);

if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (defined($filename) && defined($indir) ) {
  print "Option filename and indir are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if ( ( ! defined($filename)  ) && ( ! defined($indir) ) && ( ! defined($importHookScript) ) ) {
  print "Option filename, indir or importHookScript is required \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if ( ! ( defined($importHook) || defined($updateHook) ||  defined($importHookScript) ) ) {
  print "One of the following option is required importHook, updateHook, importDBHooks or importHookScript \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if ( defined($importHookScript) && (! defined($name) ) ) {
  print "Hook name is required to import script \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  # load objects for current engine

  my $op_templates;
  

  $op_templates = new Op_template_obj (  $engine_obj, undef, $debug );
  


  if (defined($filename) || defined($importHookScript)) {
    if (defined($importHook)) {
      if ($op_templates->importHookTemplate($filename)) {
        print "Problem with load operation template from file $filename\n";
        $ret = $ret + 1;
        next;
      }
    } elsif (defined($updateHook)) {  
      if ($op_templates->updateHookTemplate($filename)) {
        print "Problem with update operation template from file $filename\n";
        $ret = $ret + 1;
        next;
      }
    } elsif (defined($importHookScript)) {  
      my $scripthook = $op_templates->getHookByName($name);
      if (!defined($scripthook)) {
        print "Can't find operation template name $name \n";
        $ret = $ret + 1;
        next;
      }
      if ($op_templates->updateHookScript($scripthook, $importHookScript)) {
        print "Problem with update script operation template from file $filename\n";
        $ret = $ret + 1;
        next;
      }
    } 
  } else {
    opendir (my $DIR, $indir) or die ("Can't open a directory $indir : $!");

    while (my $file = readdir($DIR)) {
        # take only .template files
        if ($file =~ m/\.opertemp$/) {
          my $filename = $indir . "/" . $file;
          if (defined($importHook)) {
            if ($op_templates->importHookTemplate($filename)) {
              print "Problem with load operation template from file $filename\n";
              $ret = $ret + 1;
              next;
            }
          } elsif (defined($updateHook)) {  
            if ($op_templates->updateHookTemplate($filename)) {
              print "Problem with update operation template from file $filename\n";
              $ret = $ret + 1;
              next;
            }
          }        
        }
    }

    closedir ($DIR);
  }




}

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_ctl_op_template    [ -engine|d <delphix identifier> | -all ] [ -configfile file ] 
                       [ -name operation_template_name ] 
                       [ -importHook ]
                       [ -updateHook ]
                       [ -importHookScript filename ]
                       [ -indir dir ]
                       [ -filename filename ]

=head1 DESCRIPTION

Import operation template into engine.

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=item B<-configfile file>
Location of the configuration file.
A config file search order is as follow:
- configfile parameter
- DXTOOLKIT_CONF variable
- dxtools.conf from dxtoolkit location

=back

=head2 Filters

=over 4

=item B<-name operation_template_name>
Operation Template name

=back

=head1 OPTIONS


=over 3

=item B<-importHook>                                                                                                                                            
Import operation template from file or directory

=item B<-updateHook>                                                                                                                                            
Update operation template from file or directory


=item B<-importHookScript filename>   
Import script body from filename into operation template 

=item B<-filename name>                                                                                                                                            
Location of file with operation template

=item B<-indir dir>                                                                                                                                            
Location of directory with operation templates files

=item B<-format>                                                                                                                                            
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Import operation templates from directory where they were exported by dx_get_hooks

 dx_ctl_op_template -d Landshark5 -importHook -indir /tmp/a
 Importing operation template from file /tmp/a/after.opertemp. 
 Import completed Operation template test1 from file /tmp/a/test1.opertemp already exist.
 

Update an operation template test1 with a new script

 dx_ctl_op_template -d Landshark5 -name test1 -importHookScript /tmp/test_new.sh
 Updating operation template test1 command from file /tmp/test_new.sh. 
 Update completed

=cut



