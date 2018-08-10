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
# Program Name : dx_get_dbhooks.pl
# Description  : Export database hooks
# Author       : Marcin Przepiorowski
# Created      : 16 Feb 2018 (v2.3.X)

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
use Hook_obj;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'hooktype=s' => \(my $hooktype),
  'hookname=s' => \(my $hookname),
  'dbname=s'  => \(my $dbname),
  'type=s' => \(my $type),
  'group=s' => \(my $group),
  'host=s' => \(my $host),
  'outdir=s' => \(my $outdir),
  'save' => \(my $save),
  'exportDBHooks' => \(my $exportDBHooks),
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

if ( defined($exportDBHooks)  && ( ! defined($outdir) ) ) {
  print "Option exportDBHooks require option outdir to be specified \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if ( defined($save)  && ( ! defined($outdir) ) ) {
  print "Option save require option outdir to be specified \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}



Toolkit_helpers::check_filer_options (undef, $type, $group, $host, $dbname, undef);

my @hooksTypeArray = ('configureClone','preRefresh','postRefresh','preRollback','postRollback','preSnapshot','postSnapshot','preStart','postStart','preStop','postStop');

my @hookLoopArray;

if (defined($hooktype)) {
  if (lc $hooktype eq 'all') {
    @hookLoopArray = @hooksTypeArray;
  } else {
    push(@hookLoopArray, $hooktype);
  }
} else {
  @hookLoopArray = @hooksTypeArray;
}

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $output = new Formater();

$output->addHeader(
    {'dbname',   20},
    {'hook type', 20},
    {'name',   20},
    {'type',   15},
    {'command', 100}
);

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    $ret = $ret + 1;
    next;
  };

  # load objects for current engine



    my $databases = new Databases ( $engine_obj );
    my $groups = new Group_obj($engine_obj, $debug);

    # filter implementation

    my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups, undef, undef, undef, undef, undef, undef, $debug);
    if (! defined($db_list)) {
      print "There is no DB selected to process on $engine . Please check filter definitions. \n";
      $ret = $ret + 1;
      next;
    }

    if (defined($exportDBHooks)) {

      # export hooks in DE JSON format for backup
      for my $dbitem (@{$db_list}) {
        my $dbobj = $databases->getDB($dbitem);
        $dbobj->exportDBHooks($outdir);
      }

    } else {

      # list hooks and bodys in formatter
      # or save a directory tree

      for my $dbitem (@{$db_list}) {
        my $dbobj = $databases->getDB($dbitem);
        #print Dumper $dbobj->getName();
        #print Dumper $outdir;
        my $dbname = $dbobj->getName();
        my $groupname = $groups->getName($dbobj->getGroup());
        my $hookfound = 0;
        for my $hookitem (@hookLoopArray) {
          my $array = $dbobj->getHook($hookitem, $save);

          if (scalar(@{$array})>0) {

            for my $h (@{$array}) {

              if (defined($hookname)) {
                if ($h->{name} ne $hookname) {
                  next;
                } else {
                  $hookfound = 1;
                }
              }

              if (defined($save)) {
                my $loc = File::Spec->catfile($outdir,$groupname);
                mkdir $loc;
                $loc = File::Spec->catfile($loc,$dbname);
                mkdir $loc;
                $loc = File::Spec->catfile($loc,$h->{hooktype});
                mkdir $loc;
                $loc = File::Spec->catfile($loc,$h->{name});
                $loc = $loc . '.' . $h->{hookOSType};
                print "Saving hook to file $loc\n";
                $dbobj->exportHook($h->{command}, $loc);
              } else {
                $output->addLine(
                  $dbname,
                  $h->{hooktype},
                  $h->{name},
                  $h->{hookOSType},
                  $h->{command}
                );
              }
            }
          }
          if (!$hookfound && (!defined($save))) {
            print "Hook $hookitem with name $hookname not found in database $dbname\n";
            $ret = $ret + 1;
          }
        }
      }

    }



}

if (!(defined($exportDBHooks) || defined($save)) ) {
  Toolkit_helpers::print_output($output, $format, $nohead);
}

exit $ret;

__DATA__


=head1 SYNOPSIS

 dx_get_dbhooks    [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                   [ -hookname hook_name ]
                   [ -dbname dbname | -group group | -host host | -type type ]
                   [ -outdir dir]
                   [ -save ]
                   [ -exportDBHooks ]
                   [ -format csv|json ]
                   [ -help|? ]
                   [ -debug ]

=head1 DESCRIPTION

List or export operation templates from engine. If no operation template name is specified all templates will be processed.

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

=item B<-hookname>
Hook name

=item B<-group>
Group Name

=item B<-dbname>
Database Name

=item B<-host>
Host Name

=item B<-type>
Type (dsource|vdb)


=back

=head1 OPTIONS

=over 3

=item B<-exportDBHooks>
Export database (specified by database filters) hooks in Delphix Engine JSON format into a outdir directory
This file(s) can by used by dx_provision_vdb or dx_ctl_dbhooks script

=item B<-save>
Save a hook(s) as file(s) into a directory structure started by -outdir
Output structure is defined as follow: OUTDIR/DBNAME/HOOKTYPE/hookname

=item B<-outdir>
Location of exported operation templates files

=item B<-format>
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Display all hooks from datasets on Delphix Engine

 dx_get_dbhooks -d Landshark51

 dbname               hook type            name                 type            command
 -------------------- -------------------- -------------------- --------------- ----------------------------------------------------------------------------------------------------
 autofs               preRollback          savestate            BASH            cp /home/save /tmp
 autotest             postRefresh          changepassword       BASH            sqlplus / as sysdba <<EOF<cr>alter user app identified by app;<cr>EOF

Save hooks into file under outdir folder

 dx_get_dbhooks.pl -d Landshark51 -save -outdir /tmp
 Saving hook to file /tmp/autofs/preRollback/savestate
 Saving hook to file /tmp/autotest/postRefresh/changepassword

Export hooks using Delphix Engine JSON format for other dxtoolkit scripts

 dx_get_dbhooks.pl -d Landshark51 -exportDBHooks -outdir /tmp -dbname autotest
 Exporting database autotest hooks into  /tmp/autotest.dbhooks


=cut
