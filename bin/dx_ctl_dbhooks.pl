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
# Copyright (c) 2016 by Delphix. All rights reserved.
#
# Program Name : dx_ctl_hooks.pl
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
  'indir=s' => \(my $indir),
  'importDBHooks' => \(my $importDBHooks),
  'action=s' => \(my $action),
  'hook=s@' =>\(my $hook),
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

my @hooksTypeArray = ('configureClone','preRefresh','postRefresh','preRollback','postRollback','preSync','postSync','preSnapshot','postSnapshot','preStart','postStart','preStop','postStop');

my @hookLoopArray;

if (defined($hooktype)) {
  if (lc $hooktype eq 'all') {
    @hookLoopArray = @hooksTypeArray;
  } else {
    my @hookCSname = grep { lc $_ eq lc $hooktype } @hooksTypeArray;
    if (scalar(@hookCSname) ne 1) {
      print "Hooktype not found or too wide\n";
      pod2usage(-verbose => 1,  -input=>\*DATA);
      exit (1);
    }
    push(@hookLoopArray, $hookCSname[0]);
  }
}

if ( !defined($action) ) {
  print "Argument -action is required \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if ( ! ( ( lc $action eq 'set' ) || ( lc $action eq 'delete') || ( lc $action eq 'load' )  ) ) {
  print "Wrong parameter for argument -action : $action \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (lc $action ne 'load') {
  if (scalar(@hookLoopArray) < 1) {
    print "Argument hooktype has to be set for action $action\n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }
  if ((!defined($hookname) && (lc $action eq 'delete'))) {
    print "Argument hookname has to be set for action $action\n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }
}



# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $ret = 0;



for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    $ret = $ret + 1;
    next;
  };

  my $databases = new Databases ( $engine_obj );
  my $groups = new Group_obj($engine_obj, $debug);

  if (lc $action eq 'load') {
    opendir (my $DIRG, $indir);
    my @groups = readdir($DIRG);
    for my $groupname (@groups) {
      next if $groupname =~ /^\./;
      if (!defined($groups->getGroupByName($groupname))) {
        print "Group $groupname not found\n";
        next;
      }
      my $dirg = File::Spec->catfile($indir,$groupname);
      if (-d $dirg) {
        opendir (my $DIR, $dirg);
        my @dbs = readdir($DIR);
        for my $dbname (@dbs) {
          ## skip hidden
          next if $dbname =~ /^\./;
          my $dir1 = File::Spec->catfile($dirg,$dbname);
          my $db_list = Toolkit_helpers::get_dblist_from_filter(undef, $groupname, undef, $dbname, $databases, $groups, undef, undef, undef, undef, undef, undef, undef, $debug);

          if (!defined($db_list)) {
            print "Database $dbname not found\n";
            next;
          }
          if (scalar(@{$db_list}) > 1) {
            print "Database can't be identified by group name and database name\n";
            next;
          }
          print "Adding hooks to database $groupname / $dbname\n";
          if (-d $dir1) {
            opendir (my $DIR1, $dir1);
            my @hooktypes = readdir($DIR1);
            for my $hook (@hooktypes) {
              ## skip hidden
              next if $hook =~ /^\./;
              my $dir2 = File::Spec->catfile($dir1, $hook);
              if (-d $dir2) {
                opendir (my $DIR2, $dir2);
                my @hookfiles = readdir($DIR2);
                for my $file (@hookfiles) {
                  ## skip hidden
                  next if $file =~ /^\./;
                  my $filepath = File::Spec->catfile($dir2, $file);
                  if (-f $filepath) {
                    my ($type) = $filepath =~ /.*\.(.*)$/;
                    my $dbobj = $databases->getDB($db_list->[0]);
                    my @tf = File::Basename::fileparse($file, ('.BASH','.SHELL','.EXPECT','.PS'));
                    my @hookline = ( $tf[0] . ',' . $filepath . ',' .$type );
                    print "Hooktype $hook hookname $tf[0] from $filepath\n";
                    my $lret = $dbobj->setAnyHook($hook, \@hookline);
                  }
                }
                closedir($DIR2);
              }
            }
            closedir($DIR1);
          }
        }

        closedir($DIR);
      }
    }
    closedir($DIRG);
  } else {
    Toolkit_helpers::check_filer_options (1, $type, $group, $host, $dbname, undef);
    my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups, undef, undef, undef, undef, undef, undef, undef, $debug);
    if (! defined($db_list)) {
      print "There is no DB selected to process on $engine . Please check filter definitions. \n";
      $ret = $ret + 1;
      next;
    }

    for my $dbitem (@{$db_list}) {
      my $dbobj = $databases->getDB($dbitem);
      for my $hookitem (@hookLoopArray) {

        if (lc $action eq 'delete') {
          my $lret = $dbobj->deleteHook($hookitem, $hookname);
          if ($lret == 0) {
            print "Hook $hookitem with name $hookname was sucessfully deleted\n";
          } elsif ($lret == 1) {
            print "Problem with deleting hook $hookitem with name $hookname\n";
            $ret = $ret + 1;
            next;
          } elsif ($lret == 2) {
            print "Hook type $hookitem hookname $hookname not found\n";
            $ret = $ret + 1;
            next;
          } else {
            print "Unknown error with $hookname\n";
            $ret = $ret + 1;
            next;
          }
        } elsif (lc $action eq 'set') {
          my $lret = $dbobj->setAnyHook($hookitem, $hook);
          $ret = $ret + $lret;
        }

      }


    }
  }






}

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_ctl_dbhooks    [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                   <-action delete|set|load>
                   [ -hookname hook_name ]
                   [ -hooktype configureClone|preRefresh|postRefresh|preRollback|postRollback|preSnapshot|postSnapshot|preStart|postStart|preStop|postStop ]
                   [ -dbname dbname | -group group | -host host | -type type ]
                   [ -hook [hookname,]template|filename[,OS_shell]]
                   [ -indir /path ]


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

=head2 Options

=over 3

=item B<-hookname>
Hook name

=item B<-hooktype>
Hook type - configureClone,preRefresh,postRefresh,preRollback,postRollback,preSnapshot,postSnapshot,preStart,postStart,preStop,postStop

=item B<-hook>
Hook definition.
File name is a path to a file with a hook body on machine
with dxtoolkit.
Template name is an operational template name

Allowed combinations:
- hookname,template_name,OS_shell
- hookname,filename,OS_shell
- hookname,template_name
- hookname,filename
- template_name
- filename


=item B<-group>
Group Name

=item B<-dbname>
Database Name

=item B<-host>
Host Name

=item B<-type>
Type (dsource|vdb)


=item B<-indir dir>
Location of directory with hook files

=item B<-format>
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Adding a ConfigureClone hook for database autotest with hook name hook1, defined in file ./hook1 to be executed by BASH shell

 dx_ctl_dbhooks -d Landshark51 -action set -hook hook1,./hook1,BASH -hooktype configureclone -dbname autotest
 Waiting for all actions to complete. Parent action is ACTION-12767
 Hook added

Delete a ConfigureClone hook for database autotest with hook name hook1

 dx_ctl_dbhooks -d Landshark51 -action delete -hooktype configureclone -hookname hook1 -dbname autotest
 Waiting for all actions to complete. Parent action is ACTION-12769
 Hook deleted
 Hook configureClone with name hook1 was sucessfully deleted

Loading hooks from directory created by dx_get_dbhooks for all VDBs

 dx_ctl_dbhooks -d Landshark51 -action load -indir /tmp/hooks/ -type VDB
 Adding hooks to database Analytics / autofs
 Hooktype configureClone hookname hook1 from /tmp/hooks/Analytics/autofs/configureClone/hook1.BASH
 Waiting for all actions to complete. Parent action is ACTION-12810
 Hook added
 Hooktype preRollback hookname savestate from /tmp/hooks/Analytics/autofs/preRollback/savestate.BASH
 Waiting for all actions to complete. Parent action is ACTION-12811
 Hook added
 Adding hooks to database Analytics / OH121_TARGET
 Hooktype configureClone hookname hook1 from /tmp/hooks/Analytics/OH121_TARGET/configureClone/hook1.BASH
 Waiting for all actions to complete. Parent action is ACTION-12812
 Hook added
 Adding hooks to database Analytics / OH122_TARGET
 Hooktype configureClone hookname hook1 from /tmp/hooks/Analytics/OH122_TARGET/configureClone/hook1.BASH
 Waiting for all actions to complete. Parent action is ACTION-12813
 Hook added
 Adding hooks to database Analytics / vdb122
 Hooktype configureClone hookname hook1 from /tmp/hooks/Analytics/vdb122/configureClone/hook1.BASH
 Waiting for all actions to complete. Parent action is ACTION-12814
 Hook added

=cut
