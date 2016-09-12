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
# Program Name : dx_ctl_dsource.pl
# Description  : Create / attach / detach dSource
# Author       : Marcin Przepiorowski
# Created      : 12 Apr 2016 (v2.2.4)
#


use warnings;
use strict;
use JSON;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev); #avoids conflicts with ex host and help
use File::Basename;
use Pod::Usage;
use FindBin;
use Data::Dumper;

my $abspath = $FindBin::Bin;

use lib '../lib';
use Databases;
use Engine;
use Jobs_obj;
use Group_obj;
use Toolkit_helpers;
use FileMap;

my $version = $Toolkit_helpers::version;

my $logsync = "no";

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'sourcename=s' => \(my $sourcename), 
  'dsourcename=s'  => \(my $dsourcename),
  'action=s' => \(my $action),
  'group=s' => \(my $group), 
  'creategroup' => \(my $creategroup), 
  'sourceinst=s' => \(my $sourceinst),
  'sourceenv=s' => \(my $sourceenv),
  'stageinst=s' => \(my $stageinst),
  'stageenv=s' => \(my $stageenv),
  'dbuser=s'  => \(my $dbuser),
  'password=s'  => \(my $password),
  'source_os_user=s'  => \(my $source_os_user),
  'stage_os_user=s'  => \(my $stage_os_user),
  'backup_dir=s' => \(my $backup_dir),
  'dumppwd=s' => \(my $dumppwd),
  'logsync=s' => \($logsync),
  'validatedsync=s' => \(my $validatedsync), 
  'delphixmanaged=s' => \(my $delphixmanaged),
  'type=s' => \(my $type),
  'dever=s' => \(my $dever),
  'debug:n' => \(my $debug), 
  'all' => (\my $all),
  'version' => \(my $print_version)
) or pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);



pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   


my $engine_obj = new Engine ($dever, $debug);
my $path = $FindBin::Bin;
my $config_file = $path . '/dxtools.conf';

$engine_obj->load_config($config_file);


if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
  exit (1);
}


if ( (! defined($action) ) || ( ! ( ( $action eq 'create') || ( $action eq 'attach') || ( $action eq 'detach') ) ) ) {
  print "Option -action not defined or has invalid parameter - $action \n";
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
  exit (1);
}


if ($action ne 'detach') {

  if ( ! ( defined($type) && defined($sourcename) && defined($dsourcename) && defined($password) && defined($source_os_user) && defined($group) && defined($dbuser)  ) ) {
    print "Options -type, -sourcename, -dsourcename, -group, -password, -source_os_user and -dbuser are required. \n";
    pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
    exit (1);
  }


  if ( ! ( ( lc $type eq 'oracle') || ( lc $type eq 'sybase') || ( lc $type eq 'mssql')) )  {
    print "Option -type has invalid parameter - $type \n";
    pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
    exit (1);
  }

  if (( $type eq 'sybase' ) && ( ! ( defined($stage_os_user) && defined($stageinst) && defined($stageenv) && defined($backup_dir) && defined($sourceinst) && defined($sourceenv) ) ) ) {
    print "Options -stage_os_user, -stageinst, -stageenv, -sourceinst, -sourceenv and -backup_dir are required. \n";
    pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
    exit (1);
  }

  if ( defined($logsync) && ( ! ( ( lc $logsync eq 'yes') || ( lc $logsync eq 'no')  ) ) ) {
    print "Options -logsync has yes and no value only. \n";
    pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
    exit (1);
  }  


} else {
  if ( ! ( defined($dsourcename)  ) ) {
    print "Options  -dsourcename is required to detach. \n";
    pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
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
    next;
  };

  my $db;
  my $jobno;
  
  my $groups = new Group_obj($engine_obj, $debug); 
  
  
  if (! defined($groups->getGroupByName($group))) {
    if (defined($creategroup)) {
      print "Creating not existing group - $group \n";
      my $jobno = $groups->createGroup($group);
      my $actionret = Toolkit_helpers::waitForAction($engine_obj, $jobno, "Action completed with success", "There were problems with group creation");
      if ($actionret > 0) {
        $ret = $ret + 1;
        print "There was a problem with group creation. Skipping source actions on engine\n";
        next;
      }
    } else {
      print "Group $group for target database doesn't exist.\n Skipping source actions on engine.\n";
      $ret = $ret + 1;
      next;
    }
  } 

  if (($action eq 'attach') || ($action eq 'detach'))  {
    my $databases = new Databases($engine_obj,$debug);
    my $source_ref = Toolkit_helpers::get_dblist_from_filter(undef, $group, undef, $dsourcename, $databases, $groups, undef, undef, undef, undef, $debug);

    if (!defined($source_ref)) {
      print "Source database not found.\n";
      $ret = $ret + 1;
      next;
    }
    elsif (scalar(@{$source_ref})>1) {
      print "Source database not unique defined.\n";
      $ret = $ret + 1;
      next;
    } elsif (scalar(@{$source_ref}) eq 0) {
      print "Source database not found.\n";
      $ret = $ret + 1;
      next;
    }

    my $source = ($databases->getDB($source_ref->[0]));


    if ($action eq 'attach') {
      $jobno = $source->attach_dsource($sourcename,$sourceinst,$sourceenv,$source_os_user,$dbuser,$password,$stageenv,$stageinst,$stage_os_user, $backup_dir);
    } else {
      $jobno = $source->detach_dsource();
    }

  } elsif ($action eq 'create') {

    if ( $type eq 'oracle' ) {
      my $db = new OracleVDB_obj($engine_obj,$debug);
      $jobno = $db->addSource($sourcename,$sourceinst,$sourceenv,$source_os_user,$dbuser,$password,$dsourcename,$group,$logsync);
    } 
    elsif ($type eq 'sybase') {
      my $db = new SybaseVDB_obj($engine_obj,$debug);
      $jobno = $db->addSource($sourcename,$sourceinst,$sourceenv,$source_os_user,$dbuser,$password,$dsourcename,$group,$logsync,$stageenv,$stageinst,$stage_os_user, $backup_dir, $dumppwd);
    } 
    elsif ($type eq 'mssql') {
      my $db = new MSSQLVDB_obj($engine_obj,$debug);
      $jobno = $db->addSource($sourcename,$sourceinst,$sourceenv,$source_os_user,$dbuser,$password,$dsourcename,$group,$logsync,$stageenv,$stageinst,$stage_os_user, $backup_dir, $dumppwd, $validatedsync, $delphixmanaged);
    }


  } 

  $ret = $ret + Toolkit_helpers::waitForAction($engine_obj, $jobno, "Action completed with success", "There were problems with dSource creation");

}

exit $ret;

__DATA__


=head1 SYNOPSIS

 dx_ctl_dsource.pl [ -engine|d <delphix identifier> | -all ]  
  -action action_name
  -type dsourcetype
  -sourcename name
  -dsourcename dsourcename
  -group groupname  
  -sourceinst source_instance
  -sourceenv source_environment
  -dbuser username
  -password password
  -source_os_user osusername
  [-logsync yes/no ]
  [-stageinst staging_inst ]
  [-stageenv staging_env ]
  [-stage_os_user staging_osuser ]
  [-backup_dir backup_dir ]
  [-dumppwd password ]
  [-validatedsync mode ]
  [-delphixmanaged yes/no ]
  [-debug ]
  [-version ]
  [-help|? ] 

=head1 DESCRIPTION

Create or attache dSource to a Delphix Engine

=head1 ARGUMENTS

=head2 Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=head2 dSource arguments

=over 1

=item B<-type>
Type (oracle|sybase)

=item B<-action>
Action - create 

=item B<-group>
Source Group Name

=item B<-sourcename>
Database Name

=item B<-dsourcename>
dSource name

=item B<-sourceinst source_instance>
Source database instance / home

=item B<-sourceenv source_environment>
Source database environment name

=item B<-dbuser username>
Database user

=item B<-password password>
Database password

=item B<-source_os_user osusername>
Source database os user

=item B<-stageinst staging_inst>
Staging database instance

=item B<-stageenv staging_env>
Staging database environment


=item B<-stage_os_user staging_osuser>
Staging database os user

=item B<-backup_dir backup_dir>
Backup location

=item B<-logsync yes/no>
Enable or no LogSync for dSource. Default LogSync is disabled.

=item B<-dumppwd password>
Password for backup used to create dsource

=item B<-validatedsync mode>
Set validated sync mode for MS SQL. Allowed values 
TRANSACTION_LOG, FULL, FULL_OR_DIFFERENTIAL
 
=item B<-delphixmanaged yes/no>
Use Delphix Manage backup mode for MS SQL

=back


=head1 OPTIONS

=over 2

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=back



=cut



