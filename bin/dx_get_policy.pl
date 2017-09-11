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
# Copyright (c) 2015,2016 by Delphix. All rights reserved.
# 
# Program Name : dx_get_policy.pl
# Description  : Get Delphix Engine policies
# Author       : Marcin Przepiorowski
# Created      : 01 Oct 2015 (v2.2.0)
#
# 

use strict;
use warnings;
use JSON;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev ); #avoids conflicts with ex host and help
use File::Basename;
use Pod::Usage;
use FindBin;
use Data::Dumper;


my $abspath = $FindBin::Bin;

use lib '../lib';
use Engine;
use Formater;
use Toolkit_helpers;
use Action_obj;
use Policy_obj;
use Databases;
use Group_obj;

my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'type=s' => \(my $type),
  'name=s' => \(my $dbname),  
  'policy=s' => \(my $policytype), 
  'policyname=s' => \(my $policyname),
  'group=s' => \(my $group), 
  'host=s' => \(my $host),
  'envname=s' => \(my $envname),
  'format=s' => \(my $format), 
  'outdir=s' => \(my $outdir),
  'export' => \(my $export),
  'mapping=s' => \(my $mapping),
  'all' => (\my $all),
  'dever=s' => \(my $dever),
  'version' => \(my $print_version),
  'nohead' => \(my $nohead),
  'debug:i' => \(my $debug),
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

if (defined($export) && ( ! defined($outdir) ) ) {
  print "Option export require option outdir to be specified \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (defined($mapping) && ( $mapping eq '' ) ) {
  print "Option mapping require value to be specified \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


if ((! defined($export)) && (! defined($mapping)) && (! defined($policytype)) && (! defined($policyname))) {
  print "Option policy or policyname is required\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


if ( defined($policytype) && ( ! ( (uc $policytype eq 'RETENTION') || (uc $policytype eq 'SNAPSYNC') || (uc $policytype eq 'SNAPSHOT') || (uc $policytype eq 'REFRESH') ) ) )  {
  print "Option policy can have only RETENTION, SNAPSYNC, SNAPSHOT or REFRESH value\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $output = new Formater();


if (! (defined($export) || defined($mapping) ) ) {
  if (defined($policyname)) {
    $output->addHeader(
          {'Appliance',     20},
          {'Policy Name',   30},
          {'Policy Type',   30},
          {'Policy schedule', 100}
      );
  } else {
    $output->addHeader(
        {'Appliance',   20},
        {'Group',       15},   
        {'Database',    30},
        {ucfirst (lc $policytype),   30},
        {ucfirst (lc $policytype) . ' schedule', 100}
    );
  }
}

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    $ret = $ret + 1;
    next;
  };

  my $policy = new Policy_obj( $engine_obj, $debug);
  my $db = new Databases ( $engine_obj, $debug );
  my $groups = new Group_obj ( $engine_obj, $debug );


  my $contref;
  my %cont_type;

  if (! ( defined($type) || defined($group) || defined($host) || defined($dbname) || defined($envname) ) ) {

    my @cont;
    my @groups_array = @{$groups->getPrimaryGroupList()};
    %cont_type  = map { $_ => 'group' } @groups_array;

    for my $groupitem ( @groups_array) {
      push (@cont, $groupitem);
      my @temp = $db->getDBForGroup($groupitem);
      my %temp_type = map { $_ => $db->getDB($_)->getType() } @temp;  
      %cont_type = (%temp_type, %cont_type);
      push (@cont, @temp);
    }

    $contref = \@cont;
  } else {
       
    $contref = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $db, $groups, $envname, undef, undef, undef, undef, $debug);
    if (! defined($contref)) {
      next;
    }
    %cont_type = map { $_ => $db->getDB($_)->getType() } @{$contref};  
  }


  $policy->loadPolicyMapping($contref);


  if (defined($policyname)) {

    my $polref = $policy->getPolicyByName($policyname);

    if (defined($polref)) {
      $output->addLine (
        $engine,
        $policy->getName($polref),        
        $policy->getType($polref),
        $policy->getSchedule($polref)
      );
      if (defined($export)) {
        $policy->exportPolicy($polref, $outdir);
      }
    } else {
      print "Policy $policyname not found\n";
      $ret = $ret + 1;
    }

  } else {

    if (defined($export)) {
        for my $polref ( $policy->getPolicyList() ) {
          $policy->exportPolicy($polref, $outdir);
        }

        if (defined($mapping)) {
          $policy->exportMapping($mapping, $groups,$db);
        }     

    } elsif (defined($mapping)) {
          $policy->exportMapping($mapping, $groups,$db);
    } else {
      for my $contitem ( @{$contref} ) {

        my $polref;

        if (uc $policytype eq 'SNAPSYNC') {
          $polref = $policy->getSnapSync($contitem, $cont_type{$contitem});
        } elsif (uc $policytype eq 'SNAPSHOT') {
          $polref = $policy->getSnapshot($contitem, $cont_type{$contitem});
        } elsif (uc $policytype eq 'REFRESH') {
          $polref = $policy->getRefresh($contitem, $cont_type{$contitem});
        } elsif (uc $policytype eq 'RETENTION') {
          $polref = $policy->getRetention($contitem);
        }

        my $groupname; 
        my $dbname;


        if ($cont_type{$contitem} eq 'group') {
          $groupname = $groups->getName($contitem);
          $dbname = ' ';
        } else {
          $dbname = $db->getDB($contitem)->getName();
          $groupname = ' ';
        }

        $output->addLine (
          $engine,
          $groupname,
          $dbname,
          $policy->getName($polref, $policy->isInherited($polref, $contitem)),
          $policy->getSchedule($polref)
        );

      }
    }

  }

}


if ((! defined($export) ) && (! defined($mapping)) )  {
  Toolkit_helpers::print_output($output, $format, $nohead);
}

exit $ret;

__DATA__

=head1 SYNOPSIS

 dx_get_policy [ -engine|d <delphix identifier> | -all ] -policy ( RETENTION | SNAPSYNC | SNAPSHOT  | REFRESH ) | -policyname name [ -group group_name | -name db_name | -host host_name | -type dsource|vdb | -envname environment ] [-export -ourdir dir] [-mapping file] [ -format csv|json ]  [ --help|? ] [ -debug ]

=head1 DESCRIPTION

Get the list of policies from Delphix Engine.

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=item B<-policy type>
Choose policy type to display - RETENTION | SNAPSYNC | SNAPSHOT  | REFRESH 

=back

=head2 Filters

Filter databases using one of the following filters

=over 4

=item B<-policyname>
Policy Name

=item B<-group>
Group Name

=item B<-name>
Database Name

=item B<-host>
Host Name

=item B<-type>
Type (dsource|vdb)

=item B<-envname>
Environment name

=back

=head1 OPTIONS

=over 3

=item B<-export>                                                                                                                                            
Export all polices or policy selected by policyname

=item B<-outdir dir>                                                                                                                                            
Direcotry where policies will be exported

=item B<-mapping filename>                                                                                                                                            
Export mapping between policies and database / groups into filename.
Use a database filters like name, group, etc to limit mapping export to the particular objects

=item B<-format>                                                                                                                                            
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=item B<-nohead>
Turn off header output

=back

=head1 EXAMPLES

Display a retention policy

 dx_get_policy -d DE001 -policy RETENTION

 Appliance            Group           Database                       Retention                      Retention schedule
 -------------------- --------------- ------------------------------ ------------------------------ ----------------------------------------------------------------------------------------------------
 DE001                Sources                                        Default Retention              Logs 1 day(s), snapshots 1 week(s)
 DE001                                vasmsrc1                       Default Retention              Logs 1 day(s), snapshots 1 week(s)
 DE001                Untitled                                       Default Retention              Logs 1 day(s), snapshots 1 week(s)
 DE001                                SLOB                           * Default Retention            Logs 1 day(s), snapshots 1 week(s)
 DE001                                SLOB1                          * Default Retention            Logs 1 day(s), snapshots 1 week(s)
 DE001                                Vvas_DA3                       * Default Retention            Logs 1 day(s), snapshots 1 week(s)
 DE001                                installs                       * Default Retention            Logs 1 day(s), snapshots 1 week(s)

Export polices and mapping into files 

 dx_get_policy -d Landshark -export -outdir /tmp/policy -mapping /tmp/policy/mapping.Landshark
 Exporting policy into file /tmp/policy/Default Retention.policy
 Exporting policy into file /tmp/policy/Default Snapshot.policy
 Exporting policy into file /tmp/policy/Default SnapSync.policy
 Exporting mapping into file /tmp/policy/mapping.Landshark


=cut



