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
# Copyright (c) 2014,2017 by Delphix. All rights reserved.
#
# Program Name : dx_get_hierarchy.pl
# Description  : Get database hierarchy
# Author       : Marcin Przepiorowski
# Created: 15 Jan 2017 (v2.3.1)
#


use strict;
use warnings;
use JSON;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev); #avoids conflicts with ex host and help
use File::Basename;
use Pod::Usage;
use FindBin;
use Data::Dumper;
use File::Spec;
use Date::Manip;

my $abspath = $FindBin::Bin;

use lib '../lib';
use Databases;
use Engine;
use Timeflow_obj;
use Formater;
use Group_obj;
use Toolkit_helpers;
use Snapshot_obj;
use Replication_obj;

my $version = $Toolkit_helpers::version;



GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'name=s' => \(my $dbname),
  'format=s' => \(my $format),
  'type=s' => \(my $type),
  'group=s' => \(my $group),
  'host=s' => \(my $host),
  'dsource=s' => \(my $dsource),
  'primary' => \(my $primary),
  'envname=s' => \(my $envname),
  'instance=n' => \(my $instance),
  'instancename=s' => \(my $instancename),
  'parent_engine=s' => \(my $parent_engine),
  'printhierarchy:s' => \(my $printhierarchy),
  'debug:i' => \(my $debug),
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'version' => \(my $print_version),
  'nohead' => \(my $nohead),
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

if (defined($printhierarchy) && (! ( (lc $printhierarchy eq 'p2c') || (lc $printhierarchy eq 'c2p') || (lc $printhierarchy eq '') )  ) ) {
  print "Option printhierarchy has a wrong argument - $printhierarchy \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
} 


Toolkit_helpers::check_filer_options (undef, $type, $group, $host, $dbname, $envname);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $output = new Formater();
my $dsource_output;

my $parentlast_head;
my $hostenv_head;


$output->addHeader(
      {'Appliance',   10},
      {'Database',    30},
      {'Group',       15},
      {'Type',         8},
      {'dSource',     30},
      {'dS time',     35},
      {'Physical DB', 30},
      {'First child DB', 30}
    );



my $ret = 0;

my $databases_parent;
my $snapshots_parent;
my $timeflows_parent;
my $groups_parent;
my $engine_parent;
my $replication_parent;

if (defined($parent_engine)) {

  $engine_parent = new Engine ($dever, $debug);
  $engine_parent->load_config($config_file);
  $engine_parent->dlpx_connect($parent_engine);
  
  # load objects for current engine
  $databases_parent = new Databases( $engine_parent, $debug);
  $groups_parent = new Group_obj($engine_parent, $debug);
  
  $snapshots_parent = new Snapshot_obj($engine_parent, undef, undef, $debug);
  $timeflows_parent = new Timeflow_obj($engine_parent, $debug);
  $replication_parent = new Replication_obj ($engine_parent, $debug);
}  


for my $engine ( sort (@{$engine_list}) ) {

  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $engine\n\n";
    next;
  };

  # load objects for current engine
  my $databases = new Databases( $engine_obj, $debug);
  my $groups = new Group_obj($engine_obj, $debug);

  my $snapshots = new Snapshot_obj($engine_obj, undef, undef, $debug);
  my $timeflows = new Timeflow_obj($engine_obj, $debug);  
  

  my $object_map;

  
  if (defined($parent_engine)) {
    $object_map = $databases->{_namespace}->generate_replicate_mapping($engine_parent, $timeflows_parent, $replication_parent);
  }  



  # filter implementation

  my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups, $envname, $dsource, $primary, $instance, $instancename, $debug);
  if (! defined($db_list)) {
    print "There is no DB selected to process on $engine . Please check filter definitions. \n";
    $ret = $ret + 1;
    next;
  }

  
  my %dbs = (
    'l' => $databases,
    'p' => $databases_parent
  );
  
  my %tfs = (
    'l' => $timeflows,
    'p' => $timeflows_parent
  );
  
  my %snps = (
    'l' => $snapshots,
    'p' => $snapshots_parent
  );


  my $hier = $timeflows->generateHierarchy($object_map, $timeflows_parent, $databases);
  
  my $hierc = $databases->generateHierarchy($object_map, $databases_parent);
  

  my $parentname;
  
  # for filtered databases on current engine - display status
  for my $dbitem ( @{$db_list} ) {
    my $dbobj = $databases->getDB($dbitem);
    my $groupname = $groups->getName($dbobj->getGroup());

    my $snaptime;
    my $timezone;
    my $childname;
    my $physicaldb;
    
    my $dsource_snapforchild;
    
    if (defined($printhierarchy)) {
      my $arr = $databases->returnHierarchy($dbitem, $hierc);
            
      my @printarr;
      
      for my $hi (@{$arr}) {
        my $db = $dbs{$hi->{source}}->getDB($hi->{ref})->getName();
        push(@printarr, $db);
      }
       
      my @revarray; 
       
      if (lc $printhierarchy eq 'p2c') {
        my @revarray = reverse @printarr;
        print $engine . " : " . join(' --> ', @revarray) . "\n";
      } 
      elsif ((lc $printhierarchy eq 'c2p') || ($printhierarchy eq '')) {
        print $engine . " : " . join(' --> ', @printarr) . "\n";
      }


    } else {
          
      if ($dbobj->getType() eq 'VDB') {
        my ($topds, $child);
        my ($topdsc, $childc);
        ($topds, $child) = $timeflows->finddSource($dbobj->getCurrentTimeflow(), $hier);
        ($topdsc, $childc) = $databases->finddSource($dbitem, $hierc);

              
        if (defined($topdsc)) {
          if ($topdsc eq 'deleted') {
            $parentname = 'parent deleted';
            $physicaldb = 'N/A';
          } elsif ($topdsc eq 'notlocal') {
            $parentname = 'dSource on other DE';
            $physicaldb = 'N/A';
          } else {           
            $parentname = ($dbs{$hierc->{$topdsc}->{source}})->getDB($topdsc)->getName();
            $physicaldb = ($dbs{$hierc->{$topdsc}->{source}})->getDB($topdsc)->getSourceConfigName();
          }
        } else {
          print "no dSource found - error ?\n";
          $ret = $ret + 1;
          next;
        }
        
        #check time 
        if (defined($child)) {        
          my $childdb = ($tfs{$hier->{$topds}->{source}})->getContainer($child);
          my $cobj = ($dbs{$hier->{$child}->{source}})->getDB($childdb);
          $childname = $cobj->getName();
          $dsource_snapforchild = ($tfs{$hier->{$child}->{source}})->getParentSnapshot($child);

          if (($dsource_snapforchild ne '') && ($cobj->getType() eq 'VDB')) {
            
            my $timestamp = ($tfs{$hier->{$child}->{source}})->getParentPointTimestamp($child);
            my $loc = ($tfs{$hier->{$child}->{source}})->getParentPointLocation($child);
            my $lastsnaploc = ($snps{$hier->{$child}->{source}})->getlatestChangePoint($dsource_snapforchild);
            my $timestampwithtz;
                        
            if (defined($timestamp)) {
              $timezone = ($snps{$hier->{$child}->{source}})->getSnapshotTimeZone($dsource_snapforchild);
              if ($timezone ne 'N/A') {
                
                $snaptime = Toolkit_helpers::convert_from_utc($timestamp, $timezone, 1);
                
                if (!defined($snaptime)) {
                  $snaptime = 'N/A';
                }
                
              } else {
                $snaptime = 'N/A - unknown timezone';
              }
            } elsif ( $loc != $lastsnaploc) {
              $snaptime = $loc;
            } else {
              ($snaptime,$timezone) = ($snps{$hier->{$child}->{source}})->getSnapshotTimewithzone($dsource_snapforchild);
            }
            
            
          } else {
            $snaptime = 'N/A';
          }

        } else {
          $dsource_snapforchild = '';
          if ($topdsc eq 'notlocal') {
            $snaptime = 'N/A'
          } else {
            $snaptime = 'N/A - timeflow deleted';            
          }
          $childname = 'N/A';
        }  
              
      } else {
        $dsource_snapforchild = '';
        $snaptime = 'N/A';
        $childname = 'N/A';
        $parentname = '';    
        $physicaldb = $dbobj->getSourceConfigName();
      }
      
      $output->addLine(
          $engine,
          $dbobj->getName(),
          $groupname,
          $dbobj->getType(),
          $parentname,
          $snaptime,
          $physicaldb,
          $childname
        );
      }

  
}

}


if (!defined($printhierarchy)) {
  Toolkit_helpers::print_output($output, $format, $nohead);  
}

exit $ret;



__DATA__

=head1 SYNOPSIS

 dx_get_hierarchy [-engine|d <delphix identifier> | -all ] 
                  [-group group_name | -name db_name | -host host_name | -type dsource|vdb | -instancename instname] 
                  [-parent_engine <delphix identifier>]
                  [-printhierarchy [c2p|p2c]]
                  [-format csv|json ] 
                  [-help|? ] [ -debug ]

=head1 DESCRIPTION

Get the information about databases hierarchy

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

Filter databases using one of the following filters

=over 4

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

=item B<-dsource dsourcename>
Dsource name

=item B<-instancename instname>
Instance name 

=back

=head3 Instance option

Specify a instance number (only with combination with host)

=over 4

=item B<-instance inst_no>
Instance number

=back

=head1 OPTIONS

=over 3

=item B<-parent_engine delphix identifier>
When replication between engines is configured, specify a parent engine to the engine from -d parameter.

=item B<-printhierarchy>
Display a hierarchy of databases (from VDB to dSource) or (from dSource to VDB)
c2p represents "child to parent". 
p2c represents "parent to child"
If no value is provided this "child to parent" is used.

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

=head1 Output columns

dS snapshot - time of dSource snapshot used to create/refresh a VDB. If there is a chain of VDB's it's a snapshot used to create first one
Physical DB - name of physical database used to create dSource                   
First child DB - name of first VDB in chain. 

=head1 EXAMPLES

Print database hierarchy on single engine

 dx_get_hierarchy -d Landshark51

 Appliance  Database                       Group           Type     dSource                        dS snapshot                         Physical DB                    First child DB
 ---------- ------------------------------ --------------- -------- ------------------------------ ----------------------------------- ------------------------------ ------------------------------
 Landshark5 autotest                       Analytics       VDB      Oracle dsource                 2017-02-08 09:00:35 EST             orcl                           autotest
 Landshark5 AdventureWorksLT2008R2         Sources         dSource                                 N/A                                 AdventureWorksLT2008R2         N/A
 Landshark5 Oracle dsource                 Sources         dSource                                 N/A                                 orcl                           N/A
 Landshark5 racdba                         Sources         dSource                                 N/A                                 racdba                         N/A
 Landshark5 Sybase dsource                 Sources         dSource                                 N/A                                 pubs3                          N/A


Print database hierarchy on replication target engine 

 dx_get_hierarchy -d Delphix33

 Appliance  Database                       Group           Type     dSource                        dS snapshot                         Physical DB                    First child DB
 ---------- ------------------------------ --------------- -------- ------------------------------ ----------------------------------- ------------------------------ ------------------------------
 Delphix33  racdb@delphix32-2              Sources@delphix dSource                                 N/A                                 racdb                          N/A
 Delphix33  mask1@delphix32-4              Test@delphix32- VDB      dSource on other DE            N/A                                 N/A                            N/A
 Delphix33  maskedms@delphix32-4           Test@delphix32- VDB      dSource on other DE            N/A                                 N/A                            N/A
 Delphix33  cloneMSmas                     Untitled        VDB      dSource on other DE            N/A                                 N/A                            N/A
 Delphix33  mask1clone                     Untitled        VDB      dSource on other DE            N/A                                 N/A                            N/A
 Delphix33  maskclone                      Untitled        VDB      parent deleted                 N/A                                 N/A                            N/A
 Delphix33  Vracdb_70C                     Untitled        VDB      racdb@delphix32-2              2016-09-28 14:57:16 GMT             racdb                          Vracdb_70C

Print database hierarchy on replication target engine with connection to replication source

dx_get_hierarchy -d Delphix33 -parent_engine Delphix32

 Appliance  Database                       Group           Type     dSource                        dS snapshot                         Physical DB                    First child DB
 ---------- ------------------------------ --------------- -------- ------------------------------ ----------------------------------- ------------------------------ ------------------------------
 Delphix33  racdb@delphix32-2              Sources@delphix dSource                                 N/A                                 racdb                          N/A
 Delphix33  mask1@delphix32-4              Test@delphix32- VDB      test1                          2017-01-30 13:02:53 GMT             test1                          man
 Delphix33  maskedms@delphix32-4           Test@delphix32- VDB      tpcc                           2017-02-03 12:05:15 GMT             tpcc                           maskedms
 Delphix33  cloneMSmas                     Untitled        VDB      tpcc                           2017-02-03 12:05:15 GMT             tpcc                           maskedms
 Delphix33  mask1clone                     Untitled        VDB      test1                          2017-01-30 09:15:28 GMT             test1                          man
 Delphix33  maskclone                      Untitled        VDB      parent deleted                 N/A                                 N/A                            N/A
 Delphix33  Vracdb_70C                     Untitled        VDB      racdb@delphix32-2              2016-09-28 14:57:16 GMT             racdb                          Vracdb_70C

Print database hierarchy chain starting with VDB up to dSource (c2p)

 dx_get_hierarchy -d Delphix33 -parent_engine Delphix32 -printhierarchy c2p
 Delphix33 : racdb@delphix32-2
 Delphix33 : mask1@delphix32-4 --> mask1 --> man --> test1
 Delphix33 : maskedms@delphix32-4 --> maskedms --> tpcc
 Delphix33 : cloneMSmas --> maskedms@delphix32-4 --> maskedms --> tpcc
 Delphix33 : mask1clone --> mask1@delphix32-4
 Delphix33 : maskclone
 Delphix33 : Vracdb_70C --> racdb@delphix32-2
 
 Print database hierarchy chain starting with dSource up to VDB (p2c)

 dx_get_hierarchy -d Delphix33 -parent_engine Delphix32 -printhierarchy p2c
 Delphix33 : racdb@delphix32-2
 Delphix33 : test1 --> man --> mask1 --> mask1@delphix32-4
 Delphix33 : tpcc --> maskedms --> maskedms@delphix32-4
 Delphix33 : tpcc --> maskedms --> maskedms@delphix32-4 --> cloneMSmas
 Delphix33 : mask1@delphix32-4 --> mask1clone
 Delphix33 : maskclone
 Delphix33 : racdb@delphix32-2 --> Vracdb_70C

=cut
