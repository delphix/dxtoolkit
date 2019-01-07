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
# Copyright (c) 2014,2016 by Delphix. All rights reserved.
#
# Program Name : dx_get_capacity.pl
# Description  : Get database and host information
# Author       : Edward de los Santos
# Created      : 30 Jan 2014 (v1.0.0)
#
# Modified     : 14 Mar 2015 (v2.0.0) Marcin Przepiorowski
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
use Timeflow_obj;
use Capacity_obj;
use Formater;
use Group_obj;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'name=s' => \(my $dbname),
  'format=s' => \(my $format),
  'type=s' => \(my $type),
  'group=s' => \(my $group),
  'dsource=s' => \(my $dsource),
  'host=s' => \(my $host),
  'sortby=s' => \(my $sortby),
  'forcerefresh' => \(my $forcerefresh),
  'dbdetails'   => \(my $dbdetails),
  'debug:i' => \(my $debug),
  'details:s' => \(my $details),
  'dever=s' => \(my $dever),
  'unvirt'    => \(my $unvirt),
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


if (defined($details) && defined($dbdetails)) {
  print "Options -details and -dbdetails are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (defined($sortby) && ( ! ( (uc $sortby eq 'SIZE') ) ) ) {
  print "Option sortby can have only size \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

Toolkit_helpers::check_filer_options (undef,$type, $group, $host, $dbname);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $output = new Formater();

my @header_array = (
  {'Engine',         30},
  {'Group',          20},
  {'Database',       35},
  {'Replica',         3},
  {'Size [GB]',      10}
);

if (defined($details)) {
  push (@header_array, {'Type',20});
  push (@header_array, {'Size [GB]',10});
  if (lc $details eq 'all') {
    push (@header_array, {'Snapshots',35});
    push (@header_array, {'Size [GB]',10});
  }
} else {
  if (defined($unvirt)) {
    push (@header_array, {'Unvirt [GB]',11});
  }

  if (defined($dbdetails)) {
    push (@header_array, {'Environment name', 30});
    push (@header_array, {'Parent', 30});
  }
}

$output->addHeader(@header_array);

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    $ret = $ret + 1;
    next;
  };

  my $capacity = new Capacity_obj($engine_obj, $debug);

  if (defined($forcerefresh)) {
    if ($capacity->forcerefesh()) {
      print "Problem with forcerefesh. Skipping results for engine $engine\n";
      next;
    }
  }

  # load objects for current engine
  my $databases = new Databases( $engine_obj, $debug);
  my $groups = new Group_obj($engine_obj, $debug);

  $capacity->LoadDatabases();

  # filter implementation

  my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups, undef, $dsource, undef, undef, undef, undef, $debug);
  if (! defined($db_list)) {
    print "There is no DB selected to process on $engine . Please check filter definitions. \n";
    $ret = 1;
    next;
  }

  # for filtered databases on current engine - display status
  for my $dbitem ( @{$db_list} ) {
    my $dbobj = $databases->getDB($dbitem);
    my $capacity_hash = $capacity->getDetailedDBUsage($dbitem, $details);

    if ((defined($capacity_hash->{snapshots_shared})) && ($capacity_hash->{snapshots_shared} eq 0)) {
      # storage info not found - maybe database is deleted
      # skipping it
      next;
    }


    if (defined($details) && ($details eq '')) {
      $output->addLine(
        $engine,
        $groups->getName($dbobj->getGroup()),
        $dbobj->getName(),
        $dbobj->isReplica(),
        sprintf("%10.2f", $capacity_hash->{totalsize}),
        '',
        ''
      );

      $output->addLine(
        '',
        '',
        '',
        '',
        '',
        'Current copy',
        sprintf("%10.2f", $capacity_hash->{currentcopy})
      );

      $output->addLine(
        '',
        '',
        '',
        '',
        '',
        'DB Logs',
        sprintf("%10.2f", $capacity_hash->{dblogs})
      );

      $output->addLine(
        '',
        '',
        '',
        '',
        '',
        'Snapshots total',
        sprintf("%10.2f", $capacity_hash->{snapshots_total})
      );
    } elsif (defined($details) && (lc $details eq 'all')) {

        $output->addLine(
          $engine,
          $groups->getName($dbobj->getGroup()),
          $dbobj->getName(),
          $dbobj->isReplica(),
          sprintf("%10.2f", $capacity_hash->{totalsize}),
          '',
          '',
          '',
          ''
        );

        $output->addLine(
          '',
          '',
          '',
          '',
          '',
          'Current copy',
          sprintf("%10.2f", $capacity_hash->{currentcopy}),
          '',
          ''
        );

        $output->addLine(
          '',
          '',
          '',
          '',
          '',
          'DB Logs',
          sprintf("%10.2f", $capacity_hash->{dblogs}),
          '',
          ''
        );

        $output->addLine(
          '',
          '',
          '',
          '',
          '',
          'Snapshots total',
          sprintf("%10.2f", $capacity_hash->{snapshots_total}),
          '',
          ''
        );

        $output->addLine(
          '',
          '',
          '',
          '',
          '',
          '',
          '',
          'Snapshots shared',
          sprintf("%10.2f", $capacity_hash->{snapshots_shared})
        );

        for my $snapitem ( @{$capacity_hash->{snapshots_list}} ) {
          $output->addLine(
            '',
            '',
            '',
            '',
            '',
            '',
            '',
            'Snapshot ' . $snapitem->{snapshotTimestamp},
            sprintf("%10.2f", $snapitem->{space})
          );
        }
    } else {
      my @linearray = (
        $engine,
        $groups->getName($dbobj->getGroup()),
        $dbobj->getName(),
        $dbobj->isReplica(),
        sprintf("%10.2f", $capacity_hash->{totalsize})
      );
      if (defined($unvirt)) {
        push(@linearray, sprintf("%10.2f", $capacity_hash->{unvirtualized}));
      }

      if (defined($dbdetails)) {
          my $parentName;
          if ($dbobj->getType() eq 'VDB') {
            $parentName = $dbobj->getParentName();
            if ($parentName eq '') {
              $parentName = "N/A - deleted";
            }
          } else {
            $parentName = 'N/A - dSource';
          }
          push(@linearray, $dbobj->getEnvironmentName());
          push(@linearray, $parentName);
      }
      $output->addLine(@linearray);

    }

  }

 my $held_array = $capacity->getStorageContainers();



 if (scalar(@{$held_array}) > 0) {

   my $held_hash;

   for my $hs (@{$held_array}) {

     $held_hash = $capacity->getDetailedDBUsage($hs, undef);

     my $groupname;
     if (defined($held_hash->{group_name})) {
       $groupname = $held_hash->{group_name};
     } else {
       $groupname = 'N/A';
     }

     my @printarray = (
       $engine,
       $groupname,
       "Held space - " . $held_hash->{storageContainer},
       "N/A",
       sprintf("%10.2f", $held_hash->{totalsize})
     );

     # make sure all columns for detail view are filled with ''
     if (($output->getHeaderSize() - scalar(@printarray)) gt 0) {
       push @printarray, ('') x ($output->getHeaderSize() - scalar(@printarray)) ;
     }

     $output->addLine(
      @printarray
     );
   }
 }



}

if ( (! defined($details) ) && defined($sortby) ) {
  if (uc $sortby eq 'SIZE') {
    $output->sortbynumcolumn(4);
  }
}

Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_get_capacity    [-engine|d <delphix identifier> | -all ]
                    [-group group_name | -name db_name | -host host_name | -type dsource|vdb | -dsource name ]
                    [-details [all]]
                    [-sortby size ]
                    [-format csv|json ]
                    [-forcerefresh]
                    [-unvirt]
                    [-help|? ]
                    [-debug ]

=head1 DESCRIPTION

Get the information about databases space usage.

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

=item B<-group groupname>
Group Name

=item B<-name dbname>
Database Name

=item B<-host hostname>
Host Name

=item B<-type type>
Type (dsource|vdb)

=item B<-dsource name>
Name of dsource

=back

=head1 OPTIONS

=over 3

=item B<-forcerefresh>
Force refresh of capacity data (in >= 5.2 )

=item B<-details [all]>
Display breakdown of usage.
If all is specified, breakdown snapshot usage into individual snapshots

=item B<-sortby size>
Sort output by size of VDB - can't be mixed with -details flag
By default ourput is sorted by group name and db name

=item B<-unvirt>
Display a information about unvirtualized size of object - can't be mixed with -details flag


=item B<-format>
Display output in csv or json format
If not specified pretty formatting is used.



=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=item B<-details>
Display more information about database capacity

=item B<-nohead>
Turn off header output

=back

=head1 EXAMPLES

Display a size of databases on Delphix Engine

 dx_get_capacity -d Landshark5

 Engine                         Group                Database                       Rep Size [GB]
 ------------------------------ -------------------- ------------------------------ --- ----------
 Landshark5                     Analytics            cont1                          NO        0.05
 Landshark5                     Analytics            cont2                          NO        0.03
 Landshark5                     Analytics            test                           NO        0.01
 Landshark5                     Sources              AdventureWorksLT2008R2         NO        0.00
 Landshark5                     Sources              Oracle dsource                 NO        0.55
 Landshark5                     Sources              orcl_tar                       NO        0.49
 Landshark5                     Sources              PDB                            NO        0.24
 Landshark5                     Sources              racdba                         NO        0.53
 Landshark5                     Sources              RMAN dsource                   NO        0.47
 Landshark5                     Sources              singpdb                        NO        0.80
 Landshark5                     Sources              Sybase dsource                 NO        0.00
 Landshark5                     Sources              test_src                       NO        0.00
 Landshark5                     Test                 vFiles                         NO        0.00

Display a size of databases from group Analytics with details

 dx_get_capacity -d Landshark5 -details -group Analytics

 Engine                         Group                Database                       Rep Size [GB]  Type                 Size [GB]
 ------------------------------ -------------------- ------------------------------ --- ---------- -------------------- ----------
 Landshark5                     Analytics            cont1                          NO        0.05
                                                                                                   Current copy               0.04
                                                                                                   DB Logs                    0.00
                                                                                                   Snapshots total            0.00
 Landshark5                     Analytics            cont2                          NO        0.03
                                                                                                   Current copy               0.02
                                                                                                   DB Logs                    0.00
                                                                                                   Snapshots total            0.00
 Landshark5                     Analytics            test                           NO        0.01
                                                                                                   Current copy               0.01
                                                                                                   DB Logs                    0.00
                                                                                                   Snapshots total            0.00


Display a size of database name cont1 with snapshot details

 dx_get_capacity -d Landshark5 -details all -name cont1

 Engine                         Group                Database                       Rep Size [GB]  Type                 Size [GB]  Snapshots                           Size [GB]
 ------------------------------ -------------------- ------------------------------ --- ---------- -------------------- ---------- ----------------------------------- ----------
 Landshark5                     Analytics            cont1                          NO        0.05
                                                                                                   Current copy               0.04
                                                                                                   DB Logs                    0.00
                                                                                                   Snapshots total            0.00
                                                                                                                                   Snapshots shared                          0.00
                                                                                                                                   Snapshot 2016-10-24T18:22:56.858Z         0.00
                                                                                                                                   Snapshot 2016-10-25T07:30:00.630Z         0.00
                                                                                                                                   Snapshot 2016-10-25T07:38:02.098Z         0.00
                                                                                                                                   Snapshot 2016-10-25T07:39:37.475Z         0.00
                                                                                                                                   Snapshot 2016-10-25T07:41:33.710Z         0.00
                                                                                                                                   Snapshot 2016-10-25T07:42:11.884Z         0.00
                                                                                                                                   Snapshot 2016-10-25T07:44:21.183Z         0.00
                                                                                                                                   Snapshot 2016-10-25T07:55:52.038Z         0.00
                                                                                                                                   Snapshot 2016-10-25T07:58:04.423Z         0.00

=cut
