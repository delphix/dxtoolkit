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
  'host=s' => \(my $host),
  'sortby=s' => \(my $sortby),
  'debug:i' => \(my $debug), 
  'details:s' => \(my $details),
  'dever=s' => \(my $dever),
  'unvirt'    => \(my $unvirt),
  'all' => (\my $all),
  'version' => \(my $print_version),
  'nohead' => \(my $nohead)
) or pod2usage(-verbose => 1,  -input=>\*DATA);

pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   

my $engine_obj = new Engine ($dever, $debug);
my $path = $FindBin::Bin;
my $config_file = $path . '/dxtools.conf';

$engine_obj->load_config($config_file);


if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
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

if (defined($details)) {
  if (lc $details eq 'all') {
    $output->addHeader(
      {'Engine',         30},
      {'Group',          20},
      {'Database',       30},
      {'Replica',         3},
      {'Size [GB]',      10},
      {'Type',           20},
      {'Size [GB]',      10},
      {'Snapshots',      35}, 
      {'Size [GB]',      10}  
    );
  } else {
    $output->addHeader(
      {'Engine',         30},
      {'Group',          20},
      {'Database',       30},
      {'Replica',         3},
      {'Size [GB]',      10},
      {'Type',           20},
      {'Size [GB]',      10}
    );  
  }
} else {
  if (defined($unvirt)) {
    $output->addHeader(
      {'Engine',         30},
      {'Group',          20},
      {'Database',       30},
      {'Replica',         3},
      {'Size [GB]',      10},
      {'Unvirt [GB]',    11}
    );
  } else {
     $output->addHeader(
      {'Engine',         30},
      {'Group',          20},
      {'Database',       30},
      {'Replica',         3},
      {'Size [GB]',      10}
    );   
  }
}

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  # load objects for current engine
  my $databases = new Databases( $engine_obj, $debug);
  my $capacity = new Capacity_obj($engine_obj, $debug);
  my $groups = new Group_obj($engine_obj, $debug);  

  # filter implementation 

  my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups);
  if (! defined($db_list)) {
    print "There is no DB selected to process on $engine . Please check filter definitions. \n";
    $ret = 1;
    next;
  }

#  my @db_sorted = sort { Toolkit_helpers::sort_by_dbname($a,$b,$databases,$groups) } ;


  # for filtered databases on current engine - display status
  for my $dbitem ( @{$db_list} ) {
    my $dbobj = $databases->getDB($dbitem);
    my $capacity_hash = $capacity->getDetailedDBUsage($dbitem, $details);

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
            sprintf("%10.2f", $snapitem->{snapshot_usedspace})
          );
        }
    } else {
      if (defined($unvirt)) {
        $output->addLine(
          $engine,
          $groups->getName($dbobj->getGroup()),
          $dbobj->getName(),
          $dbobj->isReplica(),
          sprintf("%10.2f", $capacity_hash->{totalsize}),
          sprintf("%10.2f", $capacity_hash->{unvirtualized})
        );   
      } else {
        $output->addLine(
          $engine,
          $groups->getName($dbobj->getGroup()),
          $dbobj->getName(),
          $dbobj->isReplica(),
          sprintf("%10.2f", $capacity_hash->{totalsize})
        );           
      }
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

 dx_get_capacity.pl [ -engine|d <delphix identifier> | -all ] [ -group group_name | -name db_name | -host host_name | -type dsource|vdb ] [-details [all]] 
                    [-sortby size ][ -format csv|json ]  [ --help|? ] [ -debug ]

=head1 DESCRIPTION

Get the information about databases space usage.

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

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

=back

=head1 OPTIONS

=over 3

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




=cut



