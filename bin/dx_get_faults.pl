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
# Program Name : dx_get_faults.pl
# Description  : Get Delphix Engine faults
# Author       : Edward de los Santos
# Created      : 30 Jan 2014 (v1.0.0)
#
# Modified     : 20 Jul 2015 (v2.0.0) Marcin Przepiorowski
# 

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
use Toolkit_helpers;
use Faults_obj;

my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \$help, 
  'd|engine=s' => \(my $dx_host), 
  'st=s' => \(my $st), 
  'et=s' => \(my $et), 
  'target=s' => \(my $target),
  'severity=s' => \(my $severity),
  'status=s' => \(my $status), 
  'outdir=s' => \(my $outdir),
  'format=s' => \(my $format), 
  'all' => (\my $all),
  'version' => \(my $print_version),
  'dever=s' => \(my $dever),
  'nohead' => \(my $nohead),
  'debug:i' => \(my $debug)
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


if (defined($status) && ( ! ( (uc $status eq 'ACTIVE') || (uc $status eq 'RESOLVED') || (uc $status eq 'IGNORED') ) ) ) {
  print "Option status can have only ACTIVE, IGNORED and RESOLVED value\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (defined($severity) && ( ! ( (uc $severity eq 'WARNING') || (uc $severity eq 'CRITICAL') ) ) ) {
  print "Option severity can have only WARNING and CRITICAL value\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $output = new Formater();


$output->addHeader(
    {'Appliance',  20},
    {'Fault ref',  20},
    {'Status',  10},
    {'Date Diagnosed', 25},
    {'Severity',       8},   
    {'Target',  55},
    {'Title', 35}
);


for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  if ($status eq 'IGNORED') {
  # this is for 4.2 >
    if ($engine_obj->getApi() le '1.5') {
      print "Status IGNORED is allowed for Delphix Engine version 4.3 or higher\n";
      pod2usage(-verbose => 1,  -input=>\*DATA);
      exit (1); 
    }

  }


  if (! defined($st_timestamp = Toolkit_helpers::timestamp($st, $engine_obj))) {
    print "Wrong start time (st) format \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);  
  }

  my $et_timestamp;

  if (defined($et)) {
    $et = Toolkit_helpers::timestamp_to_timestamp_with_de_timezone($et, $engine_obj);
    if (! defined($et_timestamp = Toolkit_helpers::timestamp($et, $engine_obj))) {
      print "Wrong end time (et) format \n";
      pod2usage(-verbose => 1,  -input=>\*DATA);
      exit (1);  
    } 
  }

  my $faults = new Faults_obj($engine_obj, $st_timestamp, $et_timestamp,  uc $status, uc $severity);

  for my $fault ( @{ $faults->getFaultsList('asc') } ) {

    my $faultTarget = $faults->getTarget($fault);
    
    if (defined($target)) {

      # if like is defined we are going to resolve only ones maching like
      if ( ! ($faultTarget =~ m/\Q$target/)  ) {
        next;
      } 

    }

    $output->addLine(
        $engine,
        $fault,
        $faults->getStatus($fault),
        $faults->getTimeWithTZ($fault),
        $faults->getSeverity($fault),
        $faults->getTarget($fault),
        $faults->getTitle($fault)
    );
  }
}

if (defined($outdir)) {
  Toolkit_helpers::write_to_dir($output, $format, $nohead,'faults',$outdir,1);
} else {
  Toolkit_helpers::print_output($output, $format, $nohead);
}


__DATA__

=head1 SYNOPSIS

 dx_get_faults    [ -engine|d <delphix identifier> | -all ] 
                  [-st timestamp] 
                  [-et timestamp] 
                  [-severity severity] 
                  [-status status] 
                  [-target target]
                  [-format csv|json ]
                  [-outdir path]  
                  [-help|? ] [ -debug ]

=head1 DESCRIPTION

Get the list faults from Delphix Engine.

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=head2 Filters

Filter faults using one of the following filters

=over 4

=item B<-severity>
Fault severity - WARNING / CRITICAL

=item B<-status>
Fault status - ACTIVE / RESOLVED

=item B<-target>
Fault target ( VDB name, target host name)

=back

=head1 OPTIONS

=over 3


=item B<-st timestamp>
Start time for faults list. Format "YYYY-MM-DD [HH24:MI:SS]". Default value is "now - 7 days"

=item B<-et timestamp>
End time for faults list. Format "YYYY-MM-DD [HH24:MI:SS]"

=item B<-format>                                                                                                                                            
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-outdir path>                                                                                                                                            
Write output into a directory specified by path.
Files names will include a timestamp and type name


=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=item B<-nohead>
Turn off header output

=back

=head1 EXAMPLES

Display faults from Delphix Engine

 dx_get_faults -d Landshark5

 Appliance            Fault ref            Status     Date Diagnosed            Severity Target                                                  Title
 -------------------- -------------------- ---------- ------------------------- -------- ------------------------------------------------------- -----------------------------------
 Landshark5           FAULT-518            ACTIVE     2016-11-01 17:20:10 GMT   WARNING  Analytics/cont2                                         Unable to connect to remote databas
 Landshark5           FAULT-519            ACTIVE     2016-11-01 17:20:10 GMT   WARNING  Analytics/cont1                                         Unable to connect to remote databas
 Landshark5           FAULT-520            RESOLVED   2016-11-01 17:20:10 GMT   WARNING  Test/vFiles                                             An error occurred during policy enf
 Landshark5           FAULT-521            RESOLVED   2016-11-01 17:20:10 GMT   WARNING  Test/vFiles                                             An error occurred during policy enf
 Landshark5           FAULT-522            ACTIVE     2016-11-01 17:21:16 GMT   WARNING  rmantest                                                Oracle home not found
 Landshark5           FAULT-523            RESOLVED   2016-11-08 12:57:43 GMT   WARNING  test                                                    Oracle home not found
 Landshark5           FAULT-524            ACTIVE     2016-11-08 13:13:10 GMT   WARNING  test                                                    Oracle home not found

Export faults from Delphix Engine into file

 dx_get_faults -d Landshark5 -outdir /tmp
 Data exported into /tmp/faults-20161108-16-11-58.txt

=cut



