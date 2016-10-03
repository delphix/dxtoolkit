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

# Program Name : dx_get_storage_tests.pl
# Description  : Get storage test results
# Author       : Marcin Przepiorowski
# Created      : 01 Sep 2016 (v2.0.0)
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

my $abspath = $FindBin::Bin;

use lib '../lib';
use Engine;
use Formater;
use Toolkit_helpers;
use Storage_obj;


my $version = $Toolkit_helpers::version;
my $gradeonly = 'yes';

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'iorc=s' => \(my $iorc),
  'details' => \(my $details),
  'testid=s'  => \(my $testid),
  'gradeonly=s' => \($gradeonly),
  'format=s' => \(my $format),
  'debug:i' => \(my $debug),
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'nohead' => \(my $nohead),
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

if (defined($iorc) && defined($details)) {
   print "Options -iorc and -details are mutually exclusive \n";
   pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
   exit (1);
}

if ( ! ( ( lc $gradeonly eq 'yes') || (lc $gradeonly eq 'no' ) ) ) {
   print "Option -gradeonly has a wrong value - $gradeonly \n";
   pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
   exit (1);   
}



my $output = new Formater();

if (defined($details)) {

  $output->addHeader(
      {'engine name',          35},
      {'test id',              15},
      {'test name',            30},
      {'IOPS',                 10}, 
      {'Throughput',           15},
      {'Grade',                 7},
      {'average',              10},
      {'95pct',                10},
      {'minimum',              10},
      {'maximum',              10},
      {'stddev',               10}
  );
  
} else {
  $output->addHeader(
      {'engine name',          35},
      {'test id',              15},
      {'start time',           30},
      {'status',               10}
  );
}

my @fulltests = (
"Random 4K Read w/ 8 jobs",    
"Random 4K Read w/ 16 jobs",   
"Random 4K Read w/ 32 jobs",   
"Random 4K Read w/ 64 jobs",   
"Random 8K Read w/ 8 jobs",    
"Random 8K Read w/ 16 jobs",   
"Random 8K Read w/ 32 jobs",   
"Random 8K Read w/ 64 jobs",   
"Sequential 1K Write w/ 4 jobs",    
"Sequential 4K Write w/ 4 jobs",    
"Sequential 8K Write w/ 4 jobs",    
"Sequential 16K Write w/ 4 jobs",   
"Sequential 32K Write w/ 4 jobs",   
"Sequential 64K Write w/ 4 jobs",   
"Sequential 128K Write w/ 4 jobs",  
"Sequential 1M Write w/ 4 jobs",    
"Sequential 1K Write w/ 16 jobs",   
"Sequential 4K Write w/ 16 jobs",   
"Sequential 8K Write w/ 16 jobs",   
"Sequential 16K Write w/ 16 jobs",  
"Sequential 32K Write w/ 16 jobs",  
"Sequential 64K Write w/ 16 jobs",  
"Sequential 128K Write w/ 16 jobs", 
"Sequential 1M Write w/ 16 jobs",   
"Sequential 64K Read w/ 4 jobs",    
"Sequential 64K Read w/ 8 jobs",    
"Sequential 64K Read w/ 16 jobs",   
"Sequential 64K Read w/ 32 jobs",   
"Sequential 64K Read w/ 64 jobs",   
"Sequential 128K Read w/ 4 jobs",   
"Sequential 128K Read w/ 8 jobs",   
"Sequential 128K Read w/ 16 jobs",  
"Sequential 128K Read w/ 32 jobs",  
"Sequential 128K Read w/ 64 jobs",  
"Sequential 1M Read w/ 4 jobs",     
"Sequential 1M Read w/ 8 jobs",     
"Sequential 1M Read w/ 16 jobs",    
"Sequential 1M Read w/ 32 jobs",    
"Sequential 1M Read w/ 64 jobs"    
);

my @gradetests = (   
"Random 4K Read w/ 16 jobs",       
"Random 8K Read w/ 16 jobs",     
"Sequential 1K Write w/ 4 jobs",      
"Sequential 128K Write w/ 4 jobs",      
"Sequential 1M Read w/ 4 jobs",     
);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
   # main loop for all work
   if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
   };
  
   my $st = new Storage_obj ($engine_obj, $debug);
   
   my @testidloop;
   
   if (defined($testid)) {
     if (defined($st->isTestExist($testid))) {
       push(@testidloop, $testid);
     } else {
       print "Test id - $testid doesn't exist on engine $engine\n";
       $ret = $ret + 1;
       next;
     }
   } else {
     @testidloop = @{$st->getTestList()};
   }
   
   for my $testiditem ( @testidloop ) {

     if (defined($details)) {
       if ($st->getState($testiditem) eq 'COMPLETED') {
         $st->parseTestResults($testiditem);
         my @testnameloop;
         if (lc $gradeonly eq 'yes') {
           @testnameloop = @gradetests;
         } else {
           @testnameloop = @fulltests;
         }
         for my $testname (@testnameloop) {
           $output->addLine(
             $engine,
             $testiditem,
             $testname,
             $st->getTestIOPS($testiditem, $testname),
             $st->getTestThoughput($testiditem, $testname),
             $st->getLatencyGrade($testiditem, $testname),
             $st->getLatencyAvg($testiditem, $testname),
             $st->getLatency95($testiditem, $testname),
             $st->getLatencyMin($testiditem, $testname),
             $st->getLatencyMax($testiditem, $testname),
             $st->getLatencyStdDev($testiditem, $testname)
           );

         }
       } else {
         print "Test id - $testiditem is not completed on $engine\n";
         $ret = $ret + 1;
         next;       
       }
     } elsif (defined($iorc)) {
       if ($st->getState($testiditem) eq 'COMPLETED') {
         if (! -d $iorc) {
           print "$iorc is not a directory \n";
         }
           
         if ( -w $iorc ) {
           my $filename =  File::Spec->catfile($iorc, 'IORC_' . $engine . "_" . $testiditem . ".txt"); 
           if ($st->generateIORC($testiditem, $filename)) {
             print "Problem with generating a IORC $filename \n";
             $ret = $ret + 1;
             next;
           } else {
             print "IORC saved into file $filename \n";
           };
         } else {
           print "Can't write into directory " . $iorc . "\n";
           $ret = $ret + 1;
           next;
         }
       } else {
         print "Test id - $testiditem is not completed on $engine\n";
         $ret = $ret + 1;
         next;       
       }
     } else {
       $output->addLine(
        $engine,
        $testiditem,
        $st->getStartTime($testiditem),
        $st->getState($testiditem)
       );
     };
   }

}

if (!defined($iorc)) {
  Toolkit_helpers::print_output($output, $format, $nohead);
}

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_get_storage_tests.pl [-engine|d <delphix identifier> | -all ] 
                         [-iorc path]
                         [-testid ref] 
                         [-details] 
                         [-gradeonly yes/no]
                         [-format csv|json]  
                         [-help|? ] [ -debug ]

=head1 DESCRIPTION

Get the results of the network tests

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=head1 OPTIONS

=over 3

=item B<-testid ref>
Limit displayed tests to ref

=item B<-iorc path>
Extract IORC card into path 

=item B<-details>
Display details for storage test

=item B<-gradeonly yes/no>
Display results with grades (default) or all results

=item B<-format>
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back




=cut
