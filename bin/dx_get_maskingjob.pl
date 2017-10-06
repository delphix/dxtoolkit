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
# Program Name : dx_get_maskingjob.pl
# Description  : Dislay masking jobs assinged to virtualization engine
# Author       : Marcin Przepiorowski
# Created      : 23 December 2016 (v2.3.0)
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
use MaskingJob_obj;
use Toolkit_helpers;
use Databases;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'name|n=s' => \(my $name), 
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


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $output = new Formater();
my $ret = 0;

$output->addHeader(
  {'Appliance',         20},
  {'Masking job name',  30},
  {'Assigned Database', 30}
);


for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  # load objects for current engine
  my $maskingjob_obj = new MaskingJob_obj ( $engine_obj, $debug );
  my $maskingjob_list;

  if (defined($name)) {
    my $maskingjob = $maskingjob_obj->getMaskingJobByName($name);
    if (!defined($maskingjob)) {
      $ret = $ret + 1;
      next;
    }
    my @temp_mj;
    push (@temp_mj, $maskingjob);
    $maskingjob_list = \@temp_mj;
  } else {
    $maskingjob_list = $maskingjob_obj->getMaskingJobs();
  }

  my $databases = new Databases ( $engine_obj, $debug );

  for my $mjitem ( @{$maskingjob_list} ) {
    
    my $dbref = $maskingjob_obj->getAssociatedContainer($mjitem);
    my $dbname = 'N/A';
    
    if ($dbref ne 'N/A') {
      $dbname = ($databases->getDB($dbref)->getName());
    }
    
    
    $output->addLine(
      $engine,
      $maskingjob_obj->getName($mjitem),
      $dbname
    );


  }


}

Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;



__DATA__

=head1 SYNOPSIS

 dx_get_maskingjob  [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                    [ -name maskingjob_name ] 
                    [ -format csv|json ]  
                    [ -help|? ] 
                    [ -debug ] 
                    
=head1 DESCRIPTION

List a list of masking job known to virtualization engine.

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

=item B<-name>
Masking job name

=back

=head1 OPTIONS

=over 3

=item B<-format>                                                                                                                                            
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Display list of masking jobs

 dx_get_maskingjob -d Delphix32

 Appliance            Masking job name               Assigned Database
 -------------------- ------------------------------ ------------------------------
 Delphix32            SCOTT_JOB                      TestORCL
 Delphix32            JOB2                           N/A


=cut



