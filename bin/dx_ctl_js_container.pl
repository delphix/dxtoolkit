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
# Program Name : dx_ctl_js_container.pl
# Description  : Get Delphix Engine JS container
# Author       : Marcin Przepiorowski
# Created      : 02 Mar 2016 (v2.2.5)
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
use Toolkit_helpers;
use JS_template_obj;
use JS_container_obj;
use JS_branch_obj;
use JS_datasource_obj;
use Databases;
use Group_obj;
use Jobs_obj;


my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'container_name=s' => \(my $container_name),
  'template_name=s' => \(my $template_name),
  'container_def=s@' => \(my $container_def),
  'timestamp=s' => \(my $timestamp),
  'action=s' => \(my $action),
  'dropvdb=s' => \(my $dropvdb),
  'all' => (\my $all),
  'version' => \(my $print_version),
  'dever=s' => \(my $dever),
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




if (!defined($action) || ( ! ( (lc $action eq 'reset' ) || (lc $action eq 'refresh' ) || (lc $action eq 'recover' ) 
    || (lc $action eq 'create' ) || (lc $action eq 'delete' ) ) ) ) {
  print "Action parameter not specified or has a wrong value - $action \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);  
}

if (!defined($container_name)) {
  print "Container name is required \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);  
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
  
  my $jstemplates = new JS_template_obj ($engine_obj, $debug );

  my $template_ref;

  if (defined($template_name)) {
    $template_ref = $jstemplates->getJSTemplateByName($template_name);
    if (! defined($template_ref)) {
      print "Can't find template $template_name \n";
      $ret = $ret + 1;
      next;
    }
  }

  my $jobno;
  my $jscontainers = new JS_container_obj ( $engine_obj, $template_ref, $debug);
  
  if ((lc $action eq 'reset' ) || (lc $action eq 'refresh' ) || (lc $action eq 'recover' ) 
      || (lc $action eq 'delete' )) {
    my $jscontainer_ref = $jscontainers->getJSContainerByName($container_name);
    if (! defined($jscontainer_ref)) {
      print "Can't find container name $container_name\n";
      next;
    }
    
    if (lc $action eq 'reset') {
      $jobno = $jscontainers->resetContainer($jscontainer_ref);
    } elsif (lc $action eq 'refresh') {
      $jobno = $jscontainers->refreshContainer($jscontainer_ref);
    } elsif (lc $action eq 'recover') {
      $jobno = $jscontainers->recoverContainer($jscontainer_ref, $timestamp);
    } elsif (lc $action eq 'delete') {
      if (!defined($dropvdb) || ( ! ( ( lc $dropvdb eq 'yes' ) || (lc $dropvdb eq 'no' ) ) ) ) {
        print "dropvdb parameter has to be set to yes or no \n";
        pod2usage(-verbose => 1,  -input=>\*DATA);
        exit (1); 
      }
      $jobno = $jscontainers->deleteContainer($jscontainer_ref, $dropvdb);
    }
  }
  
  
  if (lc $action eq 'create') {
    if (!defined($container_def)) {
      print "Container definition parameter -container_def is required to create container \n";
      pod2usage(-verbose => 1,  -input=>\*DATA);
      exit (1);  
    }
    
    if (!defined($template_name)) {
      print "Template name has to be defined to create container \n";
      pod2usage(-verbose => 1,  -input=>\*DATA);
      exit (1);     
    }
    
    # find a source definition from template matching a VDB provided
    my $jsdatasources = new JS_datasource_obj ( $engine_obj, $template_ref, $debug );
    my $databases = new Databases($engine_obj, $debug);
    my $groups = new Group_obj($engine_obj, $debug);
    
    my @cont_array;
    
    my %dupVDBprotection;
    
    for my $coitem ( @{$container_def} ) {
            
      my @single_cont = split(',', $coitem);
      if (scalar(@single_cont) ne 2) {
        print "container_def required a 2 comma separated values - group name, database name\n";
        pod2usage(-verbose => 1,  -input=>\*DATA);
        exit (1);  
      }
      
      if (defined($dupVDBprotection{$single_cont[0].$single_cont[1]})) {
        print "VDB $single_cont[0]/$single_cont[1] is already used in other container definition. Exiting\n";
        exit(1);
      } else {
        $dupVDBprotection{$single_cont[0].$single_cont[1]} = 1;
      }
      
      for my $ds ( @{$jsdatasources->getJSDataSourceList()} ) {
        my $dsource = $jsdatasources->getJSDBContainer($ds);
        my $vdb_ref = Toolkit_helpers::get_dblist_from_filter(undef, $single_cont[0], undef, $single_cont[1], $databases, $groups, undef, undef, undef, undef, undef, undef);
        
        if ((!defined($vdb_ref)) || (scalar(@{$vdb_ref}) < 1)) {
          print "VDB $single_cont[0]/$single_cont[1] not found \n";
          exit(1)
        }

        if (scalar(@{$vdb_ref}) > 1) {
          print "VDB $single_cont[0]/$single_cont[1] is not unique \n";
          exit(1)
        }

        my $pc = ($databases->getDB($vdb_ref->[0]))->getParentContainer();

        if ($dsource eq $pc) {
          #($databases->getDB($pc))->getName(),
          my %con = (
            "source" => $jsdatasources->getName($ds),
            "vdb_ref" => $vdb_ref->[0]
          );
          push(@cont_array, \%con);
        }  
        
      }
          
    }

    if (scalar(@cont_array) ne scalar(@{$container_def})) {
      print "Not all VDBs mapped to sources. Exiting\n";
      exit(1);
    }

    if (scalar(@{$jsdatasources->getJSDataSourceList()}) ne scalar(@cont_array)) {
      print "Template definition contain more sources than VDBs provided.Exiting\n";
      exit(1);
    }
    
    $jobno = $jscontainers->createContainer($container_name, $template_ref, \@cont_array);

  }



  if (defined ($jobno) ) {
    print "Starting job $jobno for container $container_name.\n";
    my $job = new Jobs_obj($engine_obj, $jobno, 'true', $debug);

    my $jobstat = $job->waitForJob();
    if ($jobstat ne 'COMPLETED') {
      $ret = $ret + 1;
    }
  } else {
    print "Job for container is not created. \n";
    $ret = $ret + 1;
  }

}


exit $ret;



__DATA__

=head1 SYNOPSIS

 dx_ctl_js_container    [ -engine|d <delphix identifier> | -all ] 
                        -action reset|refresh|recover|create
                        -container_name container_name 
                        [-container_def GroupName,VDBName]
                        [-template_name template_name]  
                        [-timestamp timestamp]
                        [-dropvdb yes|no]
                        [ -help|? ] [ -debug ]

=head1 DESCRIPTION

Run a action on the JetStream container

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 3

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=item B<-action reset|refresh|recover|create|delete>
Run a action on the container

=over 3

=item B<-create> - create JS container 

=item B<-delete> - create JS container 

=item B<-reset> - reset JS container to latest point

=item B<-refresh> - refresh JS container from latest point of template

=item B<-recover> - recover a JS container to point in time or bookmark

=back

=item B<-container_name container_name>
Name of container to run action on 

=item B<-template_name template_name>
Name of container's templates

=back

=head1 OPTIONS

=over 3

=item B<-container_def GroupName,VDBName>
Specify a VDB which will be used for  container.
This parameter can be repeated if more than one VDB is required.

=item B<-timestamp "YYYY-MM-DD HH24:MI:SS" or bookmark name >                                                                                                                                            
Use timestamp or bookmark name to recover container

=item B<-dropvdb yes|no>
Drop VDB when deleteing container

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging


=back

=head1 Examples

Reset of the container

 dx_ctl_js_container -d Landshark5 -container_name cont -action reset
 Starting job JOB-5043 for container cont.
 0 - 3 - 4 - 23 - 26 - 29 - 30 - 34 - 47 - 52 - 54 - 57 - 58 - 59 - 60 - 61 - 76 - 90 - 100 
 Job JOB-5043 finished with state: COMPLETED
 
 
Refresh of the container from latest point in time in source
 
 dx_ctl_js_container -d Landshark5 -container_name cont -action refresh
 Starting job JOB-5050 for container cont.
 0 - 3 - 4 - 12 - 26 - 29 - 30 - 34 - 45 - 47 - 52 - 54 - 57 - 58 - 59 - 60 - 61 - 70 - 77 - 83 - 100 
 Job JOB-5050 finished with state: COMPLETED

Recover of the cointainer to a bookmark "fixeddate"

 dx_ctl_js_container -d Landshark5 -container_name cont1 -action recover -timestamp fixeddate
 Starting job JOB-7637 for container cont1.
 0 - 3 - 4 - 23 - 26 - 29 - 30 - 34 - 45 - 47 - 52 - 54 - 57 - 58 - 59 - 60 - 61 - 68 - 77 - 82 - 100
 Job JOB-7637 finished with state: COMPLETED

=cut



