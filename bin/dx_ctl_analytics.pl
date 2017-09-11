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
# Program Name : dx_ctl_analytics.pl
# Description  : Control analytics inside Delphix Engine
# Author       : Marcin Przepiorowski
# Created      : 01 Jun 2016 (v2.0.0)
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
use Analytics;
use Formater;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \( my$help), 
  'd|engine=s' => \(my $dx_host), 
  'debug:i' => \(my $debug), 
  'all' => (\my $all),
  'type|t=s' => (\my $type),
  'action=s' => (\my $action),
  'format=s' => \(my $format), 
  'nohead' => \(my $nohead),
  'dever=s' => \(my $dever),
  'version' => \(my $print_version),
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


if (! (defined($type) && defined($action) ) ) {
  print "Option -action and -type are mandatory \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


if ( ! ( (lc $action eq 'create') || (lc $action eq 'delete') || (lc $action eq 'display') || (lc $action eq 'stop') || (lc $action eq 'start') || (lc $action eq 'restart') ) ) {
  print "Option -action has a wrong argument $action \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if ( ( (lc $action eq 'create') || (lc $action eq 'delete') ) &&  ( ! ( (lc $type eq 'nfs-all') || (lc $type eq 'nfs-by-client') || (lc $type eq 'iscsi-by-client') ) ) ) {
  print "Create or delete action can be done with those types only : nfs-all, nfs-by-client or iscsi-by-client  \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $engine\n\n";
    next;
  } else {
    print "Connected to Delphix Engine $engine (IP " . $engine_obj->getIP() .")\n\n";
  }


  my $analytic_list = new Analytics($engine_obj, $debug);

  if ($action eq 'create') {
      if ($analytic_list->create_analytic($type)) {
        $ret = $ret + 1;
        next;
      }  
  } elsif ($action eq 'delete') {
      my $analytic = $analytic_list->getAnalyticByName($type);
      if (defined($analytic)) {
        if ($analytic->delete_analytic()) {
          $ret = $ret + 1;
          next;         
        }
      } else {
        print "Analytics $type not found\n";
        $ret = $ret + 1;
        next;          
      }
  } else {


    my @analytic_array;
    if (lc $type eq 'all') {
        for my $ref (@{$analytic_list->getAnalyticsList()}) {
            push(@analytic_array, $analytic_list->getName($ref));
        }
    } elsif (lc $type eq 'standard') {
        push(@analytic_array, 'cpu');
        push(@analytic_array, 'network');
        push(@analytic_array, 'disk');
        push(@analytic_array, 'nfs');
    } else {

        my @a = split (',', $type);

        for my $n (@a) {
            if (defined($analytic_list->getAnalyticByName($n))) {
                push(@analytic_array, $n);
            } else {
                print "Analytic name $n not found. It will be not included in output \n";
            }
        }
    }

    if (scalar(@analytic_array) < 1) {
        print "Can't find an analytic\n";
        $ret = $ret + 1;
        next;        
    }


    my $output = new Formater();
    $output->addHeader(
        {'Engine',         20},
        {'Analytic',       20},
        {'State',          20},
        {'Axes',          100}
    );


    for my $n (sort @analytic_array) {

        my $anal = $analytic_list->getAnalyticByName($n);

        if ($action eq 'display' ) {
          $output->addLine(
            $engine,
            $n,
            $anal->getState(),
            $anal->getAxes()
          );

        } elsif ($action eq 'stop') {
          $anal->pause_analytic();
        } elsif ($action eq 'start') {
          $anal->resume_analytic();
        } elsif ($action eq 'restart') {
          $anal->pause_analytic();
          $anal->resume_analytic();
        }

    }

    if ($action eq 'display') {
      Toolkit_helpers::print_output($output, $format, $nohead);
    }

  }

}




exit $ret;



__DATA__

=head1 SYNOPSIS

 dx_ctl_analytics ( -d <delphix identifier> | -all ) 
                       -type <cpu|disk|nfs|iscsi|network|nfs-by-client|nfs-all|all|standard|comma separated names> 
                       -action start|stop|restart|display|create|delete
                       [-format csv|json]
                       [-debug]

=head1 DESCRIPTION

Control analytics collector inside Delphix Engine
  
=head1 ARGUMENTS

=over 4

=item B<-type|t> Type: cpu|disk|nfs|iscsi|network|nfs-by-client|nfs-all|all|standard|comma separated names

ex.

=over 4

=item B<-type all> - for all analytics

=item B<-type standard> - for cpu,disk,network and nfs analytics

=item B<-t cpu,disk> - for cpu and disk

=back 

=item B<-action start|stop|restart|display|create|delete>

Choose action on selected analytic type

Custom analytics can be created or deleted using create or delete operation and following types: nfs-by-client, nfs-all


=back

=head1 OPTIONS

=over 4

=item B<-format csv|json>                                                                                                                                            
Display output in csv or json format
If not specified csv formatting is used.

=item B<-nohead>
Turn off header output

=item B<-help>          
Print this screen

=item B<-debug>          
Turn on debugging

=back

=head1 EXAMPLES

Restart all collectors

 dx_ctl_analytics -d Landshark5 -action restart -type all
 Connected to Delphix Engine Landshark5 (IP 172.16.180.131)
 Analytic default.cpu has been stopped 
 Analytic default.cpu has been started 
 Analytic default.disk has been stopped 
 Analytic default.disk has been started 
 Analytic default.iscsi has been stopped 
 Analytic default.iscsi has been started 
 Analytic iscsi-by-client has been stopped 
 Analytic iscsi-by-client has been started 
 Analytic default.network has been stopped 
 Analytic default.network has been started 
 Analytic default.nfs has been stopped 
 Analytic default.nfs has been started 
 Analytic nfs-all has been stopped 
 Analytic nfs-all has been started 
 Analytic nfs-by-client has been stopped 
 Analytic nfs-by-client has been started 
 Analytic default.tcp has been stopped 
 Analytic default.tcp has been started
 
Create new collector - nfs-all

 dx_ctl_analytics -d Landshark5 -action create -type nfs-all 
 Connected to Delphix Engine Landshark5 (IP 172.16.180.131) 
 New analytic nfs-all has been created


Display collectors specified as a comma separated list

 dx_ctl_analytics -d Landshark5 -action display -type cpu,disk,nfs 
 Connected to Delphix Engine Landshark5 (IP 172.16.180.131)
 Engine         Analytic   State    Axes 
 -------------- ---------- -------- -------------------------------------------------------
 Landshark5     cpu        RUNNING  idle,user,kernel
 Landshark5     disk       RUNNING  latency,avgLatency,throughput,count,op
 Landshark5     nfs        RUNNING  latency,throughput,count,op

=cut