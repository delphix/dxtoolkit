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
# Copyright (c) 2015,2017 by Delphix. All rights reserved.
#
# Program Name : Analytics.pm
# Description  : Delphix Engine Analytics object
# Author       : Marcin Przepiorowski
# Created      : 2 Mar 2015 (v2.0.0)
#


# class Analytic_cpu_obj - is a child class of Analytic_obj

package Analytic_cpu_obj;
use strict;
use Data::Dumper;
use Date::Manip;
use List::Util qw (sum);
use JSON;
use Toolkit_helpers qw (logger);
use Formater;
use Analytic_obj;
our @ISA = qw(Analytic_obj);

# constructor
# parameters 
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
  my $class  = shift;
  my $dlpx = shift;
  my $name = shift;
  my $reference = shift;
  my $type = shift;
  my $collectionAxes = shift;
  my $collectionInterval = shift;
  my $statisticType = shift;
  my $debug = shift;

  logger($debug,"Entering Analytic_cpu_obj::constructor",1);
  # call Analytic_obj constructor
  my $self       = $class->SUPER::new($dlpx, $name, $reference, $type, $collectionAxes, $collectionInterval, $statisticType,  $debug); 
}

# Procedure getData
# parametres
# dlpx - Delphix object with connection
# additional_parms - additional parameters for webapi call (like time, resolution in URL, etc)
# resolution - data resolution
# Load analytic data from Delphix Engine into object

sub getData {
   my $self = shift;
   my $additional_parms = shift;
   my $resolution = shift;
   my $dlpx = $self->{_dlpx};

   logger($self->{_debug}, "Entering Analytic_cpu_obj::getData",1);
   my $op = "resources/json/delphix/analytics/" . $self->{_reference} . "/getData?" . $additional_parms;
   


   my ($result, $result_fmt, $retcode) = $dlpx->getJSONResult($op);
   
   if ($retcode) {
    # timeout 
    return 1;
   }
   
   if (scalar(@{$result->{result}->{datapointStreams}}) < 1) {
    # nodata
    return 2;
   }
   
   if ($result->{status} ne 'OK') {
    # unknown error
    return 3;
   }

   $self->{_overflow} = $result->{result}->{overflow};
   
   # for every data stream
   
   my %resultset;
   my $timestampfix;
   
   my $timezone = $self->{_detimezone};
   my $tz = new Date::Manip::TZ;
   my ($err,$date,$offset,$isdst,$abbrev);
   my $dt = new Date::Manip::Date;
   #$dt->config("tz","GMT");
   $dt->config("setdate","zone,GMT");
   
   for my $ds ( @{$result->{result}{datapointStreams}} ) {
    
        for my $dp ( @{$ds->{datapoints}} ) {
            
            # my $ts = $dp->{timestamp};
            # chomp($ts); 
            # $ts =~ s/T/ /;
            # $ts =~ s/\.000Z//;

            my $zulutime = $dp->{timestamp} ;
            chomp($zulutime); 
            $zulutime =~ s/T/ /;
            $zulutime =~ s/\.000Z//;          
            #$dt = ParseDate($zulutime);
            my $err = $dt->parse($zulutime);
            my $dttemp = $dt->value();

            ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($dttemp, $timezone);
            my $ts = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);


            
            # translate ts to resolution size
            
            if ($resolution eq 'H') {
                if ( ! defined ($timestampfix) ) {
                    $timestampfix = substr $ts, 13, 18;
                }
                $ts = ( substr $ts, 0, 13 ) ;  
                $ts = $ts . $timestampfix;
                logger($self->{_debug}, "ts after applying resolution size $ts",2 ); 
            }
            
            if ($resolution eq 'M') {
                if ( ! defined ($timestampfix) ) {
                    $timestampfix = substr $ts, 16, 18;
                }
                $ts = ( substr $ts, 0, 16 ) ;
                $ts = $ts . $timestampfix;
                logger($self->{_debug}, "ts after applying resolution size $ts",2 );   
            }            
                    
            my %row;
            for my $ca ( @{$self->{_collectionAxes}} ) {
                if (defined $dp->{$ca} ) {
                        $row{$ca} = $dp->{$ca};
                }
            }

            $resultset{$ts} = \%row;
        } 

   }
   
   $self->{resultset} = \%resultset;

   return 0;
   
}

# Procedure processData_cpu
# parametres
# aggregation
#
# Process cpu data and add data into aggregation

sub processData {
    my $self = shift;
    my $aggregation = shift;

    undef $self->{aggreg}; 
    logger($self->{_debug}, "Entering Analytics_cpu_obj::processData",1);
    
    my $resultset = $self->{resultset};
    
    my @timestamps = sort( keys %{ $resultset } );

    my $output = new Formater();

    $output->addHeader(
      {'timestamp', 20},
      {'util',      10}
    );
    
    if ($self->{_overflow}) {
      print "Please reduce a range. API is not able to provide all data.\n";
      print "min date " . $timestamps[0] . " max date " . $timestamps[-1] . "\n";
    } 
    
    for my $ts (@timestamps) {
        
        my $kernel  = $resultset->{$ts}->{kernel} ;
        my $user  = $resultset->{$ts}->{user};
        my $idle  = $resultset->{$ts}->{idle};
        my $ttl_cpu =  ($idle+$user+$kernel);
        my $util = ( $ttl_cpu == 0 ) ? 0 : ((($user+$kernel) / ($ttl_cpu)) * 100);
        
        $self->aggregation($ts, $aggregation, 'none', 'utilization', $util);
        $output->addLine(
            $ts, sprintf("%2.2f",$util)
        );

    }   

    $self->{_output} = $output;

}

sub doAggregation {
    my $self = shift;
    
    logger($self->{_debug}, "Entering Analytics_cpu_obj::doAggregation",1);
    $self->doAggregation_worker('utilization');

}


# End of package
1;