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


# class Analytic_network_obj - is a child class of Analytic_obj

package Analytic_network_obj;
use strict;
use Data::Dumper;
use Date::Manip;
use List::Util qw (sum);
use JSON;
use Toolkit_helpers qw (logger);
use Formater;
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

  logger($debug,"Entering Analytic_network_obj::constructor",1);
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

   logger($self->{_debug}, "Entering Analytic_network_obj::getData",1);
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
   my $dt = new Date::Manip::Date;
   #$dt->config("tz","GMT");
   $dt->config("setdate","zone,GMT");   
   my ($err,$date,$offset,$isdst,$abbrev);

   
   for my $ds ( @{$result->{result}{datapointStreams}} ) {
    
        # for data points in data stream
        
        my $nic;

        if (defined($ds->{networkInterface})) {
            $nic = $ds->{networkInterface};
        }
        
        
        for my $dp ( @{$ds->{datapoints}} ) {
            
            my $zulutime = $dp->{timestamp} ;
            my $ts = Toolkit_helpers::convert_from_utc($zulutime, $timezone);

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


            if (defined($nic) ) {
                 $resultset{$ts}->{$nic} = \%row;
            }
            else {
                $resultset{$ts} = \%row;
            }


        } 

   }
   
   $self->{resultset} = \%resultset;
   
   return 0;
}


sub processData {
    my $self = shift;
    my $aggregation = shift;
    my $io_obj = shift;

    logger($self->{_debug}, "Entering Analytic_network_obj::processData",1);

    undef $self->{aggreg}; 
    
    my $output = new Formater();
    
    my $resultset = $self->{resultset};
    
    my @timestamps = sort( keys %{ $resultset } );
    
    my $header;


    if (defined($io_obj)) {
        $output->addHeader(
          {'timestamp', 20},
          {'inBytes',   20},
          {'outBytes',  20},
          {'vdb_write', 20},
          {'vdb_read', 20}
        );
    } else {
      my @headerlist;
      
      push(@headerlist, {'timestamp', 20});
      push(@headerlist, {'inBytes',   20});
      push(@headerlist, {'outBytes',  20});
      push(@headerlist, {'inPackets',   20});
      push(@headerlist, {'outPackets',  20});
      
      for my $nic ( sort (keys %{$resultset->{$timestamps[0]}} )) {
        push(@headerlist, {$nic . "_inBytes", 20});
        push(@headerlist, {$nic . "_outBytes", 20});
        push(@headerlist, {$nic . "_inPackets", 20});
        push(@headerlist, {$nic . "_outPackets", 20});
      }

      $output->addHeader(
        @headerlist
      );     
    }
    
  
    if ($self->{_overflow}) {
      print "Please reduce a range. API is not able to provide all data.\n";
      print "min date " . $timestamps[0] . " max date " . $timestamps[-1] . "\n";
    } 

    for my $ts (@timestamps) {

        
        my $inBytes  = 0;
        my $outBytes  = 0;
        my $inPackets = 0;
        my $outPackets = 0;
        
        my @printarray;
        
        push(@printarray, $ts);
        
        
        my @nicarray;
        for my $nic ( sort (keys %{$resultset->{$ts}} )) {
            $inBytes = $inBytes + $resultset->{$ts}->{$nic}->{inBytes} ;
            $outBytes = $outBytes + $resultset->{$ts}->{$nic}->{outBytes};
            $inPackets = $inPackets + $resultset->{$ts}->{$nic}->{inPackets};
            $outPackets = $outPackets + $resultset->{$ts}->{$nic}->{outPackets};
            push(@nicarray, sprintf("%d",$resultset->{$ts}->{$nic}->{inBytes}));
            push(@nicarray, sprintf("%d",$resultset->{$ts}->{$nic}->{outBytes}));
            push(@nicarray, sprintf("%d",$resultset->{$ts}->{$nic}->{inPackets}));
            push(@nicarray, sprintf("%d",$resultset->{$ts}->{$nic}->{outPackets}));
            
        }

        push(@printarray, sprintf("%d",$inBytes));
        push(@printarray, sprintf("%d",$outBytes));
        push(@printarray, sprintf("%d",$inPackets));
        push(@printarray, sprintf("%d",$outPackets));
        
        push(@printarray, @nicarray);


        $self->aggregation($ts, $aggregation, 'none', 'inBytes', $inBytes);
        $self->aggregation($ts, $aggregation, 'none', 'outBytes', $outBytes);        
        
        if (defined($io_obj)) {
            my $vdb_write = $io_obj->{size_hist}->{$ts}->{wsize} ? sprintf("%d",$io_obj->{size_hist}->{$ts}->{wsize}) : 'N/A';
            my $vdb_read = $io_obj->{size_hist}->{$ts}->{rsize} ? sprintf("%d",$io_obj->{size_hist}->{$ts}->{rsize}) : 'N/A';
            $output->addLine(
                $ts , sprintf("%d",$inBytes) , sprintf("%d",$outBytes) , $vdb_write,  sprintf("%d",$vdb_read)
            );
        } else {
            $output->addLine(
                #$ts , sprintf("%d",$inBytes) , sprintf("%d",$outBytes) 
                @printarray
            );   
        }    
        
    }  

    $self->{_output} = $output;

}

# Procedure doAggregation
# parametres
# generate aggregation

sub doAggregation {
    my $self = shift;
    
    logger($self->{_debug}, "Entering Analytic_network_obj::doAggregation",1);    
    $self->doAggregation_worker('inBytes,outBytes');
      
}


# End of package
1;