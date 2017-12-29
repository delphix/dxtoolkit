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
# Program Name : Analytics.pm
# Description  : Delphix Engine Analytics object
# Author       : Marcin Przepiorowski
# Created      : 2 Mar 2015 (v2.0.0)
#


package Analytic_obj;
use strict;
use warnings;
use Data::Dumper;
use Date::Manip;
use List::Util qw (sum0);
use JSON;
use Toolkit_helpers qw (logger);
use Formater;

# constructor
# Parameters
# _name - name of collection
# _reference - reference of collection
# _type - type of collection       
# _collection_axies - collection axies
# _collectionInterval - collection interval
# _statisticType - statistic type
# _debug - debug flag


sub new
{
   my $class = shift;
   my $self = {
        _dlpx => shift,
        _name => shift,
        _reference => shift,
        _type => shift,
        _collectionAxes => shift,
        _collectionInterval => shift,
        _statisticType => shift,
        _debug => shift
   };
   logger($self->{_debug}, "Entering Analytics_obj::constructor",1);
   bless $self, $class;
   my $timezone;
   if (defined($self->{_dlpx})) {
     $timezone = $self->{_dlpx}->getTimezone();
   }
   $self->{_detimezone} = $timezone;
   return $self;
}


# Procedure printDetails_banner
# parameters: none
# Print header of analytics list from Delphix Engine

sub printDetails_banner {
    my $self = shift;
    logger($self->{_debug}, "Entering Analytics_obj::printDetails_banner",1);
    print "Name             StatisticType          CollectionInterval  CollectionAxes\n";
}

# Procedure printDetails
# parameters: none
# Print line analytics list from Delphix Engine

sub printDetails {
    my $self = shift;
    logger($self->{_debug}, "Entering Analytics_obj::printDetails",1);
    printf "%-15.15s  %-20.20s   %-18.18s  %-60.60s\n",$self->getName(),$self->{_statisticType},$self->{_collectionInterval},join(',', @{ $self->{_collectionAxes} });
}


# Procedure getType
# parameters: none
# Return type of analytic object 

sub getType {
    my $self = shift;
    logger($self->{_debug}, "Entering Analytics_obj::getType",1);
    my $ret = $self->{_statisticType};
    return $ret;
}

# Procedure getInterval
# parameters: none
# Return collection interval of analytic object 

sub getInterval {
    my $self = shift;
    logger($self->{_debug}, "Entering Analytics_obj::getInterval",1);
    my $ret = $self->{_collectionInterval};
    return $ret;
}

# Procedure getAxes
# parameters: none
# Return collection axes of analytic object (joined by ,)

sub getAxes {
    my $self = shift;
    logger($self->{_debug}, "Entering Analytics_obj::getAxes",1);
    my $ret = join(',', @{ $self->{_collectionAxes} });
    return $ret;
}

# Procedure getState
# parameters: none
# Return state of analytic object (joined by ,)

sub getState {
    my $self = shift;
    logger($self->{_debug}, "Entering Analytics_obj::getState",1);
    my $operation = "resources/json/delphix/analytics/" . $self->{_reference} ;
    my ($result, $result_fmt) = $self->{_dlpx}->getJSONResult($operation);

    my $ret;

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        $ret = $result->{result}->{state};
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }

    return $ret;
}


# Procedure getName
# parameters: none
# Return name of analytic object - (it is removing default. from name)

sub getName {
    my $self = shift;
    logger($self->{_debug}, "Entering Analytics_obj::getName",1);
    my $ret = $self->{_name};
    $ret =~ s/default\.//;
    return $ret;
}


# Procedure print
# parameter - file 
# Print raw data

sub print {
    my $self = shift;
    my $file = shift;
    my $format = shift;
    logger($self->{_debug}, "Entering Analytics_obj::print",1);
    if (defined($file)) {
        if (defined($format) && (lc $format eq 'json')) {
            $self->{_output}->savejson($file);
        } else {
            $self->{_output}->savecsv(undef,$file);
        }
    } else {
        $self->{_output}->print();
    }
}

# Procedure print_aggregation
# parameter - file 
# Print aggregated data into csv 

sub print_aggregation {
    my $self = shift;
    my $file = shift;
    my $format = shift;
    logger($self->{_debug}, "Entering Analytics_obj::print_aggregation",1);
    if (defined($file)) {
        if (defined($format) && (lc $format eq 'json')) {
            $self->{_output_aggregation}->savejson($file);
        } else {
            $self->{_output_aggregation}->savecsv(undef,$file);
        }
    } else {
        $self->{_output_aggregation}->print();
    }
}



# Procedure add_histogram
# parametres
# target - target hash
# add - hash being added to target
#
# Add two hash objects together and keep result in target hash
# used to add latency histograms

sub add_histogram {
    my $self = shift;
    my $target = shift;
    my $add = shift;

    logger($self->{_debug}, "Entering Analytics_obj::add_histogram",1);
    for my $i ( keys %{$add } ) {
        if ( defined ($target->{$i} ) ) {
            $target->{$i} = $add->{$i} + $target->{$i};
        } else {
            $target->{$i} = $add->{$i};
        }
    }
}


# Procedure add_row
# parametres
# ts - time stamp
# row - row hash
# dc - client
# op - operation
# ca - cached
# 
# Procedure used for test of Analytic objects
# is adding data into object instead of reading from Delphix Engine

sub add_row {
    my $self = shift;
    my $ts = shift;
    my $row = shift;
    my $dc = shift; # client
    my $op = shift; # operation
    my $ca = shift; #cached
    my $nic = shift;
    my $rs;
    my $cache;
    
    logger($self->{_debug}, "Entering Analytics_obj::add_row",1);
    
    my $cached = defined ( $ca ) ? $ca : "none";
    
    if (defined ($self->{resultset})) {
        $rs = $self->{resultset};
        logger($self->{_debug}, "Add new row", 2);        
    } else {
        my %resultset;
        $self->{resultset} = \%resultset;
        logger($self->{_debug}, "Add new resultset", 2);
    }
    
    if (ref($row->{'latency'}) eq 'HASH') {
        $row->{'latency'} = $row->{'latency'};
    }

    if ( defined($dc) && defined ($op) ) {
        $self->{resultset}->{$ts}->{$dc}->{$cached}->{$op} =  $row;
    } 
    elsif (defined($nic)) {
        $self->{resultset}->{$ts}->{$nic} = $row ;
    }
    else {
        $self->{resultset}->{$ts} = $row ;
    }
    
    
}


# Procedure calc_avg
# parametres
# arr - array of values
#
# return avg value for a arr

sub calc_avg {
    my $self = shift;
    my $arr = shift;

    logger($self->{_debug}, "Entering Analytics_obj::calc_avg",1);
    return defined($arr) ? sprintf("%2.2f",sum0(@{$arr})/scalar(@{$arr}) ) : 0  ;
    
}

# Procedure calc_percentile
# parametres
# arr - array of values
# pct - percentile
#
# return percentile value for a arr
# using Nearest Rank method

sub calc_percentile {
    # using  Nearest Rank method
    my $self = shift;
    my $arr = shift;
    my $pct = shift;

    logger($self->{_debug}, "Entering Analytics_obj::calc_percentile",1);
    my @sorted_array = sort {$a <=> $b} ( @{$arr} );
    
    return $sorted_array[sprintf("%.0f",($pct*($#sorted_array)))] ? $sorted_array[sprintf("%.0f",($pct*($#sorted_array)))] : 0  ;
    
}


# Procedure calculate_size
# parametres
# size_hash - hash of latency
#
# return sum of size hash

sub calculate_size {
    my $self = shift;
    my $size_hash = shift;
    logger($self->{_debug}, "Entering Analytics_obj::calculate_size",1);

    my ( $sumSize )  = 0;

    if (defined ($size_hash)) {

        while ( my ($key,$value) = each %{$size_hash} ) {
            $sumSize = $sumSize + $key * $value;
        }
    }
    return $sumSize;
    
}

# Procedure calculate_latency
# parametres
# latency_hash - hash of latency
#
# return avg value for a histogram of latency
# using middle points

sub calculate_latency {
    my $self = shift;
    my $latency_hash = shift;
    logger($self->{_debug}, "Entering Analytics_obj::calculate_latency",1);

    
    my $sumCount = 0;
    my $sumLatency = 0;
    my $latency ;
    while ( my ($key,$value) = each %{$latency_hash} ) {

        if ( $key eq "< 10000" ) {  #There's a very low bucket marked < 10000.  We'll workaround this and set it equal to 1000
            $key = 1000;
        }

        my $base;
        if ( $key > 0 ) {
            $sumCount += $value;
            $base = int (log($key)/log(10)  +0.00000001 );

            my $sub = 10**($base-1) * 5;

            my $partLatency = $key + $sub;

            $sumLatency += ($partLatency * $value);  
        }
    }
        
    $latency = (($sumLatency == 0) && ( $sumCount == 0 )) ? undef : ($sumLatency / $sumCount); 
    
    #print Dumper sprintf("%.2f", $latency);

    if (defined($latency)) {
      return sprintf("%.2f", $latency);  
    } else {
      return $latency;
    }
    
}


# Procedure metric_desc
# parametres
# name
# Return human description of metric
sub metric_desc {
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering Analytics_obj::metric_desc",1);

    my %metric_names = (
      throughput_t => $self->getName() . " throughput MB/s",
      throughput_r => $self->getName() . " throughput MB/s",
      throughput_w => $self->getName() . " throughput MB/s",
      latency_r => $self->getName() . " latency milliseconds ",
      latency_w => $self->getName() . " latency milliseconds ",
      latency_t => $self->getName() . " latency milliseconds ",
      utilization => $self->getName() . " utilization"
    );

    return $metric_names{$name};
}


# Procedure doAggregation_worker
# parametres
# stat_list - list of statistic to print - comma separated list, ex. throughput_r - collection list in print_data
#
# Calculate and print aggregated data into csv file 

sub doAggregation_worker {
    my $self = shift;
    my $stat_list = shift; # comma separated list, ex. throughput_r - collection list in print_data

    logger($self->{_debug}, "Entering Analytics_obj::doAggregation_worker",1);

    my @timestamps =  sort ( keys %{ $self->{aggreg} } ); # take all aggregated time stamps
    
    my @stat_list_array = split(",", $stat_list);
    
    my $header;
    
    
    if (defined($timestamps[0])) {
      if ( $self->{aggreg}->{$timestamps[0]}->{none} ) {
          $header = "time";
      } else {
          $header = "time,client";
      }
    } else {
      $header = "time";
    }
    
    for my $stat ( @stat_list_array ) {
        $header = $header . "," . $stat . "_min," . $stat . "_max," . $stat . "_85pct" ;
    }
    


    #print $FD $header . "\n";

    my $output = new Formater();

    my @header_array;

    for my $h ( split (',' , $header) ) {
        my %header_line = (
            $h => '10'
        );
        push (@header_array, \%header_line )
    }

    $output->addHeader( @header_array );

    for my $ts ( @timestamps ) {
        my $line = $ts ;
        #print $FD "$ts";

        for my $client ( sort ( keys %{ $self->{aggreg}->{$ts} }  ) ) { # all clients

            if ($client ne "none" ) {
                #print $FD ",$client";
                $line = $line . ",$client";
            }

            for my $stat ( @stat_list_array ) {

                if (defined($self->{aggreg}->{$ts}->{$client}->{$stat})) {
                  my @values = sort  {$a <=> $b} ( @{ $self->{aggreg}->{$ts}->{$client}->{$stat} } );



                  #printf $FD ",%2.2f,%2.2f,%2.2f",$values[0],$values[$#values],$self->calc_percentile (\@values, 0.85);
                  $line = $line . sprintf( ",%2.2f,%2.2f,%2.2f",$values[0],$values[$#values],$self->calc_percentile (\@values, 0.85) );
                } else {
                  $line = $line . "N/A, N/A, N/A"
                }

            }
            #print $FD "\n";
            $output->addLine( split (',', $line));
            $line = $ts;
        }
    }
    
    $self->{_output_aggregation} = $output;
    
}


# Procedure get_avg
# parametres
# stat - statistic name
#
# Return avg for all timestamps (used for 5 min avg)

sub get_avg {
    my $self = shift;
    my $stat = shift; 
    my $client = shift;

    logger($self->{_debug}, "Entering Analytics_obj::get_avg",1);

    if (! defined($client) ) {
      $client = 'none';
    }

    my $timestamp = ( keys %{ $self->{aggreg} } )[0]; # take time stamp - we aggregate for century here so there will be only one timestamp
    
    if (!defined($timestamp)) {
      return -1;
    }
    
    if (! defined ($self->{aggreg}->{$timestamp}->{$client})) {
      return -1;
    }

    my @values = sort  {$a <=> $b} ( @{ $self->{aggreg}->{$timestamp}->{$client}->{$stat} } );
                
    my $avg =  sprintf( "%2.2f",$self->calc_avg (\@values) );

    return $avg;
    
}


# Procedure get_stats
# parametres
# stat - statistic name
#
# Return avg for all timestamps (used for 5 min avg)

sub get_stats {
    my $self = shift;
    my $stat = shift; 
    my $client = shift;

    logger($self->{_debug}, "Entering Analytics_obj::get_stats",1);

    if (! defined($client) ) {
      $client = 'none';
    }

    my $timestamp = ( keys %{ $self->{aggreg} } )[0]; # take time stamp - we aggregate for century here so there will be only one timestamp
    
    if (! defined ($timestamp) ) {
      return (0, 0, 0, 0);
    }
    
    if (! defined ($self->{aggreg}->{$timestamp}->{$client})) {
      return (0, 0, 0, 0);
    }

    my @values = sort  {$a <=> $b} ( @{ $self->{aggreg}->{$timestamp}->{$client}->{$stat} } );
                
    my $avg = sprintf( "%2.2f",$self->calc_avg (\@values) );
    my $min = sprintf( "%2.2f",$values[0] );
    my $max = sprintf( "%2.2f",$values[$#values] );
    my $per85 = sprintf( "%2.2f",$self->calc_percentile (\@values, 0.85) );
    
    return ($avg, $min, $max, $per85);
    
}


# Procedure aggregation
# parametres
# ts - timestamp
# aggegation_size - number of characters from ts to do aggregation on (10 for a daily)
# client - aggregation client
# stat_name - statistic name to do aggregation on
# value - value of statistic in particular ts
#
# Aggregate (put) a value into buckets specified by ts and aggregation_size

sub aggregation {
    my $self = shift;
    my $ts = shift;
    my $aggegation_size =  shift;
    my $client = shift;
    my $stat_name = shift;
    my $value = shift;
    

    logger($self->{_debug}, "Entering Analytics_obj::aggregation",1);

    my $current_agg_period;
    
    if ( defined ( $self->{_current_agg_period} ) ) {
        $current_agg_period = $self->{_current_agg_period};
    } else {
        $current_agg_period = substr $ts, 0, $aggegation_size;
        $self->{_current_agg_period} = $current_agg_period;
    }
    
    my $ts_agg = substr $ts, 0, $aggegation_size;
    
    logger($self->{_debug}, "TS agg $ts_agg",2);
    
    if ( $ts_agg =~ m/$current_agg_period/ ) {
        $current_agg_period = $self->{_current_agg_period};
    } else {
        $current_agg_period = substr $ts, 0, $aggegation_size;
        $self->{_current_agg_period} = $current_agg_period;          
    }
    
    
    if ( ! defined ( $self->{aggreg}->{$ts_agg} ) ) {
        my @stat = ( $value );
        my %temp = (
            $client => {
                $stat_name =>  \@stat
            } 
        );
        $self->{aggreg}->{$ts_agg} = \%temp;
    } 
    elsif ( defined ( $self->{aggreg}->{$ts_agg}->{$client}->{$stat_name}  ) ) {
        push (@{$self->{aggreg}->{$ts_agg}->{$client}->{$stat_name}}, $value);  
        #defined($self->{_debug}) ? print "add row to stat $stat_name $value \n" : 0;     
    } else {
        my @stat = ( $value );
        $self->{aggreg}->{$ts_agg}->{$client}->{$stat_name} = \@stat;
        #defined($self->{_debug}) ?  print "new row to stat $stat_name $value \n" : 0;  
    } 
    
        
}




# Procedure delete_analytic
# parametres
#
# Delete analytic from Delphix Engine

sub delete_analytic {
    my $self = shift;
    logger($self->{_debug}, "Entering Analytics_obj::delete_analytic",1);
    
    my $operation = "resources/json/delphix/analytics/" . $self->{_reference} . "/delete";
    my($result, $result_fmt) =$self->{_dlpx}->postJSONData($operation,"{}");       
    my $status = $result->{status};

    if ( $status ne "OK"  ) {
          print "Error: $result->{error}{details}\n";
          return 1;
    }
    else {
          print "Analytic $self->{_name}  has been deleted\n";
          return 0;
    }

        
}


# Procedure pause_analytic
# parametres
#
# Pause analytic from Delphix Engine

sub pause_analytic {
    my $self = shift;
    logger($self->{_debug}, "Entering Analytics_obj::pause_analytic",1);
    
    my $operation = "resources/json/delphix/analytics/" . $self->{_reference} . "/pause";
    my($result, $result_fmt) =$self->{_dlpx}->postJSONData($operation,"{}");       
    my $status = $result->{status};

    if ( $status ne "OK"  ) {
          print "Error: $result->{error}{details}\n";
          return 1;
    }
    else {
          print "Analytic $self->{_name} has been stopped\n";
          return 0;
    }

        
}


# Procedure resume_analytic
# parametres
#
# Pause analytic from Delphix Engine

sub resume_analytic {
    my $self = shift;
    logger($self->{_debug}, "Entering Analytics_obj::resume_analytic",1);
    
    my $operation = "resources/json/delphix/analytics/" . $self->{_reference} . "/resume";
    my($result, $result_fmt) =$self->{_dlpx}->postJSONData($operation,"{}");       
    my $status = $result->{status};

    if ( $status ne "OK"  ) {
          print "Error: $result->{error}{details}\n";
          return 1;
    }
    else {
          print "Analytic $self->{_name} has been started\n";
          return 0;
    }

        
}



#######################
# end of Analytic_obj class
#######################

# 
# # class Analytic_io_obj - is a child class of Analytic_obj
# 
# package Analytic_io_obj;
# use strict;
# use Data::Dumper;
# use Date::Manip;
# use List::Util qw (sum);
# use JSON;
# use Toolkit_helpers qw (logger);
# use Formater;
# our @ISA = qw(Analytic_obj);
# 
# # constructor
# # parameters 
# # - dlpxObject - connection to DE
# # - debug - debug flag (debug on if defined)
# 
# sub new {
#   my $class  = shift;
#   my $dlpx = shift;
#   my $name = shift;
#   my $reference = shift;
#   my $type = shift;
#   my $collectionAxes = shift;
#   my $collectionInterval = shift;
#   my $statisticType = shift;
#   my $debug = shift;
# 
#   logger($debug,"Entering Analytic_io_obj::constructor",1);
#   # call Analytic_obj constructor
#   my $self       = $class->SUPER::new($dlpx, $name, $reference, $type, $collectionAxes, $collectionInterval, $statisticType,  $debug); 
# }
# 
# # Procedure getData
# # parametres
# # dlpx - Delphix object with connection
# # additional_parms - additional parameters for webapi call (like time, resolution in URL, etc)
# # resolution - data resolution
# # Load analytic data from Delphix Engine into object
# 
# sub getData {
#    my $self = shift;
#    my $additional_parms = shift;
#    my $resolution = shift;
#    my $dlpx = $self->{_dlpx};
# 
#    logger($self->{_debug}, "Entering Analytic_io_obj::getData",1);
#    my $op = "resources/json/delphix/analytics/" . $self->{_reference} . "/getData?" . $additional_parms;
#    
#    
#    my ($result, $result_fmt, $retcode) = $dlpx->getJSONResult($op);
# 
#    if ($retcode) {
#     return 1;
#    }
# 
#    $self->{_overflow} = $result->{result}->{overflow};
#       
# 
#    # for every data stream
#    
#    my %resultset;
#    my $timestampfix;
#    
# 
#    my $timezone = $self->{_detimezone};
#    my $tz = new Date::Manip::TZ;
#    my $dt = new Date::Manip::Date;
#    my ($err,$date,$offset,$isdst,$abbrev);
# 
#    #$dt->config("tz","GMT");
#    $dt->config("setdate","zone,GMT");
# 
#    for my $ds ( @{$result->{result}{datapointStreams}} ) {
#     
#         # for data points in data stream
#         
#         # device / client switch
#         my $dc = "none";
#         my $cache = "none";
#         
#         my $op = $ds->{op};
#         
#         $self->{op} = $op;
#         
#         my $client = defined ($ds->{client} ) ? $dc = $ds->{client} : "none"; 
#         my $device = defined ($ds->{device} ) ? $dc = $ds->{device} : "none";
#         my $cached = defined ($ds->{cached} ) ? $cache = $ds->{cached} : "none";
# 
#         
#         logger($self->{_debug}, "Device/client " . $dc . " cache/nocache " . $cached ,2);
# 
#         my $zulutime;
#         
#         for my $dp ( @{$ds->{datapoints}} ) {
#             
#             # my $ts = $dp->{timestamp};
# 
#             # chomp($ts); 
#             # $ts =~ s/T/ /;
#             # $ts =~ s/\.000Z//;
#             
# 
# 
#             $zulutime = $dp->{timestamp} ;
# 
# 
# 
#             chomp($zulutime); 
#             $zulutime =~ s/T/ /;
#             $zulutime =~ s/\.000Z//;     
#             $zulutime = $zulutime ;     
# 
# 
#             #$dt = ParseDate($zulutime );
#             my $err = $dt->parse($zulutime);
#             my $dttemp = $dt->value();
# 
# 
#             ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($dttemp, $timezone);
#             my $ts = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);
# 
#             # translate ts to resolution size
#             
#             if ($resolution eq 'H') {
#                 if ( ! defined ($timestampfix) ) {
#                     $timestampfix = substr $ts, 13, 18;
#                 }
#                 $ts = ( substr $ts, 0, 13 ) ;  
#                 $ts = $ts . $timestampfix;
#                 logger($self->{_debug}, "ts after applying resolution size $ts",2 ); 
#             }
#             
#             if ($resolution eq 'M') {
#                 if ( ! defined ($timestampfix) ) {
#                     $timestampfix = substr $ts, 16, 18;
#                 }
#                 $ts = ( substr $ts, 0, 16 ) ;
#                 $ts = $ts . $timestampfix;
#                 logger($self->{_debug}, "ts after applying resolution size $ts",2 );   
#             }            
#                     
#             my %row;
#             for my $ca ( @{$self->{_collectionAxes}} ) {
#                 if (ref($dp->{$ca}) eq 'HASH') {
#                     if ($ca eq "latency") {
#                         $row{$ca} = $dp->{latency};
#                     }  
#                     if ($ca eq "size") {
#                         $row{$ca} = $dp->{size};
#                     }        
#                 } else {
#                     if (defined $dp->{$ca} ) {
#                         $row{$ca} = $dp->{$ca};
#                     }
#                 }
#             }
# 
# 
#             if (defined ($op) ) {
#                 $resultset{$ts}->{$dc}->{$cache}->{$op} = \%row;
#             }
#             else {
#                 $resultset{$ts} = \%row;
#             }
# 
# 
#         } 
# 
#    }
#    
#    $self->{resultset} = \%resultset;
#  
#    return 0;  
# }
# 
# 
# sub doAggregation {
#     my $self = shift;
#     
#     logger($self->{_debug}, "Entering Analytic_io_obj::doAggregation",1);
# 
#     if ($self->{_name} =~ m/nfs-all/ ) {
#         $self->doAggregation_worker('throughput_r,throughput_w,throughput_t,latency_r,latency_w,cache_hit_ratio');
#     } else {  
#         $self->doAggregation_worker('throughput_r,throughput_w,throughput_t,latency_r,latency_w,iops_r,iops_w,iops');
#     }
# }
# 
# 
# # Procedure processData
# # parametres
# # - aggregation ( 10 - a daily aggregation for aggregated results ), 2 - aggregation of all values for 5 min stats
# # - obj - optional VDB info for network stats (not used now)
# # Process analytic data and prepare to print
# 
# sub processData {
#     my $self = shift;
#     my $aggregation = shift;
#     my $obj = shift;
# 
#     logger($self->{_debug}, "Entering Analytic_disk_obj::processData",1);
# 
#     undef $self->{aggreg}; 
#     
#     logger($self->{_debug}, "name " . $self->{_name},2);
# 
#     my $output = new Formater();
# 
#     undef $self->{size_hist};
#     my %size_hist;
#     my %read_hist_total;
#     my %write_hist_total;
#     
#     my $resultset = $self->{resultset};
#     
#     undef $self->{aggreg};
#     
#     my @timestamps = sort( keys %{ $resultset } );
#     my $rchr;
#     
#     if ($self->{_name} =~ m/nfs-by-client/ ) {
#         # NFS output
#         $output->addHeader(
#           {'timestamp', 20},
#           {'client',   20},
#           {'read_throughput',  20},
#           {'write_throughput', 20},
#           {'total_throughput', 20},
#           {'read_latency', 20},
#           {'write_latency', 20},
#           {'ops_read', 20},
#           {'ops_write', 20},
#           {'total_ops', 20}
#         );        
#     } elsif ($self->{_name} =~ m/nfs-all/ ) {    
#         # NFS output for cache
#         $rchr = 0;
#         $output->addHeader(
#           {'timestamp', 20},
#           {'client',   20},
#           {'read_throughput',  20},
#           {'write_throughput', 20},
#           {'total_throughput', 20},
#           {'read_latency', 10},
#           {'write_latency', 10},
#           {'read_cache_hit_ratio', 10},
#           {'ops_read', 20},
#           {'ops_write', 20},
#           {'total_ops', 20}
#         );                  
#     } elsif ($self->{_name} =~ m/default.nfs/ ) { 
#         $output->addHeader(
#           {'timestamp', 20},
#           {'read_throughput',  20},
#           {'write_throughput', 20},
#           {'total_throughput', 20},
#           {'read_latency', 20},
#           {'write_latency', 20},
#           {'ops_read', 20},
#           {'ops_write', 20},
#           {'total_ops', 20}
#         ); 
#     }else {
#         # Disk output
#         if (defined($timestamps[0])) {
#           if ($resultset->{$timestamps[0]}->{"none"}) {
#               $output->addHeader(
#                 {'timestamp', 20},
#                 {'read_throughput',  20},
#                 {'write_throughput', 20},
#                 {'total_throughput', 20},
#                 {'ops_read', 10},
#                 {'ops_write', 10},
#                 {'total_ops', 10},
#                 {'read_latency', 10},
#                 {'write_latency', 10}
#                ); 
#           } else {
#               $output->addHeader(
#                 {'timestamp', 20},
#                 {'client',   20},
#                 {'read_throughput',  20},
#                 {'write_throughput', 20},
#                 {'total_throughput', 20},
#                 {'ops_read', 10},
#                 {'ops_write', 10},
#                 {'total_ops', 10},
#                 {'read_latency', 10},
#                 {'write_latency', 10}
#                ); 
#           }
#         } else {
#           $output->addHeader(
#             {'timestamp', 20},
#             {'read_throughput',  20},
#             {'write_throughput', 20},
#             {'total_throughput', 20},
#             {'ops_read', 10},
#             {'ops_write', 10},
#             {'total_ops', 10},
#             {'read_latency', 10},
#             {'write_latency', 10}
#            );     
#         }
#     }    
# 
#     if ($self->{_overflow}) {
#       print "Please reduce a range. API is not able to provide all data.\n";
#       print "min date " . $timestamps[0] . " max date " . $timestamps[-1] . "\n";
#     } 
#     
#     for my $ts (@timestamps) {
#         
#         my @dc_keys = sort (keys %{ $resultset->{$ts} });
#         my %r_size_hist = ();
#         my %w_size_hist = ();
# 
#         
#         for my $dc_cur (@dc_keys) {
#                 #dc_cur - is a client IP if client is specified or none
#                 my $dc = $resultset->{$ts}->{$dc_cur};
#                 
#                 my $read_throughput = 0;
#                 my $write_throughput = 0;
#                 my $total_throughput = 0;
#                 
#                 my $read_iops; 
#                 my $write_iops;
#                 my $total_iops;
#                 
#                 my $w_latency;
#                 my $r_latency;
#                 
#                 my %w_latency_hist;
#                 my %r_latency_hist;
#                 my %total_latency_hist;
# 
#          
#                 if ( $dc->{none} ) {
#                     #no cache metrics
#                     $read_throughput  =  $dc->{none}->{read}->{throughput} ? $dc->{none}->{read}->{throughput} : 0;
#                     $write_throughput  = $dc->{none}->{write}->{throughput} ? $dc->{none}->{write}->{throughput} : 0;
#                     $total_throughput = $read_throughput + $write_throughput ;
#                      
#                     $read_iops = $dc->{none}->{read}->{count} ? $dc->{none}->{read}->{count} : 0; 
#                     $write_iops = $dc->{none}->{write}->{count} ? $dc->{none}->{write}->{count} : 0;
#                     $total_iops = $read_iops + $write_iops ;
#                     
#                     $self->add_histogram(\%r_latency_hist, $dc->{none}->{read}->{latency});
#                     $self->add_histogram(\%w_latency_hist, $dc->{none}->{write}->{latency});    
# 
#                     
#                     $self->add_histogram(\%total_latency_hist, \%r_latency_hist);
#                     $self->add_histogram(\%total_latency_hist, \%w_latency_hist);
# 
#                     $self->add_histogram(\%r_size_hist, $dc->{none}->{read}->{size});
#                     $self->add_histogram(\%w_size_hist, $dc->{none}->{write}->{size});
# 
#                 } 
#                 
#                 if ( $dc->{1} || $dc->{0} ) {
#                     # cached metrics
#                     
#                     my $read_cached_throughput  =  $dc->{1}->{read}->{throughput} ? $dc->{1}->{read}->{throughput} : 0;
#                     my $read_noncached_throughput  =  $dc->{0}->{read}->{throughput} ? $dc->{0}->{read}->{throughput} : 0;
#                     
#                     my $write_cached_throughput  =  $dc->{1}->{write}->{throughput} ? $dc->{1}->{write}->{throughput} : 0;
#                     my $write_noncached_throughput  =  $dc->{0}->{write}->{throughput} ? $dc->{0}->{write}->{throughput} : 0;
#                     
#                     
#                     $read_throughput  =  $read_throughput + $read_cached_throughput + $read_noncached_throughput;                    
#                     $write_throughput  = $write_throughput + $write_cached_throughput + $write_noncached_throughput;
#                     #$read_throughput  =  $read_cached_throughput + $read_noncached_throughput;                    
#                     #$write_throughput  = $write_cached_throughput + $write_noncached_throughput;
#                     $total_throughput = $read_throughput + $write_throughput ;
#                                 
#                     #print Dumper $dc;
#                                 
#                     $read_iops = $dc->{read}->{count} ? $dc->{read}->{count} : 0; 
#                     $write_iops = $dc->{write}->{count} ? $dc->{write}->{count} : 0;
#                     $total_iops = $read_iops + $write_iops ;
# 
#                     $self->add_histogram(\%r_latency_hist, $dc->{1}->{read}->{latency});                    
#                     $self->add_histogram(\%w_latency_hist, $dc->{1}->{write}->{latency});
# 
#                     $self->add_histogram(\%w_size_hist, $dc->{1}->{write}->{size});
#                     $self->add_histogram(\%r_size_hist, $dc->{1}->{read}->{size});
#              
#                     $self->add_histogram(\%r_latency_hist, $dc->{0}->{read}->{latency});               
#                     $self->add_histogram(\%w_latency_hist, $dc->{0}->{write}->{latency});
# 
# 
# 
#                     $self->add_histogram(\%w_size_hist, $dc->{0}->{write}->{size});
#                     $self->add_histogram(\%r_size_hist, $dc->{0}->{read}->{size});
# 
#                     my %total_latency_hist;
#                     $self->add_histogram(\%total_latency_hist, \%r_latency_hist);
#                     $self->add_histogram(\%total_latency_hist, \%w_latency_hist);
# 
#                     
#                     my $read_cached_count = $dc->{1}->{read}->{count} ? $dc->{1}->{read}->{count} : 0;
#                     my $read_noncached_count = $dc->{0}->{read}->{count} ? $dc->{0}->{read}->{count} : 0;
#                     
#                     if ( ( $read_cached_count +  $read_noncached_count ) > 0 ) {
#                         $rchr = sprintf("%.2f", $read_cached_count / ( $read_cached_count +  $read_noncached_count ) * 100 );  
#                     } else {
#                         $rchr = sprintf("%.2f",0);
#                     }           
#                 
#                 }
#                 
#                 $self->aggregation($ts, $aggregation, $dc_cur, 'cache_hit_ratio', $rchr);
#             
#                 # convert into MB/s
#                 my $read_tp_MBytes = sprintf("%.2f",($read_throughput/(1024*1024)));
#                 my $write_tp_MBytes = sprintf("%.2f",($write_throughput/(1024*1024)));
#                 my $total_tp_MBytes = sprintf("%.2f",($total_throughput/(1024*1024)));
#                 
#                 $self->aggregation($ts, $aggregation, $dc_cur, 'throughput_r', $read_tp_MBytes);
#                 $self->aggregation($ts, $aggregation, $dc_cur, 'throughput_w', $write_tp_MBytes);
#                 $self->aggregation($ts, $aggregation, $dc_cur, 'throughput_t', $total_tp_MBytes);
# 
#                 # aggregate iops
#                 $self->aggregation($ts, $aggregation, $dc_cur, 'iops_r', $read_iops);
#                 $self->aggregation($ts, $aggregation, $dc_cur, 'iops_w', $write_iops);
#                 $self->aggregation($ts, $aggregation, $dc_cur, 'iops', $total_iops);
# 
#                 $self->add_histogram(\%read_hist_total,\%r_latency_hist);
#                 $self->add_histogram(\%write_hist_total,\%w_latency_hist);
# 
#                 my $cal_r_latency = $self->calculate_latency(\%r_latency_hist);
#                 my $cal_w_latency = $self->calculate_latency(\%w_latency_hist);
#                 my $cal_t_latency = $self->calculate_latency(\%total_latency_hist);
# 
#                 # convert into millisec
#                 
#                 
#                 my $t_latency;
#                 
#                 if (defined($cal_r_latency)) {
#                   $r_latency  = sprintf("%.2f",$cal_r_latency  / 1000000);
#                   $self->aggregation($ts, $aggregation, $dc_cur, 'latency_r', $r_latency);
#                 } else {
#                   $r_latency = 'N/A';
#                 }
# 
#                 if (defined($cal_w_latency)) {
#                   $w_latency  = sprintf("%.2f", $cal_w_latency / 1000000);
#                   $self->aggregation($ts, $aggregation, $dc_cur, 'latency_w', $w_latency);
#                 } else {
#                   $w_latency = 'N/A';
#                 }
# 
#                 if (defined($cal_t_latency)) {
#                   $t_latency = sprintf("%.2f", $cal_t_latency / 1000000);
#                   $self->aggregation($ts, $aggregation, $dc_cur, 'latency_t', $t_latency);
#                 }
# 
# 
# 
#                 if ($self->{_name} =~ m/nfs/ ) {
#                     # NFS output
#                     if ($dc_cur eq "none") {
#                         if (defined $rchr) {
#                             $output->addLine(
#                                 $ts,$read_tp_MBytes,$write_tp_MBytes,$total_tp_MBytes,$r_latency,$w_latency,$rchr, $read_iops, $write_iops, $total_iops
#                             );
#                         } else {
#                             $output->addLine(
#                                 $ts,$read_tp_MBytes,$write_tp_MBytes,$total_tp_MBytes,$r_latency,$w_latency, $read_iops, $write_iops, $total_iops
#                             );
#                         }
#                     } else {
# 
#                         if (defined $rchr) {
#                             $output->addLine(
#                                 $ts,$dc_cur,$read_tp_MBytes,$write_tp_MBytes,$total_tp_MBytes,$r_latency,$w_latency,$rchr, $read_iops, $write_iops, $total_iops
#                             );
#                         } else {
#                             $output->addLine(
#                                 $ts,$dc_cur,$read_tp_MBytes,$write_tp_MBytes,$total_tp_MBytes,$r_latency,$w_latency, $read_iops, $write_iops, $total_iops
#                             );
#                         }                 
#                     }
#                 } else {
#                     if ($dc_cur eq "none") {
#                         $output->addLine(
#                                 $ts,$read_tp_MBytes,$write_tp_MBytes,$total_tp_MBytes,$read_iops,$write_iops,$total_iops,$r_latency,$w_latency
#                         );
#                     } else {
#                         $output->addLine(
#                             $ts,$dc_cur,$read_tp_MBytes,$write_tp_MBytes,$total_tp_MBytes,$read_iops,$write_iops,$total_iops,$r_latency,$w_latency
#                         );
#                     }
#                 }
# 
# 
#         }
# 
#         #$size_hist{$ts}{rsize} = $self->calculate_size(\%r_size_hist);
#         #$size_hist{$ts}{wsize} = $self->calculate_size(\%w_size_hist)
#         #calculate_size(\%w_size_hist);
#     }
# 
#     #$self->{size_hist} = \%size_hist;
#     $self->{_output} = $output;
#     
#     $self->{_read_hist_total} = \%read_hist_total;
#     $self->{_write_hist_total} = \%write_hist_total;
# }
# 
# 
# 
# # class Analytic_cpu_obj - is a child class of Analytic_obj
# 
# package Analytic_cpu_obj;
# use strict;
# use Data::Dumper;
# use Date::Manip;
# use List::Util qw (sum);
# use JSON;
# use Toolkit_helpers qw (logger);
# use Formater;
# our @ISA = qw(Analytic_obj);
# 
# # constructor
# # parameters 
# # - dlpxObject - connection to DE
# # - debug - debug flag (debug on if defined)
# 
# sub new {
#   my $class  = shift;
#   my $dlpx = shift;
#   my $name = shift;
#   my $reference = shift;
#   my $type = shift;
#   my $collectionAxes = shift;
#   my $collectionInterval = shift;
#   my $statisticType = shift;
#   my $debug = shift;
# 
#   logger($debug,"Entering Analytic_cpu_obj::constructor",1);
#   # call Analytic_obj constructor
#   my $self       = $class->SUPER::new($dlpx, $name, $reference, $type, $collectionAxes, $collectionInterval, $statisticType,  $debug); 
# }
# 
# # Procedure getData
# # parametres
# # dlpx - Delphix object with connection
# # additional_parms - additional parameters for webapi call (like time, resolution in URL, etc)
# # resolution - data resolution
# # Load analytic data from Delphix Engine into object
# 
# sub getData {
#    my $self = shift;
#    my $additional_parms = shift;
#    my $resolution = shift;
#    my $dlpx = $self->{_dlpx};
# 
#    logger($self->{_debug}, "Entering Analytic_cpu_obj::getData",1);
#    my $op = "resources/json/delphix/analytics/" . $self->{_reference} . "/getData?" . $additional_parms;
#    
# 
# 
#    my ($result, $result_fmt, $retcode) = $dlpx->getJSONResult($op);
# 
#    if ($retcode) {
#     return 1;
#    }
# 
#    $self->{_overflow} = $result->{result}->{overflow};
#    
#    # for every data stream
#    
#    my %resultset;
#    my $timestampfix;
#    
#    my $timezone = $self->{_detimezone};
#    my $tz = new Date::Manip::TZ;
#    my ($err,$date,$offset,$isdst,$abbrev);
#    my $dt = new Date::Manip::Date;
#    #$dt->config("tz","GMT");
#    $dt->config("setdate","zone,GMT");
#    
#    for my $ds ( @{$result->{result}{datapointStreams}} ) {
#     
#         for my $dp ( @{$ds->{datapoints}} ) {
#             
#             # my $ts = $dp->{timestamp};
#             # chomp($ts); 
#             # $ts =~ s/T/ /;
#             # $ts =~ s/\.000Z//;
# 
#             my $zulutime = $dp->{timestamp} ;
#             chomp($zulutime); 
#             $zulutime =~ s/T/ /;
#             $zulutime =~ s/\.000Z//;          
#             #$dt = ParseDate($zulutime);
#             my $err = $dt->parse($zulutime);
#             my $dttemp = $dt->value();
# 
#             ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($dttemp, $timezone);
#             my $ts = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);
# 
# 
#             
#             # translate ts to resolution size
#             
#             if ($resolution eq 'H') {
#                 if ( ! defined ($timestampfix) ) {
#                     $timestampfix = substr $ts, 13, 18;
#                 }
#                 $ts = ( substr $ts, 0, 13 ) ;  
#                 $ts = $ts . $timestampfix;
#                 logger($self->{_debug}, "ts after applying resolution size $ts",2 ); 
#             }
#             
#             if ($resolution eq 'M') {
#                 if ( ! defined ($timestampfix) ) {
#                     $timestampfix = substr $ts, 16, 18;
#                 }
#                 $ts = ( substr $ts, 0, 16 ) ;
#                 $ts = $ts . $timestampfix;
#                 logger($self->{_debug}, "ts after applying resolution size $ts",2 );   
#             }            
#                     
#             my %row;
#             for my $ca ( @{$self->{_collectionAxes}} ) {
#                 if (defined $dp->{$ca} ) {
#                         $row{$ca} = $dp->{$ca};
#                 }
#             }
# 
#             $resultset{$ts} = \%row;
#         } 
# 
#    }
#    
#    $self->{resultset} = \%resultset;
# 
#    return 0;
#    
# }
# 
# # Procedure processData_cpu
# # parametres
# # aggregation
# #
# # Process cpu data and add data into aggregation
# 
# sub processData {
#     my $self = shift;
#     my $aggregation = shift;
# 
#     undef $self->{aggreg}; 
#     logger($self->{_debug}, "Entering Analytics_cpu_obj::processData",1);
#     
#     my $resultset = $self->{resultset};
#     
#     my @timestamps = sort( keys %{ $resultset } );
# 
#     my $output = new Formater();
# 
#     $output->addHeader(
#       {'timestamp', 20},
#       {'util',      10}
#     );
#     
#     if ($self->{_overflow}) {
#       print "Please reduce a range. API is not able to provide all data.\n";
#       print "min date " . $timestamps[0] . " max date " . $timestamps[-1] . "\n";
#     } 
#     
#     for my $ts (@timestamps) {
#         
#         my $kernel  = $resultset->{$ts}->{kernel} ;
#         my $user  = $resultset->{$ts}->{user};
#         my $idle  = $resultset->{$ts}->{idle};
#         my $ttl_cpu =  ($idle+$user+$kernel);
#         my $util = ( $ttl_cpu == 0 ) ? 0 : ((($user+$kernel) / ($ttl_cpu)) * 100);
#         
#         $self->aggregation($ts, $aggregation, 'none', 'utilization', $util);
#         $output->addLine(
#             $ts, sprintf("%2.2f",$util)
#         );
# 
#     }   
# 
#     $self->{_output} = $output;
# 
# }
# 
# sub doAggregation {
#     my $self = shift;
#     
#     logger($self->{_debug}, "Entering Analytics_cpu_obj::doAggregation",1);
#     $self->doAggregation_worker('utilization');
# 
# }
# 
# # class Analytic_network_obj - is a child class of Analytic_obj
# 
# package Analytic_network_obj;
# use strict;
# use Data::Dumper;
# use Date::Manip;
# use List::Util qw (sum);
# use JSON;
# use Toolkit_helpers qw (logger);
# use Formater;
# our @ISA = qw(Analytic_obj);
# 
# # constructor
# # parameters 
# # - dlpxObject - connection to DE
# # - debug - debug flag (debug on if defined)
# 
# sub new {
#   my $class  = shift;
#   my $dlpx = shift;
#   my $name = shift;
#   my $reference = shift;
#   my $type = shift;
#   my $collectionAxes = shift;
#   my $collectionInterval = shift;
#   my $statisticType = shift;
#   my $debug = shift;
# 
#   logger($debug,"Entering Analytic_network_obj::constructor",1);
#   # call Analytic_obj constructor
#   my $self       = $class->SUPER::new($dlpx, $name, $reference, $type, $collectionAxes, $collectionInterval, $statisticType,  $debug); 
# }
# 
# # Procedure getData
# # parametres
# # dlpx - Delphix object with connection
# # additional_parms - additional parameters for webapi call (like time, resolution in URL, etc)
# # resolution - data resolution
# # Load analytic data from Delphix Engine into object
# 
# sub getData {
#    my $self = shift;
#    my $additional_parms = shift;
#    my $resolution = shift;
#    my $dlpx = $self->{_dlpx};
# 
#    logger($self->{_debug}, "Entering Analytic_network_obj::getData",1);
#    my $op = "resources/json/delphix/analytics/" . $self->{_reference} . "/getData?" . $additional_parms;
#    
#  
#    my ($result, $result_fmt, $retcode) = $dlpx->getJSONResult($op);
# 
#    if ($retcode) {
#     return 1;
#    }
# 
#    $self->{_overflow} = $result->{result}->{overflow};
#    
# 
#    # for every data stream
#    
#    my %resultset;
#    my $timestampfix;
#    
#    my $timezone = $self->{_detimezone};
#    my $tz = new Date::Manip::TZ;
#    my $dt = new Date::Manip::Date;
#    #$dt->config("tz","GMT");
#    $dt->config("setdate","zone,GMT");   
#    my ($err,$date,$offset,$isdst,$abbrev);
# 
#    
#    for my $ds ( @{$result->{result}{datapointStreams}} ) {
#     
#         # for data points in data stream
#         
#         my $nic;
# 
#         if (defined($ds->{networkInterface})) {
#             $nic = $ds->{networkInterface};
#         }
#         
#         
#         for my $dp ( @{$ds->{datapoints}} ) {
#             
#             # my $ts = $dp->{timestamp};
#             # chomp($ts); 
#             # $ts =~ s/T/ /;
#             # $ts =~ s/\.000Z//;
#             
# 
#             my $zulutime = $dp->{timestamp} ;
#             chomp($zulutime); 
#             $zulutime =~ s/T/ /;
#             $zulutime =~ s/\.000Z//;          
#             #$dt = ParseDate($zulutime);
#             my $err = $dt->parse($zulutime);
#             my $dttemp = $dt->value();
#             ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($dttemp, $timezone);
#             my $ts = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);
# 
# 
# 
#             # translate ts to resolution size
#             
#             if ($resolution eq 'H') {
#                 if ( ! defined ($timestampfix) ) {
#                     $timestampfix = substr $ts, 13, 18;
#                 }
#                 $ts = ( substr $ts, 0, 13 ) ;  
#                 $ts = $ts . $timestampfix;
#                 logger($self->{_debug}, "ts after applying resolution size $ts",2 ); 
#             }
#             
#             if ($resolution eq 'M') {
#                 if ( ! defined ($timestampfix) ) {
#                     $timestampfix = substr $ts, 16, 18;
#                 }
#                 $ts = ( substr $ts, 0, 16 ) ;
#                 $ts = $ts . $timestampfix;
#                 logger($self->{_debug}, "ts after applying resolution size $ts",2 );   
#             }            
#                     
#             my %row;
#             for my $ca ( @{$self->{_collectionAxes}} ) {
#                 if (defined $dp->{$ca} ) {
#                     $row{$ca} = $dp->{$ca};
#                 }
#             }
# 
# 
#             if (defined($nic) ) {
#                  $resultset{$ts}->{$nic} = \%row;
#             }
#             else {
#                 $resultset{$ts} = \%row;
#             }
# 
# 
#         } 
# 
#    }
#    
#    $self->{resultset} = \%resultset;
#    
#    return 0;
# }
# 
# 
# sub processData {
#     my $self = shift;
#     my $aggregation = shift;
#     my $io_obj = shift;
# 
#     logger($self->{_debug}, "Entering Analytic_network_obj::processData",1);
# 
#     undef $self->{aggreg}; 
#     
#     my $output = new Formater();
#     
#     my $resultset = $self->{resultset};
#     
#     my @timestamps = sort( keys %{ $resultset } );
#     
#     my $header;
# 
# 
#     if (defined($io_obj)) {
#         $output->addHeader(
#           {'timestamp', 20},
#           {'inBytes',   20},
#           {'outBytes',  20},
#           {'vdb_write', 20},
#           {'vdb_read', 20}
#         );
#     } else {
#       my @headerlist;
#       
#       push(@headerlist, {'timestamp', 20});
#       push(@headerlist, {'inBytes',   20});
#       push(@headerlist, {'outBytes',  20});
#       push(@headerlist, {'inPackets',   20});
#       push(@headerlist, {'outPackets',  20});
#       
#       for my $nic ( sort (keys %{$resultset->{$timestamps[0]}} )) {
#         push(@headerlist, {$nic . "_inBytes", 20});
#         push(@headerlist, {$nic . "_outBytes", 20});
#         push(@headerlist, {$nic . "_inPackets", 20});
#         push(@headerlist, {$nic . "_outPackets", 20});
#       }
# 
#       $output->addHeader(
#         @headerlist
#       );     
#     }
#     
#   
#     if ($self->{_overflow}) {
#       print "Please reduce a range. API is not able to provide all data.\n";
#       print "min date " . $timestamps[0] . " max date " . $timestamps[-1] . "\n";
#     } 
# 
#     for my $ts (@timestamps) {
# 
#         
#         my $inBytes  = 0;
#         my $outBytes  = 0;
#         my $inPackets = 0;
#         my $outPackets = 0;
#         
#         my @printarray;
#         
#         push(@printarray, $ts);
#         
#         
#         my @nicarray;
#         for my $nic ( sort (keys %{$resultset->{$ts}} )) {
#             $inBytes = $inBytes + $resultset->{$ts}->{$nic}->{inBytes} ;
#             $outBytes = $outBytes + $resultset->{$ts}->{$nic}->{outBytes};
#             $inPackets = $inPackets + $resultset->{$ts}->{$nic}->{inPackets};
#             $outPackets = $outPackets + $resultset->{$ts}->{$nic}->{outPackets};
#             push(@nicarray, sprintf("%d",$resultset->{$ts}->{$nic}->{inBytes}));
#             push(@nicarray, sprintf("%d",$resultset->{$ts}->{$nic}->{outBytes}));
#             push(@nicarray, sprintf("%d",$resultset->{$ts}->{$nic}->{inPackets}));
#             push(@nicarray, sprintf("%d",$resultset->{$ts}->{$nic}->{outPackets}));
#             
#         }
# 
#         push(@printarray, sprintf("%d",$inBytes));
#         push(@printarray, sprintf("%d",$outBytes));
#         push(@printarray, sprintf("%d",$inPackets));
#         push(@printarray, sprintf("%d",$outPackets));
#         
#         push(@printarray, @nicarray);
# 
# 
#         $self->aggregation($ts, $aggregation, 'none', 'inBytes', $inBytes);
#         $self->aggregation($ts, $aggregation, 'none', 'outBytes', $outBytes);        
#         
#         if (defined($io_obj)) {
#             my $vdb_write = $io_obj->{size_hist}->{$ts}->{wsize} ? sprintf("%d",$io_obj->{size_hist}->{$ts}->{wsize}) : 'N/A';
#             my $vdb_read = $io_obj->{size_hist}->{$ts}->{rsize} ? sprintf("%d",$io_obj->{size_hist}->{$ts}->{rsize}) : 'N/A';
#             $output->addLine(
#                 $ts , sprintf("%d",$inBytes) , sprintf("%d",$outBytes) , $vdb_write,  sprintf("%d",$vdb_read)
#             );
#         } else {
#             $output->addLine(
#                 #$ts , sprintf("%d",$inBytes) , sprintf("%d",$outBytes) 
#                 @printarray
#             );   
#         }    
#         
#     }  
# 
#     $self->{_output} = $output;
# 
# }
# 
# # Procedure doAggregation
# # parametres
# # generate aggregation
# 
# sub doAggregation {
#     my $self = shift;
#     
#     logger($self->{_debug}, "Entering Analytic_network_obj::doAggregation",1);    
#     $self->doAggregation_worker('inBytes,outBytes');
#       
# }
# 
# 
# 
# 
# 
# 
# 
# # class Analytic_tcp_obj - is a child class of Analytic_obj
# 
# package Analytic_tcp_obj;
# use strict;
# use Data::Dumper;
# use Date::Manip;
# use List::Util qw (sum);
# use JSON;
# use Toolkit_helpers qw (logger);
# use Formater;
# use Environment_obj;
# our @ISA = qw(Analytic_obj);
# 
# # constructor
# # parameters 
# # - dlpxObject - connection to DE
# # - debug - debug flag (debug on if defined)
# 
# sub new {
#   my $class  = shift;
#   my $dlpx = shift;
#   my $name = shift;
#   my $reference = shift;
#   my $type = shift;
#   my $collectionAxes = shift;
#   my $collectionInterval = shift;
#   my $statisticType = shift;
#   my $debug = shift;
# 
#   logger($debug,"Entering Analytic_tcp_obj::constructor",1);
#   # call Analytic_obj constructor
#   my $self       = $class->SUPER::new($dlpx, $name, $reference, $type, $collectionAxes, $collectionInterval, $statisticType,  $debug); 
# 
#    # my $self = {
#    #      _dlpx => $dlpx,
#    #      _name => $name,
#    #      _reference => $reference,
#    #      _type => $type,
#    #      _collectionAxes => $collectionAxes,
#    #      _collectionInterval => $collectionInterval,
#    #      _statisticType => $statisticType,
#    #      _debug => $debug
#    # };
# 
#    # bless $self, $class;
# 
#   my $env = new Environment_obj($dlpx, $debug);
#   $self->{_env} = $env;
# 
#   return $self;
# 
# }
# 
# # Procedure getData
# # parametres
# # dlpx - Delphix object with connection
# # additional_parms - additional parameters for webapi call (like time, resolution in URL, etc)
# # resolution - data resolution
# # Load analytic data from Delphix Engine into object
# 
# sub getData {
#    my $self = shift;
#    my $additional_parms = shift;
#    my $resolution = shift;
#    my $dlpx = $self->{_dlpx};
# 
#    logger($self->{_debug}, "Entering Analytic_tcp_obj::getData",1);
#    my $op = "resources/json/delphix/analytics/" . $self->{_reference} . "/getData?" . $additional_parms;
#    
#    
#    my ($result, $result_fmt, $retcode) = $dlpx->getJSONResult($op);
# 
#    if ($retcode) {
#     return 1;
#    }
# 
#    $self->{_overflow} = $result->{result}->{overflow};
#    
#    # for every data stream
#    
#    my %resultset;
#    my $timestampfix;
# 
#    my $timezone = $self->{_detimezone};
#    my $tz = new Date::Manip::TZ;
#    my $dt = new Date::Manip::Date;
#    #$dt->config("tz","GMT");
#    $dt->config("setdate","zone,GMT");
#    my ($err,$date,$offset,$isdst,$abbrev);
#    
# 
# 
#    my $jdbcports = $self->{_env}->getAllEnvironmentListenersPorts();
#    
#    for my $ds ( @{$result->{result}{datapointStreams}} ) {
#     
#         # for data points in data stream
#         
#         my $localPort;
#         my $remotePort;
#         my $remoteAddress;
# 
#         if (defined($ds->{remoteAddress})) {
#             $remoteAddress = $ds->{remoteAddress};
#         } 
#         
#         if (defined($ds->{remotePort})) {
#             $remotePort = $ds->{remotePort};
#         } 
# 
#         if (defined($ds->{localPort})) {
#             $localPort = $ds->{localPort};
#         } 
# 
#         my $type = $localPort . '-' . $remotePort;
# 
#         if (($localPort eq '2049') || ($localPort eq '111') || ($localPort eq '4045')) {
#           $type = 'NFS traffic';
#         } elsif ($remotePort eq '8415') {
#           $type = 'Replication';
#         } elsif (defined($jdbcports->{$remotePort} )) {
#           $type = 'JDBC';
#         } elsif (($localPort eq '8341') || ($localPort eq '8415') || ($localPort eq '873')) {
#           $type = 'Snapsync';
#         } elsif ($remotePort eq '22') {
#           $type = 'SSH traffic';
#         }        
#         
# 
#         if (($localPort ne '80') && ($localPort ne '22') && ($localPort ne '443') && ($localPort ne '5432'))  {
#           for my $dp ( @{$ds->{datapoints}} ) {
#               
#               # my $ts = $dp->{timestamp};
#               # chomp($ts); 
#               # $ts =~ s/T/ /;
#               # $ts =~ s/\.000Z//;
# 
#               my $zulutime = $dp->{timestamp} ;
#               chomp($zulutime); 
#               $zulutime =~ s/T/ /;
#               $zulutime =~ s/\.000Z//;          
#               #$dt = ParseDate($zulutime);
#               my $err = $dt->parse($zulutime);
#               my $dttemp = $dt->value();
#               ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($dttemp, $timezone);
#               my $ts = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);
# 
#               
#               # translate ts to resolution size
#               
#               if ($resolution eq 'H') {
#                   if ( ! defined ($timestampfix) ) {
#                       $timestampfix = substr $ts, 13, 18;
#                   }
#                   $ts = ( substr $ts, 0, 13 ) ;  
#                   $ts = $ts . $timestampfix;
#                   logger($self->{_debug}, "ts after applying resolution size $ts",2 ); 
#               }
#               
#               if ($resolution eq 'M') {
#                   if ( ! defined ($timestampfix) ) {
#                       $timestampfix = substr $ts, 16, 18;
#                   }
#                   $ts = ( substr $ts, 0, 16 ) ;
#                   $ts = $ts . $timestampfix;
#                   logger($self->{_debug}, "ts after applying resolution size $ts",2 );   
#               }            
#                       
#               my %row;
# 
#               for my $ca ( @{$self->{_collectionAxes}} ) {
#                   if (defined $dp->{$ca} ) {
#                       $row{$ca} = $dp->{$ca};
#                   }
#               }
# 
# 
# 
#               $resultset{$ts}->{$remoteAddress}->{$type} = \%row;
# 
# 
#           } 
#         }
# 
#    }
# 
#    
#    $self->{resultset} = \%resultset;
#  
#    return 0;  
# }
# 
# 
# sub processData {
#     my $self = shift;
#     my $aggregation = shift;
#     my $obj = shift;
# 
#     logger($self->{_debug}, "Entering Analytic_tcp_obj::processData",1);
# 
#     undef $self->{aggreg}; 
#     
#     logger($self->{_debug}, "name " . $self->{_name},2);
# 
#     my $output = new Formater();
# 
#     undef $self->{size_hist};
#     my %size_hist;
#     
#     my $resultset = $self->{resultset};
#     
#     undef $self->{aggreg};
#     
#     my @timestamps = sort( keys %{ $resultset } );
#     my $rchr;
#     
# 
# 
#     if ($self->{_dlpx}->getApi() lt "1.8") {
#       $output->addHeader(
#         {'timestamp', 20},
#         {'client',   20},
#         {'protocol',  20},
#         {'inBytes', 20},
#         {'outBytes', 20},
#         {'inUnorderedBytes', 20},
#         {'retransmittedBytes', 20},
#         {'unacknowledgedBytes', 20},
#         {'congestionWindowSize', 20}
#       );    
#     } else {
#       $output->addHeader(
#         {'timestamp', 20},
#         {'client',   20},
#         {'protocol',  20},
#         {'inBytes', 20},
#         {'outBytes', 20},
#         {'inUnorderedBytes', 20},
#         {'retransmittedBytes', 20},
#         {'unacknowledgedBytes', 20},
#         {'congestionWindowSize', 20},
#         {'roundTripTime', 20}
#       );  
#     }    
#   
#     if ($self->{_overflow}) {
#       print "Please reduce a range. API is not able to provide all data.\n";
#       print "min date " . $timestamps[0] . " max date " . $timestamps[-1] . "\n";
#     } 
#     
#     for my $ts (@timestamps) {
#         
#         my @client_keys = sort (keys %{ $resultset->{$ts} });
#         
#         for my $client_cur (@client_keys) {
#         
#                 my @types_keys = sort ( keys %{$resultset->{$ts}->{$client_cur} } );
#                 
# 
#                 for my $type_cur (@types_keys) {
# 
#                   my $cur_line = $resultset->{$ts}->{$client_cur}->{$type_cur};
# 
#                   my $in_bytes = $cur_line->{inBytes};
#                   my $out_bytes = $cur_line->{outBytes};
#                   my $inUnorderedBytes = $cur_line->{inUnorderedBytes};
#                   my $retransmittedBytes = $cur_line->{retransmittedBytes};
#                   my $unacknowledgedBytes = $cur_line->{unacknowledgedBytes};
#                   my $congestionWindowSize = $cur_line->{congestionWindowSize}; 
#                   my $rtt = $cur_line->{roundTripTime}; 
# 
#                   $self->aggregation($ts, $aggregation,  $client_cur .'-' . $type_cur, 'inBytes', $in_bytes);
#                   $self->aggregation($ts, $aggregation,  $client_cur .'-' . $type_cur, 'outBytes', $out_bytes);
# 
#                   if ($self->{_dlpx}->getApi() lt "1.8") {
#                     $output->addLine(
#                         $ts,$client_cur, $type_cur, $in_bytes, $out_bytes, $inUnorderedBytes, $retransmittedBytes, $unacknowledgedBytes, $congestionWindowSize
#                     );
#                   } else {
#                     $output->addLine(
#                         $ts,$client_cur, $type_cur, $in_bytes, $out_bytes, $inUnorderedBytes, $retransmittedBytes, $unacknowledgedBytes, $congestionWindowSize, $rtt
#                     );                
#                   }
# 
# 
#                 }
# 
#         }
# 
# 
#     }
# 
# 
#     $self->{_output} = $output;
# }
# 
# 
# # Procedure doAggregation
# # parametres
# # generate aggregation
# 
# sub doAggregation {
#     my $self = shift;
#     
#     logger($self->{_debug}, "Entering Analytic_tcp_obj::doAggregation",1);    
#     $self->doAggregation_worker('inBytes,outBytes');
#       
# }

# End of package
1;