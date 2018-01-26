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

# End of package
1;