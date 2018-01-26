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


package Analytic_io_obj;
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

  logger($debug,"Entering Analytic_io_obj::constructor",1);
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

   logger($self->{_debug}, "Entering Analytic_io_obj::getData",1);
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
   my ($err,$date,$offset,$isdst,$abbrev);

   #$dt->config("tz","GMT");
   $dt->config("setdate","zone,GMT");

   for my $ds ( @{$result->{result}{datapointStreams}} ) {
    
        # for data points in data stream
        
        # device / client switch
        my $dc = "none";
        my $cache = "none";
        
        my $op = $ds->{op};
        
        $self->{op} = $op;
        
        my $client = defined ($ds->{client} ) ? $dc = $ds->{client} : "none"; 
        my $device = defined ($ds->{device} ) ? $dc = $ds->{device} : "none";
        my $cached = defined ($ds->{cached} ) ? $cache = $ds->{cached} : "none";

        
        logger($self->{_debug}, "Device/client " . $dc . " cache/nocache " . $cached ,2);

        my $zulutime;
        
        for my $dp ( @{$ds->{datapoints}} ) {
            
            $zulutime = $dp->{timestamp} ;
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
                if (ref($dp->{$ca}) eq 'HASH') {
                    if ($ca eq "latency") {
                        $row{$ca} = $dp->{latency};
                    }  
                    if ($ca eq "size") {
                        $row{$ca} = $dp->{size};
                    }        
                } else {
                    if (defined $dp->{$ca} ) {
                        $row{$ca} = $dp->{$ca};
                    }
                }
            }


            if (defined ($op) ) {
                $resultset{$ts}->{$dc}->{$cache}->{$op} = \%row;
            }
            else {
                $resultset{$ts} = \%row;
            }


        } 

   }
   
   $self->{resultset} = \%resultset;
 
   return 0;  
}


sub doAggregation {
    my $self = shift;
    
    logger($self->{_debug}, "Entering Analytic_io_obj::doAggregation",1);

    if ($self->{_name} =~ m/nfs-all/ ) {
        $self->doAggregation_worker('throughput_r,throughput_w,throughput_t,latency_r,latency_w,cache_hit_ratio');
    } else {  
        $self->doAggregation_worker('throughput_r,throughput_w,throughput_t,latency_r,latency_w,iops_r,iops_w,iops');
    }
}


# Procedure processData
# parametres
# - aggregation ( 10 - a daily aggregation for aggregated results ), 2 - aggregation of all values for 5 min stats
# - obj - optional VDB info for network stats (not used now)
# Process analytic data and prepare to print

sub processData {
    my $self = shift;
    my $aggregation = shift;
    my $obj = shift;

    logger($self->{_debug}, "Entering Analytic_disk_obj::processData",1);

    undef $self->{aggreg}; 
    
    logger($self->{_debug}, "name " . $self->{_name},2);

    my $output = new Formater();

    undef $self->{size_hist};
    my %size_hist;
    my %read_hist_total;
    my %write_hist_total;
    
    my $resultset = $self->{resultset};
    
    undef $self->{aggreg};
    
    my @timestamps = sort( keys %{ $resultset } );
    my $rchr;
    
    if ($self->{_name} =~ m/nfs-by-client/ ) {
        # NFS output
        $output->addHeader(
          {'timestamp', 20},
          {'client',   20},
          {'read_throughput',  20},
          {'write_throughput', 20},
          {'total_throughput', 20},
          {'read_latency', 20},
          {'write_latency', 20},
          {'ops_read', 20},
          {'ops_write', 20},
          {'total_ops', 20}
        );        
    } elsif ($self->{_name} =~ m/nfs-all/ ) {    
        # NFS output for cache
        $rchr = 0;
        $output->addHeader(
          {'timestamp', 20},
          {'client',   20},
          {'read_throughput',  20},
          {'write_throughput', 20},
          {'total_throughput', 20},
          {'read_latency', 10},
          {'write_latency', 10},
          {'read_cache_hit_ratio', 10},
          {'ops_read', 20},
          {'ops_write', 20},
          {'total_ops', 20}
        );                  
    } elsif ($self->{_name} =~ m/default.nfs/ ) { 
        $output->addHeader(
          {'timestamp', 20},
          {'read_throughput',  20},
          {'write_throughput', 20},
          {'total_throughput', 20},
          {'read_latency', 20},
          {'write_latency', 20},
          {'ops_read', 20},
          {'ops_write', 20},
          {'total_ops', 20}
        ); 
    }else {
        # Disk output
        if (defined($timestamps[0])) {
          if ($resultset->{$timestamps[0]}->{"none"}) {
              $output->addHeader(
                {'timestamp', 20},
                {'read_throughput',  20},
                {'write_throughput', 20},
                {'total_throughput', 20},
                {'ops_read', 10},
                {'ops_write', 10},
                {'total_ops', 10},
                {'read_latency', 10},
                {'write_latency', 10}
               ); 
          } else {
              $output->addHeader(
                {'timestamp', 20},
                {'client',   20},
                {'read_throughput',  20},
                {'write_throughput', 20},
                {'total_throughput', 20},
                {'ops_read', 10},
                {'ops_write', 10},
                {'total_ops', 10},
                {'read_latency', 10},
                {'write_latency', 10}
               ); 
          }
        } else {
          $output->addHeader(
            {'timestamp', 20},
            {'read_throughput',  20},
            {'write_throughput', 20},
            {'total_throughput', 20},
            {'ops_read', 10},
            {'ops_write', 10},
            {'total_ops', 10},
            {'read_latency', 10},
            {'write_latency', 10}
           );     
        }
    }    

    if ($self->{_overflow}) {
      print "Please reduce a range. API is not able to provide all data.\n";
      print "min date " . $timestamps[0] . " max date " . $timestamps[-1] . "\n";
    } 
    
    for my $ts (@timestamps) {
        
        my @dc_keys = sort (keys %{ $resultset->{$ts} });
        my %r_size_hist = ();
        my %w_size_hist = ();

        
        for my $dc_cur (@dc_keys) {
                #dc_cur - is a client IP if client is specified or none
                my $dc = $resultset->{$ts}->{$dc_cur};
                
                my $read_throughput = 0;
                my $write_throughput = 0;
                my $total_throughput = 0;
                
                my $read_iops; 
                my $write_iops;
                my $total_iops;
                
                my $w_latency;
                my $r_latency;
                
                my %w_latency_hist;
                my %r_latency_hist;
                my %total_latency_hist;

         
                if ( $dc->{none} ) {
                    #no cache metrics
                    $read_throughput  =  $dc->{none}->{read}->{throughput} ? $dc->{none}->{read}->{throughput} : 0;
                    $write_throughput  = $dc->{none}->{write}->{throughput} ? $dc->{none}->{write}->{throughput} : 0;
                    $total_throughput = $read_throughput + $write_throughput ;
                     
                    $read_iops = $dc->{none}->{read}->{count} ? $dc->{none}->{read}->{count} : 0; 
                    $write_iops = $dc->{none}->{write}->{count} ? $dc->{none}->{write}->{count} : 0;
                    $total_iops = $read_iops + $write_iops ;
                    
                    $self->add_histogram(\%r_latency_hist, $dc->{none}->{read}->{latency});
                    $self->add_histogram(\%w_latency_hist, $dc->{none}->{write}->{latency});    

                    
                    $self->add_histogram(\%total_latency_hist, \%r_latency_hist);
                    $self->add_histogram(\%total_latency_hist, \%w_latency_hist);

                    $self->add_histogram(\%r_size_hist, $dc->{none}->{read}->{size});
                    $self->add_histogram(\%w_size_hist, $dc->{none}->{write}->{size});

                } 
                
                if ( $dc->{1} || $dc->{0} ) {
                    # cached metrics
                    
                    my $read_cached_throughput  =  $dc->{1}->{read}->{throughput} ? $dc->{1}->{read}->{throughput} : 0;
                    my $read_noncached_throughput  =  $dc->{0}->{read}->{throughput} ? $dc->{0}->{read}->{throughput} : 0;
                    
                    my $write_cached_throughput  =  $dc->{1}->{write}->{throughput} ? $dc->{1}->{write}->{throughput} : 0;
                    my $write_noncached_throughput  =  $dc->{0}->{write}->{throughput} ? $dc->{0}->{write}->{throughput} : 0;
                    
                    
                    $read_throughput  =  $read_throughput + $read_cached_throughput + $read_noncached_throughput;                    
                    $write_throughput  = $write_throughput + $write_cached_throughput + $write_noncached_throughput;
                    #$read_throughput  =  $read_cached_throughput + $read_noncached_throughput;                    
                    #$write_throughput  = $write_cached_throughput + $write_noncached_throughput;
                    $total_throughput = $read_throughput + $write_throughput ;
                                
                    #print Dumper $dc;
                                
                    $self->add_histogram(\%r_latency_hist, $dc->{1}->{read}->{latency});                    
                    $self->add_histogram(\%w_latency_hist, $dc->{1}->{write}->{latency});

                    $self->add_histogram(\%w_size_hist, $dc->{1}->{write}->{size});
                    $self->add_histogram(\%r_size_hist, $dc->{1}->{read}->{size});
             
                    $self->add_histogram(\%r_latency_hist, $dc->{0}->{read}->{latency});               
                    $self->add_histogram(\%w_latency_hist, $dc->{0}->{write}->{latency});



                    $self->add_histogram(\%w_size_hist, $dc->{0}->{write}->{size});
                    $self->add_histogram(\%r_size_hist, $dc->{0}->{read}->{size});

                    my %total_latency_hist;
                    $self->add_histogram(\%total_latency_hist, \%r_latency_hist);
                    $self->add_histogram(\%total_latency_hist, \%w_latency_hist);

                    
                    my $read_cached_count = $dc->{1}->{read}->{count} ? $dc->{1}->{read}->{count} : 0;
                    my $read_noncached_count = $dc->{0}->{read}->{count} ? $dc->{0}->{read}->{count} : 0;
                    
                    my $write_iops_cache_nocache = ($dc->{1}->{write}->{count} || $dc->{0}->{write}->{count}) ? $dc->{1}->{write}->{count} + $dc->{0}->{write}->{count} : 0;
                    my $total_iops_cache_nocache = $write_iops_cache_nocache + $read_cached_count + $read_noncached_count;

                    $read_iops = $read_cached_count + $read_noncached_count;
                    $write_iops = $write_iops_cache_nocache;
                    $total_iops = $read_iops + $write_iops ;
                    
                    if ( ( $read_cached_count +  $read_noncached_count ) > 0 ) {
                        $rchr = sprintf("%.2f", $read_cached_count / ( $read_cached_count +  $read_noncached_count ) * 100 );  
                    } else {
                        $rchr = sprintf("%.2f",0);
                    }           
                
                }
                
                $self->aggregation($ts, $aggregation, $dc_cur, 'cache_hit_ratio', $rchr);
            
                # convert into MB/s
                my $read_tp_MBytes = sprintf("%.2f",($read_throughput/(1024*1024)));
                my $write_tp_MBytes = sprintf("%.2f",($write_throughput/(1024*1024)));
                my $total_tp_MBytes = sprintf("%.2f",($total_throughput/(1024*1024)));
                
                $self->aggregation($ts, $aggregation, $dc_cur, 'throughput_r', $read_tp_MBytes);
                $self->aggregation($ts, $aggregation, $dc_cur, 'throughput_w', $write_tp_MBytes);
                $self->aggregation($ts, $aggregation, $dc_cur, 'throughput_t', $total_tp_MBytes);

                # aggregate iops
                $self->aggregation($ts, $aggregation, $dc_cur, 'iops_r', $read_iops);
                $self->aggregation($ts, $aggregation, $dc_cur, 'iops_w', $write_iops);
                $self->aggregation($ts, $aggregation, $dc_cur, 'iops', $total_iops);

                $self->add_histogram(\%read_hist_total,\%r_latency_hist);
                $self->add_histogram(\%write_hist_total,\%w_latency_hist);

                my $cal_r_latency = $self->calculate_latency(\%r_latency_hist);
                my $cal_w_latency = $self->calculate_latency(\%w_latency_hist);
                my $cal_t_latency = $self->calculate_latency(\%total_latency_hist);

                # convert into millisec
                
                
                my $t_latency;
                
                if (defined($cal_r_latency)) {
                  $r_latency  = sprintf("%.2f",$cal_r_latency  / 1000000);
                  $self->aggregation($ts, $aggregation, $dc_cur, 'latency_r', $r_latency);
                } else {
                  $r_latency = 'N/A';
                }

                if (defined($cal_w_latency)) {
                  $w_latency  = sprintf("%.2f", $cal_w_latency / 1000000);
                  $self->aggregation($ts, $aggregation, $dc_cur, 'latency_w', $w_latency);
                } else {
                  $w_latency = 'N/A';
                }

                if (defined($cal_t_latency)) {
                  $t_latency = sprintf("%.2f", $cal_t_latency / 1000000);
                  $self->aggregation($ts, $aggregation, $dc_cur, 'latency_t', $t_latency);
                }



                if ($self->{_name} =~ m/nfs/ ) {
                    # NFS output
                    if ($dc_cur eq "none") {
                        if (defined $rchr) {
                            $output->addLine(
                                $ts,$read_tp_MBytes,$write_tp_MBytes,$total_tp_MBytes,$r_latency,$w_latency,$rchr, $read_iops, $write_iops, $total_iops
                            );
                        } else {
                            $output->addLine(
                                $ts,$read_tp_MBytes,$write_tp_MBytes,$total_tp_MBytes,$r_latency,$w_latency, $read_iops, $write_iops, $total_iops
                            );
                        }
                    } else {

                        if (defined $rchr) {
                            $output->addLine(
                                $ts,$dc_cur,$read_tp_MBytes,$write_tp_MBytes,$total_tp_MBytes,$r_latency,$w_latency,$rchr, $read_iops, $write_iops, $total_iops
                            );
                        } else {
                            $output->addLine(
                                $ts,$dc_cur,$read_tp_MBytes,$write_tp_MBytes,$total_tp_MBytes,$r_latency,$w_latency, $read_iops, $write_iops, $total_iops
                            );
                        }                 
                    }
                } else {
                    if ($dc_cur eq "none") {
                        $output->addLine(
                                $ts,$read_tp_MBytes,$write_tp_MBytes,$total_tp_MBytes,$read_iops,$write_iops,$total_iops,$r_latency,$w_latency
                        );
                    } else {
                        $output->addLine(
                            $ts,$dc_cur,$read_tp_MBytes,$write_tp_MBytes,$total_tp_MBytes,$read_iops,$write_iops,$total_iops,$r_latency,$w_latency
                        );
                    }
                }


        }

    }

    #$self->{size_hist} = \%size_hist;
    $self->{_output} = $output;
    
    $self->{_read_hist_total} = \%read_hist_total;
    $self->{_write_hist_total} = \%write_hist_total;
}




# End of package
1;