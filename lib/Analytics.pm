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
# Description  : Delphix Engine Analytics 
# Author       : Marcin Przepiorowski
# Created      : 27 May 2015 (v1.0.0)
#


package Analytics;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);

use lib '../lib';
use Analytic_obj;
use Analytic_io_obj;
use Analytic_cpu_obj;
use Analytic_tcp_obj;
use Analytic_network_obj;

# constructor
# parameters 
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $debug = shift;
    logger($debug, "Entering Analytics::constructor",1);

    my %analytics;
    my $self = {
        _analytics => \%analytics,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);
    

    $self->loadAnalyticsList($debug);
    return $self;
}

# Procedure getAnalytics
# parameters: 
# - reference
# Return Analictys_obj object for specific analytics reference

sub getAnalytics {
    my $self = shift;
    my $container = shift;
    
    logger($self->{_debug}, "Entering Analytics::getAnalytics",1);   

    my $analytics = $self->{_analytics};
    return $analytics->{$container};
}

# Procedure getAnalyticsList
# Return list of analytic objects loaded

sub getAnalyticsList {
    my $self = shift;
    my $container = shift;
    
    logger($self->{_debug}, "Entering Analytics::getAnalytics",1);   
    my @ret = sort (keys %{$self->{_analytics}});
    return \@ret;
}




# Procedure getName
# parameters: 
# - reference
# Return analytic name for specific analytic reference 

sub getName {
    my $self = shift;
    my $container = shift;

    logger($self->{_debug}, "Entering Analytics::getName",1);   

    my $analytics = $self->{_analytics};
    my $ret = $analytics->{$container}->getName();
    return $ret;
}

# Procedure getAnalyticByName
# parameters: 
# - name
# Return Analictys_obj object for specific analytics name

sub getAnalyticByName {
    my $self = shift;
    my $name = shift;
    my $ret;
    
    logger($self->{_debug}, "Entering Analytics::getAnalyticByName",1);   

    for my $analyticitem ( sort ( keys %{$self->{_analytics}} ) ) {

        if ( $self->getName($analyticitem) eq $name) {
            $ret = $self->getAnalytics($analyticitem); 
        }
    }

    return $ret;
}

# Procedure get_perf
# parameters: 
# - name - analytic name / all
# - outdir - location of output filenames
# - arguments - URL arguments
# - resolution - requested resolution of data
# - format - output format
# Generate a analytic data


sub get_perf {
    my $self = shift;
    my $name = shift;
    my $outdir = shift;
    my $arguments = shift;
    my $resolution = shift;
    my $format = shift;

    logger($self->{_debug}, "Entering Analytics::get_perf",1);   

    my @analytic_list;

    if (lc $name eq 'all') {
        for my $ref (keys %{$self->{_analytics}}) {
            push(@analytic_list, $self->getName($ref));
        }
    } elsif (lc $name eq 'standard') {
        push(@analytic_list, 'cpu');
        push(@analytic_list, 'network');
        push(@analytic_list, 'disk');
        push(@analytic_list, 'nfs');
    } else {

        my @a = split (',', $name);

        for my $n (@a) {
            if (defined($self->getAnalyticByName($n))) {
                push(@analytic_list, $n);
            } else {
                print "Analytic name $n not found. It will be not included in output \n";
            }
        }
    }

    my $suffix = '';
    if (defined($format) && (lc $format eq 'json') ) {
        $suffix = '.json';
    } else {
        $suffix = '.csv';
    }

    for my $n (sort @analytic_list) {
        my $analytic = $self->getAnalyticByName($n);

        my $fn = "$outdir/" . $self->{_dlpxObject}->getEngineName() . "-analytics-" . $n . "-raw" . $suffix;

        print "Gathering data for " . $analytic->getName() . "\n";
        my $ret = $analytic->getData($arguments, $resolution);
        if ($ret) {
            if ($ret eq 1) {
              print "Timeout during gathering data for " . $analytic->getName() . "\n";
              return 1;
            } elsif ($ret eq 2) {
              print "No data returned for analytics " . $analytic->getName() . ". Consider restarting collector\n";
              return 2;              
            } else {
              print "Unknown error gathering a data for " . $analytic->getName() . "\n";
              return 3;
            } 
        } else {
            print "Generating " . $analytic->getName() . " raw report file $fn\n";

            open (my $FD, "> $fn") || die "Can't open file: $fn!\n";
            
            $analytic->processData(10);
            $analytic->print($FD, $format);

            close($FD);
          
            $fn = "$outdir/" . $self->{_dlpxObject}->getEngineName() . "-analytics-" . $analytic->getName() . "-aggregated" . $suffix;
            print "Generating " . $analytic->getName() . " aggregated report file $fn\n";
            open ($FD, "> $fn") || die "Can't open file: $fn!\n";
            $analytic->doAggregation();
            $analytic->print_aggregation($FD, $format);
            close($FD);   
        }

    }

}


# Procedure display_analytics
# parameters: 
# Print list of analytics for engine

sub display_analytics {
    my $self = shift;
    logger($self->{_debug}, "Entering Analytics::display_analytics",1);   
    my $analytics = $self->{_analytics};
    $analytics->{(keys %{$analytics})[0]}->printDetails_banner();
    for my $i (sort (keys %{$analytics})) {
        $analytics->{$i}->printDetails();
    }
}

# Procedure create_analytics
# parameters: 
# - name - analytic name
# Print list of analytics for engine


sub create_analytic {
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering Analytics::create_analytics",1);   
    my $analytics = $self->{_analytics};

    my %newanalytic;    
    my @axes = ("latency",
                "throughput",
                "count",
                "op",
                "client");
    
    $newanalytic{"nfs-by-client"} = {
                "type" => "StatisticSlice",
                "name" => "nfs-by-client",
                "collectionAxes" => \@axes,
                "collectionInterval" => 1,
                "statisticType" => "NFS_OPS"
    };

    my @axes1 = ("latency",
                "throughput",
                "count",
                "op",
                "client",
                "cached",
                "size");
    
    $newanalytic{"nfs-all"} = {
                "type" => "StatisticSlice",
                "name" => "nfs-all",
                "collectionAxes" => \@axes1,
                "collectionInterval" => 1,
                "statisticType" => "NFS_OPS"
    };

    
    $newanalytic{"iscsi-by-client"} = {
                "type" => "StatisticSlice",
                "name" => "iscsi-by-client",
                "collectionAxes" => \@axes,
                "collectionInterval" => 1,
                "statisticType" => "iSCSI_OPS"
    };
    
    
    if (defined ($newanalytic{$name}) ) {
    # new analytic definition

        if (defined( $self->getAnalyticByName($name) )) {
            print "Analytic $name already exists.\n";
            return 1;          
        } else {


            my $operation = "resources/json/delphix/analytics";        
            my $json_data = encode_json($newanalytic{$name});
            logger($self->{_debug}, $json_data,2);
            my($result, $result_fmt) =$self->{_dlpxObject}->postJSONData($operation,$json_data); 
            my $status = $result->{status};
            if ( $status ne "OK"  ) {
                  print "Error: $result->{error}{details}\n";
            }
            else {
                  print "New analytic $name has been created\n";
                  return 0;
            }
        } 
        
    } else {
        print "Invalid analytic name - $name\n";
        return 1;          
    }
        
}




# Procedure getSourceList
# parametres
#
# List analytics from Delphix Engine and 
# create a instrance of Ananlytics_obj and load into _analytics hash

sub loadAnalyticsList {
    my $self = shift;
    
    my $analytics = $self->{_analytics};
    logger($self->{_debug}, "Entering Analytic::getAnalyticsList",1);
    
    my $operation = "resources/json/delphix/analytics";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    my @res;

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        @res = @{$result->{result}};
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
        exit 1;
    }

    for my $stat (@res) {
        my $ao;

        if ($stat->{name}  =~ m/cpu/ ) {
            $ao = new Analytic_cpu_obj (     $self->{_dlpxObject},
                                $stat->{name}, 
                                $stat->{reference}, 
                                $stat->{type}, 
                                $stat->{collectionAxes}, 
                                $stat->{collectionInterval}, 
                                $stat->{statisticType},
                                $self->{_debug}
                            );
        }

        if ($stat->{name}  =~ m/network/ ) {
            $ao = new Analytic_network_obj (     $self->{_dlpxObject},
                                $stat->{name}, 
                                $stat->{reference}, 
                                $stat->{type}, 
                                $stat->{collectionAxes}, 
                                $stat->{collectionInterval}, 
                                $stat->{statisticType},
                                $self->{_debug}
                            );
        }

        if ( ($stat->{name}  =~ m/disk/) || ($stat->{name}  =~ m/iscsi/) || ($stat->{name}  =~ m/nfs/)  ) {
            $ao = new Analytic_io_obj (     $self->{_dlpxObject},
                                $stat->{name}, 
                                $stat->{reference}, 
                                $stat->{type}, 
                                $stat->{collectionAxes}, 
                                $stat->{collectionInterval}, 
                                $stat->{statisticType},
                                $self->{_debug}
                            );
        }

        if ( ($stat->{name}  =~ m/default.tcp/) ) {
            $ao = new Analytic_tcp_obj (     $self->{_dlpxObject},
                                $stat->{name}, 
                                $stat->{reference}, 
                                $stat->{type}, 
                                $stat->{collectionAxes}, 
                                $stat->{collectionInterval}, 
                                $stat->{statisticType},
                                $self->{_debug}
                            );
        }

        if (defined($ao)) {
            $analytics->{$stat->{reference}} = $ao;
        } 

    } 
}




1;