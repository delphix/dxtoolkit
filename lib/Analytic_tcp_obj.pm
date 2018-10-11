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


# class Analytic_tcp_obj - is a child class of Analytic_obj

package Analytic_tcp_obj;
use strict;
use Data::Dumper;
use Date::Manip;
use List::Util qw (sum);
use JSON;
use Toolkit_helpers qw (logger);
use Formater;
use Environment_obj;
use version;
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

  logger($debug,"Entering Analytic_tcp_obj::constructor",1);
  # call Analytic_obj constructor
  my $self       = $class->SUPER::new($dlpx, $name, $reference, $type, $collectionAxes, $collectionInterval, $statisticType,  $debug);

   # my $self = {
   #      _dlpx => $dlpx,
   #      _name => $name,
   #      _reference => $reference,
   #      _type => $type,
   #      _collectionAxes => $collectionAxes,
   #      _collectionInterval => $collectionInterval,
   #      _statisticType => $statisticType,
   #      _debug => $debug
   # };

   # bless $self, $class;

  my $env = new Environment_obj($dlpx, $debug);
  $self->{_env} = $env;

  return $self;

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

   logger($self->{_debug}, "Entering Analytic_tcp_obj::getData",1);
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



   my $jdbcports = $self->{_env}->getAllEnvironmentListenersPorts();

   for my $ds ( @{$result->{result}{datapointStreams}} ) {

        # for data points in data stream

        my $localPort;
        my $remotePort;
        my $remoteAddress;

        if (defined($ds->{remoteAddress})) {
            $remoteAddress = $ds->{remoteAddress};
        }

        if (defined($ds->{remotePort})) {
            $remotePort = $ds->{remotePort};
        }

        if (defined($ds->{localPort})) {
            $localPort = $ds->{localPort};
        }

        my $type = $localPort . '-' . $remotePort;

        if (($localPort eq '2049') || ($localPort eq '111') || ($localPort eq '4045')) {
          $type = 'NFS traffic';
        } elsif ($remotePort eq '8415') {
          $type = 'Replication';
        } elsif (defined($jdbcports->{$remotePort} )) {
          $type = 'JDBC';
        } elsif (($localPort eq '8341') || ($localPort eq '8415') || ($localPort eq '873')) {
          $type = 'Snapsync';
        } elsif ($remotePort eq '22') {
          $type = 'SSH traffic';
        }


        if (($localPort ne '80') && ($localPort ne '22') && ($localPort ne '443') && ($localPort ne '5432'))  {
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



              $resultset{$ts}->{$remoteAddress}->{$type} = \%row;


          }
        }

   }


   $self->{resultset} = \%resultset;

   return 0;
}


sub processData {
    my $self = shift;
    my $aggregation = shift;
    my $obj = shift;

    logger($self->{_debug}, "Entering Analytic_tcp_obj::processData",1);

    undef $self->{aggreg};

    logger($self->{_debug}, "name " . $self->{_name},2);

    my $output = new Formater();

    undef $self->{size_hist};
    my %size_hist;

    my $resultset = $self->{resultset};

    undef $self->{aggreg};

    my @timestamps = sort( keys %{ $resultset } );
    my $rchr;



    if (version->parse($self->{_dlpx}->getApi()) < version->parse(1.8.0)) {
      $output->addHeader(
        {'timestamp', 20},
        {'client',   20},
        {'protocol',  20},
        {'inBytes', 20},
        {'outBytes', 20},
        {'inUnorderedBytes', 20},
        {'retransmittedBytes', 20},
        {'unacknowledgedBytes', 20},
        {'congestionWindowSize', 20}
      );
    } else {
      $output->addHeader(
        {'timestamp', 20},
        {'client',   20},
        {'protocol',  20},
        {'inBytes', 20},
        {'outBytes', 20},
        {'inUnorderedBytes', 20},
        {'retransmittedBytes', 20},
        {'unacknowledgedBytes', 20},
        {'congestionWindowSize', 20},
        {'roundTripTime', 20}
      );
    }

    if ($self->{_overflow}) {
      print "Please reduce a range. API is not able to provide all data.\n";
      print "min date " . $timestamps[0] . " max date " . $timestamps[-1] . "\n";
    }

    for my $ts (@timestamps) {

        my @client_keys = sort (keys %{ $resultset->{$ts} });

        for my $client_cur (@client_keys) {

                my @types_keys = sort ( keys %{$resultset->{$ts}->{$client_cur} } );


                for my $type_cur (@types_keys) {

                  my $cur_line = $resultset->{$ts}->{$client_cur}->{$type_cur};

                  my $in_bytes = $cur_line->{inBytes};
                  my $out_bytes = $cur_line->{outBytes};
                  my $inUnorderedBytes = $cur_line->{inUnorderedBytes};
                  my $retransmittedBytes = $cur_line->{retransmittedBytes};
                  my $unacknowledgedBytes = $cur_line->{unacknowledgedBytes};
                  my $congestionWindowSize = $cur_line->{congestionWindowSize};
                  my $rtt = $cur_line->{roundTripTime};

                  $self->aggregation($ts, $aggregation,  $client_cur .'-' . $type_cur, 'inBytes', $in_bytes);
                  $self->aggregation($ts, $aggregation,  $client_cur .'-' . $type_cur, 'outBytes', $out_bytes);

                  if (version->parse($self->{_dlpx}->getApi()) < version->parse(1.8.0)) {
                    $output->addLine(
                        $ts,$client_cur, $type_cur, $in_bytes, $out_bytes, $inUnorderedBytes, $retransmittedBytes, $unacknowledgedBytes, $congestionWindowSize
                    );
                  } else {
                    $output->addLine(
                        $ts,$client_cur, $type_cur, $in_bytes, $out_bytes, $inUnorderedBytes, $retransmittedBytes, $unacknowledgedBytes, $congestionWindowSize, $rtt
                    );
                  }


                }

        }


    }


    $self->{_output} = $output;
}


# Procedure doAggregation
# parametres
# generate aggregation

sub doAggregation {
    my $self = shift;

    logger($self->{_debug}, "Entering Analytic_tcp_obj::doAggregation",1);
    $self->doAggregation_worker('inBytes,outBytes');

}

# End of package
1;
