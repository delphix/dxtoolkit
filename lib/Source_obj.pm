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
# Program Name : Source_obj.pm
# Description  : Delphix Engine Source object
# It's include the following classes:
# - Source_obj - class which map a Delphix Engine source API object
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#



package Source_obj;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);

# constructor
# parameters
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $debug = shift;
    logger($debug, "Entering Source_obj::constructor",1);

    my %sources;
    my $self = {
        _sources => \%sources,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };

    bless($self,$classname);

    $self->getSourceList($debug);
    return $self;
}

# Procedure getSource
# parameters:
# - reference
# Return source hash for specific source reference

sub getSource {
    my $self = shift;
    my $container = shift;

    logger($self->{_debug}, "Entering Source_obj::getSource",1);

    my $sources = $self->{_sources};
    return $sources->{$container};
}

# Procedure getName
# parameters:
# - reference
# Return source name for specific source reference

sub getName {
    my $self = shift;
    my $container = shift;

    logger($self->{_debug}, "Entering Source_obj::getName",1);

    my $sources = $self->{_sources};
    return $sources->{$container}->{name};
}

# Procedure getSourceByName
# parameters:
# - name
# Return source hash for specific source name

sub getSourceByName {
    my $self = shift;
    my $name = shift;
    my $ret;

    logger($self->{_debug}, "Entering Source_obj::getSourceByName",1);

    for my $sourceitem ( sort ( keys %{$self->{_sources}} ) ) {

        if ( $self->getName($sourceitem) eq $name) {
            $ret = $self->getSource($sourceitem);
        }
    }

    return $ret;
}


# Procedure getSourceByConfig
# parameters:
# - config ref
# Return source ref for specific sourceconfig ref

sub getSourceByConfig {
    my $self = shift;
    my $config = shift;
    my $ret;

    logger($self->{_debug}, "Entering Source_obj::getSourceByConfig",1);
    for my $sourceitem ( sort ( keys %{$self->{_sources}} ) ) {
        if ( defined($self->getSourceConfig($sourceitem)) && ($self->getSourceConfig($sourceitem) eq $config)) {
            $ret = $self->getSource($sourceitem);
        }
    }

    return $ret;
}


# Procedure getSourceConfig
# parameters:
# - container
# Return source config reference for specific cointainer in source

sub getSourceConfig {
    my $self = shift;
    my $container = shift;

    logger($self->{_debug}, "Entering Source_obj::getSourceConfig",1);

    my $sources = $self->{_sources};
    my $ret;
    if (defined($sources->{$container})) {
      if (version->parse($self->{_dlpxObject}->getApi()) <= version->parse(1.11.10)) {
        # engine until 6.0.10
        logger($self->{_debug}, "getSourceConfig - API <= 1.11.10",2);
        $ret = $sources->{$container}->{'config'};
      } elsif (version->parse($self->{_dlpxObject}->getApi()) <= version->parse(1.11.11)) {
        # API changed and config is moved to syncStrategy but only for MSSqlLinkedSource
        if ($sources->{$container}->{'type'} eq 'MSSqlLinkedSource') {
          logger($self->{_debug}, "getSourceConfig - API >= 1.11.11 - MSSQL",2);
          $ret = $sources->{$container}->{'syncStrategy'}->{'config'};
        } else {
          logger($self->{_debug}, "getSourceConfig - API >= 1.11.11 - others",2);
          $ret = $sources->{$container}->{'config'};
        }
      } else {
        # API changed and config is moved to syncStrategy but only for MSSqlLinkedSource
        if ( ($sources->{$container}->{'type'} eq 'MSSqlLinkedSource') || ($sources->{$container}->{'type'} eq 'OracleLinkedSource')) {
          logger($self->{_debug}, "getSourceConfig - API >= 1.11.12 - MSSQL or Oracle",2);
          $ret = $sources->{$container}->{'syncStrategy'}->{'config'};
        } else {
          logger($self->{_debug}, "getSourceConfig - API >= 1.11.12 - others",2);
          $ret = $sources->{$container}->{'config'};
        }
      }
    } else {
      $ret = 'NA';
    }

    return $ret;
}

# Procedure getStaging
# parameters:
# - container
# Return staging source reference for specific cointainer in source

sub getStaging {
    my $self = shift;
    my $container = shift;

    logger($self->{_debug}, "Entering Source_obj::getStaging",1);
    my $sources = $self->{_sources};
    return $sources->{$container}->{stagingSource};
}


# Procedure getSourceList
# parameters: - none
# Load list of sources from Delphix Engine
# using source container as hash key for non-staging sources
# using source reference as hash key for staging sources

sub getSourceList
{
    my $self = shift;

    logger($self->{_debug}, "Entering Source_obj::getSourceList",1);

    my $operation = "resources/json/delphix/source";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};

        my %localsources;

        for my $source (@res) {
            $localsources{$source->{reference}} = $source;
        }

        my $sources = $self->{_sources};

        for my $source ( keys %localsources ) {
            if ( $localsources{$source}{type} =~ /StagingSource/ ) {
                $sources->{$localsources{$source}{reference}} = $localsources{$source};
                logger($self->{_debug}, "Staging: $localsources{$source}{reference} $localsources{$source}{name} $localsources{$source}{type}",2);
            } elsif ( $localsources{$source}{type} =~ /OracleLiveSource/ ) {
                # add Live Source here
                $sources->{$localsources{$source}{reference}} = $localsources{$source};
                logger($self->{_debug}, "Staging: $localsources{$source}{reference} $localsources{$source}{name} $localsources{$source}{type}",2);
            } else {
                $sources->{$localsources{$source}{container}} = $localsources{$source};
            }
        }
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }

}

# refreshSource
# -reference - source ref
# read source again from Delphix Engine

sub refreshSource {
  my $self = shift;
  my $reference = shift;

  logger($self->{_debug}, "Entering Source_obj::getSourceList",1);

  my $operation = "resources/json/delphix/source/" . $reference;
  my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
  my $ret;

  if (defined($result->{status}) && ($result->{status} eq 'OK')) {
      $ret = $result->{result};


  } else {
      print "No data returned for $operation. Try to increase timeout \n";
  }

  return $ret;
}

1;
