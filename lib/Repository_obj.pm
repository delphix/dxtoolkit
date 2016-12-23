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
# Program Name : Repository_obj.pm
# Description  : Delphix Engine Capacity object
# It's include the following classes:
# - Repository_obj - class which map a Delphix Engine repository API object
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#
#


package Repository_obj;

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
    logger($debug, "Entering Repository_obj::constructor",1);

    my %repositories;
    my $self = {
        _repositories => \%repositories,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);
    
    $self->listRepositoryList($debug);
    return $self;
}

# Procedure getRepository
# parameters: 
# - reference
# Return repository hash for specific repository reference

sub getRepository {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Repository_obj::getRepository",1);    

    my $repositories = $self->{_repositories};
    my $ret;
    if ( defined($reference) && defined($repositories->{$reference}) ) {
        $ret = $repositories->{$reference};
    } else {
        $ret = 'NA';
    }
    return $ret;
}

# Procedure getEnvironment
# parameters: 
# - reference
# Return environment reference for specific repository reference

sub getEnvironment {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Repository_obj::getEnvironment",1);  

    my $repositories = $self->{_repositories};
    my $ret;
    if ( defined($reference) && defined($repositories->{$reference})) {
        $ret = $repositories->{$reference}->{'environment'};
    } else {
        $ret = 'NA';
    }
    return $ret;
}

# Procedure getName
# parameters: 
# - reference
# Return repository name for specific repository reference

sub getName {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Repository_obj::getName",1);  

    my $repositories = $self->{_repositories};
    return $repositories->{$reference}->{'name'};
}


# Procedure getRepositoryByNameForEnv
# parameters: 
# - name - repository name
# - env - environment reference
# Return repository reference for particular name and environemnt

sub getRepositoryByNameForEnv {
    my $self = shift;
    my $name = shift;
    my $env = shift;
    my $ret;
    
    logger($self->{_debug}, "Entering Repository_obj::getRepositoryByNameForEnv",1);  

    for my $repitem ( sort ( keys %{$self->{_repositories}} ) ) {

        if ( ( $self->getName($repitem) eq $name)  && ( $self->getEnvironment($repitem) eq $env ) ) {
            $ret = $self->getRepository($repitem); 
        }
    }

    return $ret;
}


# Procedure getRepositoryByNameForEnv
# parameters: 
# - env - environment reference
# Return list repository reference for particular env

sub getRepositoryByEnv {
    my $self = shift;
    my $env = shift;
    my @ret;
    
    logger($self->{_debug}, "Entering Repository_obj::getRepositoryByEnv",1);  

    for my $repitem ( sort ( keys %{$self->{_repositories}} ) ) {

        if ( $self->getEnvironment($repitem) eq $env )  {
            push (@ret, $repitem);
        }
    }

    my @sortret = sort { $self->getName($a) cmp $self->getName($b) } (@ret);

    return \@ret;
}


# Procedure listRepositoryList
# parameters: none
# Load a list of repository objects from Delphix Engine

sub listRepositoryList 
{
    my $self = shift;
    my $debug = shift;

    logger($self->{_debug}, "Entering Repository_obj::listRepositoryList",1);  
    my $operation = "resources/json/delphix/repository";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $repositories = $self->{_repositories};
        for my $repitem (@res) {
            $repositories->{$repitem->{reference}} = $repitem;
        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}


# Procedure createRepository
# parameters:
# - reference - env reference
# - repotype
# - repopath
# Create repository
# Return 0 if OK 

sub createRepository
{
    my $self = shift;
    my $reference = shift;
    my $repotype = shift;
    my $repopath = shift;

    logger($self->{_debug}, "Entering Environment_obj::createRepository",1);

    my $type;
    
    if (!defined($repotype)) {
      print "Repository type has to be set\n";
      return 1;
    }

    if (!defined($repopath)) {
      print "Repository path or instance has to be set\n";
      return 1;
    }

    if (lc $repotype eq 'oracle') {
      $type = 'OracleInstall';
    } else {
      print "Only Oracle is supported\n";
      return 1;
    }


    my $operation = "resources/json/delphix/repository";
    my %repo_data = (
      "type" => $type,
      "environment" => $reference,
      "installationHome" => $repopath
    );

    my $json_data = to_json(\%repo_data, {pretty=>1});
    logger($self->{_debug}, $json_data, 2);

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);
    my $ret;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
      print "Repository $repopath created \n";
      $ret = 0;
    } else {
        if (defined($result->{error})) {
            print "Problem with repository creation " . $result->{error}->{details} . "\n";
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
    }
}

# Procedure createRepository
# parameters:
# - reference - env reference
# - repopath
# Create repository
# Return 0 if OK 

sub deleteRepository
{
    my $self = shift;
    my $reference = shift;
    my $repopath = shift;

    logger($self->{_debug}, "Entering Environment_obj::createRepository",1);

    my $repo = $self->getRepositoryByNameForEnv($repopath, $reference);
    
    
    if (!defined($repo->{reference})) {
      print "Can't find repository $repopath \n";
      return 1;
    }
    

    my $operation = "resources/json/delphix/repository/" . $repo->{reference} . "/delete";

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, '{}');
    my $ret;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
      print "Repository $repopath deleted \n";
      $ret = 0;
    } else {
        if (defined($result->{error})) {
            print "Problem with repository deletion " . $result->{error}->{details} . "\n";
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
    }
}


1;