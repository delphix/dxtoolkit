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
# Program Name : JS_branch_obj.pm
# Description  : Delphix Engine JS branch
# Author       : Marcin Przepiorowski
# Created      : Apr 2016 (v2.2.4)
#


package JS_branch_obj;

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
    my $template = shift;
    my $debug = shift;
    logger($debug, "Entering JS_branch_obj::constructor",1);

    my %jsbranches;
    my $self = {
        _jsbranches => \%jsbranches,
        _dlpxObject => $dlpxObject,
        _template => $template,
        _debug => $debug
    };

    bless($self,$classname);

    $self->loadJSBranchList($template, $debug);
    return $self;
}


# Procedure getJSBranchByName
# parameters:
# - name
# - array
# Return branch reference for particular name or array of branches

sub getJSBranchByName {
    my $self = shift;
    my $name = shift;
    my $array = shift;
    logger($self->{_debug}, "Entering JS_branch_obj::getJSBranchByName",1);
    my $ret;
    my @brancharray = grep { $self->getName($_) eq $name } ( sort ( keys %{$self->{_jsbranches}} ) );

    if (defined($array)) {
      $ret = \@brancharray;
    } else {
      if (scalar(@brancharray) == 1) {
        $ret = $brancharray[0];
      }
      elsif (scalar(@brancharray) < 1) {
        print "Can't find branch with name $name on engine " . $self->{_dlpxObject}->getEngineName() . "\n";
      } elsif (scalar(@brancharray) > 1) {
        print "Branch name $name on engine" . $self->{_dlpxObject}->getEngineName() . " is not unique.\n";
      }
    }
    return $ret;

}

# Procedure getBranch
# parameters:
# - reference
# Return branch hash for specific branch reference

sub getBranch {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_branch_obj::getBranch",1);

    my $jsbranches = $self->{_jsbranches};
    return $jsbranches->{$reference};
}


# Procedure getJSTemplateList
# parameters:
# Return JS template list

sub getJSBranchList {
    my $self = shift;

    logger($self->{_debug}, "Entering JS_branch_obj::getJSBranchList",1);

    my @arrret = sort (keys %{$self->{_jsbranches}} );

    return \@arrret;
}


# Procedure getName
# parameters:
# - reference
# Return JS branch name for specific branch reference

sub getName {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_branch_obj::getName",1);

    my $jsbranches = $self->{_jsbranches};
    return $jsbranches->{$reference}->{name};
}

# Procedure getDataobj
# parameters:
# - reference
# Return JS dataobj

sub getDataobj {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_branch_obj::getDataobj",1);

    my $jsbranches = $self->{_jsbranches};
    return $jsbranches->{$reference}->{dataLayout};
}


# Procedure loadJSBranchList
# parameters:
#  dataobj_ref - data object for to load branches
# Load a list of branch objects from Delphix Engine

sub loadJSBranchList
{
    my $self = shift;
    my $dataobj_ref = shift;
    logger($self->{_debug}, "Entering JS_branch_obj::loadJSBranchList",1);

    my $operation = "resources/json/delphix/jetstream/branch?";

    if (defined($dataobj_ref)) {
        $operation = $operation . "dataLayout=" . $dataobj_ref ;
    }


    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $jsbranches = $self->{_jsbranches};

        for my $branchitem (@res) {
            $jsbranches->{$branchitem->{reference}} = $branchitem;
        }
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}


# Procedure createBranch
# parameters:
# - container list class
# - branch_name
# - container_ref
# - timestamp - bookmark name / timestamp
# - from branch
# create a cranch
# return job reference

sub createBranch {
    my $self = shift;
    my $jscontainers = shift;
    my $branch_name = shift;
    my $container_ref = shift;
    my $timestamp = shift;
    my $frombranch = shift;

    logger($self->{_debug}, "Entering JS_branch_obj::createBranch",1);

    my %timehash;

    if (defined($timestamp)) {
      if ( (my ($year,$mon,$day,$hh,$mi,$ss) = $timestamp =~ /(\d\d\d\d)-(\d\d)-(\d\d) (\d?\d):(\d?\d):(\d\d)/ ) ) {
        # timestamp is real one
        my $active_branch;
        if (defined($frombranch)) {
          $active_branch = $self->getJSBranchByName($frombranch);
        } else {
          $active_branch = $jscontainers->getJSActiveBranch($container_ref);
        }
        if (defined($active_branch)) {
          my $gmttime = Toolkit_helpers::convert_to_utc($timestamp, $self->{_dlpxObject}->getTimezone(), undef, 1);
          %timehash = (
            "type" => "JSTimelinePointTimeInput",
            "time" => $gmttime,
            "branch" => $active_branch
          );
        } else {
          print "Source branch not found\n";
          logger($self->{_debug}, "Source branch not found",2);
          return undef;
        }
      } else {
        # is timestamp a bookmark ?
        my $bookmarks = new JS_bookmark_obj ( $self->{_dlpxObject}, undef, $container_ref, $self->{_debug} );
        my $book_ref = $bookmarks->getJSBookmarkByName($timestamp);
        if (defined($book_ref)) {
          %timehash = (
            "type" => "JSTimelinePointBookmarkInput",
            "bookmark" => $book_ref
          );
        } else {
          print "Timestamp format doesn't match YYYY-MM-DD HH24:MI:SS or bookmark name\n";
          logger($self->{_debug}, "Timestamp format doesn't match YYYY-MM-DD HH24:MI:SS or bookmark name",2);
          return undef;
        }
      }
    } else {
      # there is no timestamp nor bookmark - creating branch from latest point
      %timehash = (
        "type" => "JSTimelinePointLatestTimeInput",
        "sourceDataLayout" => $container_ref
      );
    }

    my $operation = "resources/json/delphix/jetstream/branch";
    my %branchhash = (
        "type" => "JSBranchCreateParameters",
        "name" => $branch_name,
        "dataContainer" => $container_ref,
        "timelinePointParameters" => \%timehash
    );

    my $json_data = to_json(\%branchhash);
    return $self->runJobOperation($operation, $json_data);

}

# Procedure deleteBranch
# parameters:
# - branch_name
# delete a branch
# return job reference

sub deleteBranch {
    my $self = shift;
    my $branch_ref = shift;

    logger($self->{_debug}, "Entering JS_branch_obj::deleteBranch",1);
    my $operation = "resources/json/delphix/jetstream/branch/" . $branch_ref . "/delete";
    return $self->runJobOperation($operation, '{}');

}


# Procedure activateBranch
# parameters:
# - branch_name
# activate a branch
# return job reference

sub activateBranch {
    my $self = shift;
    my $branch_ref = shift;

    logger($self->{_debug}, "Entering JS_branch_obj::activateBranch",1);
    my $operation = "resources/json/delphix/jetstream/branch/" . $branch_ref . "/activate";
    return $self->runJobOperation($operation, '{}');

}

# Procedure runJobOperation
# parameters:
# - operation - API string
# - json_data - JSON encoded data
# Run POST command running background job for particular operation and json data
# Return job number if job started or undef otherwise

sub runJobOperation {
    my $self = shift;
    my $operation = shift;
    my $json_data = shift;
    my $action = shift;

    logger($self->{_debug}, "Entering JS_branch_obj::runJobOperation",1);
    logger($self->{_debug}, $operation, 2);

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);
    my $jobno;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        if (defined($action) && $action eq 'ACTION') {
            $jobno = $result->{action};
        } else {
            $jobno = $result->{job};
        }
    } else {
        if (defined($result->{error})) {
            print "Problem with job " . $result->{error}->{details} . "\n";
            logger($self->{_debug}, "Can't submit job for operation $operation",1);
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
    }

    return $jobno;
}

1;
