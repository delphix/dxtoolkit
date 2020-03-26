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
# Program Name : JS_container_obj.pm
# Description  : Delphix Engine JS template
# Author       : Marcin Przepiorowski
# Created      : Apr 2016 (v2.2.4)
#
#


package JS_container_obj;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
use JS_bookmark_obj;
use JS_datasource_obj;
use Snapshot_obj;
use version;
use Date::Manip;

# constructor
# parameters
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $template_ref = shift;
    my $debug = shift;
    logger($debug, "Entering JS_container_obj::constructor",1);

    my %jscontainer;
    my $self = {
        _jscontainer => \%jscontainer,
        _dlpxObject => $dlpxObject,
        _template_ref => $template_ref,
        _debug => $debug
    };

    bless($self,$classname);

    $self->loadJSContainerList($debug);
    return $self;
}


# Procedure getJSContainerByName
# parameters:
# - name
# - array
# Return container reference for particular name
# or array of containers if array flag is set

sub getJSContainerByName {
    my $self = shift;
    my $name = shift;
    my $array = shift;
    logger($self->{_debug}, "Entering JS_container_obj::getJSContainerByName",1);
    my @contarray = grep { $self->getName($_) eq $name } ( sort ( keys %{$self->{_jscontainer}} ) );
    my $ret;

    if (defined($array)) {
      $ret = \@contarray;
    } else {
      if (scalar(@contarray) == 1) {
        $ret = $contarray[0];
      }
      elsif (scalar(@contarray) < 1) {
        print "Can't find container with name $name on engine " . $self->{_dlpxObject}->getEngineName() . "\n";
      } elsif (scalar(@contarray) > 1) {
        print "Container name $name on engine" . $self->{_dlpxObject}->getEngineName() . " is not unique. Please add template name\n";
      }
    }
    return $ret;
}

# Procedure getJSContainer
# parameters:
# - reference
# Return container hash for specific container reference

sub getJSContainer {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_container_obj::getJSContainer",1);

    my $container = $self->{_jscontainer};
    return $container->{$reference};
}


# Procedure getJSActiveBranch
# parameters:
# - reference
# Return active branch for container for specific container reference

sub getJSActiveBranch {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_container_obj::getJSActiveBranch",1);

    my $container = $self->{_jscontainer};
    return $container->{$reference}->{activeBranch};
}



# Procedure getJSContainerTemplate
# parameters:
# - reference
# Return active branch for container for specific container reference

sub getJSContainerTemplate {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_container_obj::getJSContainerTemplate",1);

    my $container = $self->{_jscontainer};
    return $container->{$reference}->{template};
}


# Procedure getJSContainerList
# parameters:
# Return JS container list

sub getJSContainerList {
    my $self = shift;

    logger($self->{_debug}, "Entering JS_container_obj::getJSTemplateList",1);

    my @arrret = sort (keys %{$self->{_jscontainer}} );

    return \@arrret;
}

# Procedure getJSFirstOperation
# parameters:
# - reference
# Return firstoperation for container for specific container reference

sub getJSFirstOperation {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_container_obj::getJSFirstOperation",1);

    my $jscontainer = $self->{_jscontainer};
    return $jscontainer->{$reference}->{firstOperation};
}


# Procedure getName
# parameters:
# - reference
# Return JS container name for specific container reference

sub getName {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_container_obj::getName",1);

    my $jscontainer = $self->{_jscontainer};
    return $jscontainer->{$reference} ? $jscontainer->{$reference}->{name} : 'N/A';
}


# Procedure getProperties
# parameters:
# - reference
# Return JS container properties hash for specific container reference

sub getProperties {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_container_obj::getProperties",1);

    my $jscontainer = $self->{_jscontainer};
    return $jscontainer->{$reference}->{properties};
}



# Procedure getGenerateDatabaseList
# parameters:
# - engine object
# - container ref
# - databases object
# - group object
# Return array of array with database name / group pairs

sub getGenerateDatabaseList {
    my $self = shift;
    my $engine_obj = shift;
    my $reference = shift;
    my $databases = shift;
    my $groups = shift;

    logger($self->{_debug}, "Entering JS_container_obj::getGenerateDatabaseList",1);

    my $jsdatasources = new JS_datasource_obj ( $engine_obj , $reference, undef);
    my @listarray;
    for my $ds (@{$jsdatasources->getJSDataSourceList()}) {
        my @dbarray;
        push(@dbarray, $groups->getName($databases->getDB($jsdatasources->getJSDBContainer($ds))->getGroup()));
        push(@dbarray, $databases->getDB($jsdatasources->getJSDBContainer($ds))->getName());
        push(@listarray, \@dbarray);
    }

    return \@listarray;
}


# Procedure getDatabaseList
# parameters:
# - engine object
# - container ref
# - databases object
# - group object
# Return semicolumn sepratrated list of databases groups and names for container

sub getDatabaseList {
    my $self = shift;
    my $engine_obj = shift;
    my $reference = shift;
    my $databases = shift;
    my $groups = shift;


    my $listarray = $self->getGenerateDatabaseList($engine_obj, $reference, $databases, $groups);
    my $ret = join('; ' , map { join('/', @{$_}) } @{$listarray});
    return $ret;

}


# Procedure getbackup
# parameters:
# - engine object
# - container ref
# - databases object
# - group object
# Return backup of container

sub getbackup {
    my $self = shift;
    my $engine_obj = shift;
    my $reference = shift;
    my $databases = shift;
    my $groups = shift;
    my $templates = shift;
    my $owners = shift;
    my $output = shift;

#   dx_ctl_js_container -d Landshark51 -action create -container_def "Analytics,testdx" -container_def "Analytics,autotest" -container_name cont2 -template_name template2 -container_owner js

    my $ret = "dx_ctl_js_container -d " . $engine_obj->getEngineName() . " -action create";

    $ret = $ret . " -container_name \"" . $self->getName($reference) . "\"";
    $ret = $ret . " -template_name \"" .$templates->getName($self->getJSContainerTemplate($reference)) . "\"";

    for my $user (@{$owners}) {
      $ret = $ret . " -container_owner \"" . $user . "\"";
    }

    my $listarray = $self->getGenerateDatabaseList($engine_obj, $reference, $databases, $groups);

    for my $contdef (map { join(',', @{$_}) } @{$listarray}) {
      $ret = $ret . " -container_def \"" . $contdef . "\" ";
    }

    $output->addLine( $ret );

}

# Procedure loadJSContainerList
# parameters: none
# Load a list of container objects from Delphix Engine

sub loadJSContainerList
{
    my $self = shift;
    logger($self->{_debug}, "Entering JS_container_obj::loadJSContainerList",1);

    my $operation = "resources/json/delphix/jetstream/container";

    if (defined($self->{_template_ref})) {
        $operation = $operation . "?template=" . $self->{_template_ref} ;
    }


    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $jscontainer = $self->{_jscontainer};

        for my $containeritem (@res) {
            $jscontainer->{$containeritem->{reference}} = $containeritem;
        }
    }
}


# Procedure restoreContainer
# parameters:
# - reference
# - branchref
# - timestamp
# - $dataobj_ref (if restore in 5.0 or lower)
# - full branch name is timestamp is bookmark name
# recover container
# return job reference

sub restoreContainer {
    my $self = shift;
    my $reference = shift;
    my $branchref = shift;
    my $timestamp = shift;
    my $dataobj_ref = shift;
    my $full_branchname = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::restoreContainer",1);

    my $detz = $self->{_dlpxObject}->getTimezone();

    my %timelineHash;

    my $zulutime;

    if ( $timestamp =~ /(\d\d\d\d)-(\d\d)-(\d\d) (\d?\d):(\d?\d):(\d\d)/ ) {


        chomp($timestamp);
        $timestamp =~ s/T/ /;
        $timestamp =~ s/\.000Z//;

        my $ret;
        $zulutime = Toolkit_helpers::convert_to_utc($timestamp, $detz, undef, 1);

        if (defined($zulutime)) {

            if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.8.0)) {
              %timelineHash = (
                  "type" => "JSTimelinePointTimeInput",
                  "sourceDataLayout" => $dataobj_ref,
                  "time" => $zulutime
              );
            } else {
              %timelineHash = (
                  "type" => "JSTimelinePointTimeInput",
                  "branch" => $branchref,
                  "time" => $zulutime
              );
            }
        } else {
            print "Can't parse timestamp - $timestamp \n";
            return undef;
        }
    } else {
        # maybe it's bookmark name
        my $bookmarks = new JS_bookmark_obj ( $self->{_dlpxObject}, undef, undef, $self->{_debug} );
        my $bookmark_ref = $bookmarks->getJSBookmarkByName($timestamp, $full_branchname);

        if (defined($bookmark_ref)) {
            %timelineHash = (
                "type" => "JSTimelinePointBookmarkInput",
                "bookmark" => $bookmark_ref
            );
        } else {
            return undef;
        }

    }

    my %recoveryHash;

    if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.9.0)) {
      %recoveryHash = %timelineHash;
    } else {
      %recoveryHash =  (
        "type" => "JSDataContainerRestoreParameters",
        "forceOption" => JSON::false,
        "timelinePointParameters" => \%timelineHash
      );
    }

    my $json_data = to_json(\%recoveryHash);

    my $operation = "resources/json/delphix/jetstream/container/" . $reference . "/restore";

    return $self->runJobOperation($operation, $json_data);

}


# Procedure refreshContainer
# parameters:
# - reference
# refresh container
# return job reference

sub refreshContainer {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::refreshContainer",1);

    my %refreshHash;

    if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.9.0)) {
      %refreshHash = ();
    } else {
      %refreshHash =  (
        "type" => "JSDataContainerRefreshParameters",
        "forceOption" => JSON::false
      );
    }

    my $operation = "resources/json/delphix/jetstream/container/" . $reference . "/refresh";
    my $json_data = to_json(\%refreshHash);

    return $self->runJobOperation($operation, $json_data);

}



# Procedure resetContainer
# parameters:
# - reference
# reset container
# return job reference

sub resetContainer {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::resetContainer",1);

    my %resetHash;

    if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.9.0)) {
      %resetHash = ();
    } else {
      %resetHash =  (
        "type" => "JSDataContainerResetParameters",
        "forceOption" => JSON::false
      );
    }

    my $operation = "resources/json/delphix/jetstream/container/" . $reference . "/reset";
    my $json_data = to_json(\%resetHash);

    return $self->runJobOperation($operation, $json_data);

}

# Procedure enableContainer
# parameters:
# - reference
# enable container
# return job reference

sub enableContainer {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::enableContainer",1);

    if ($self->{_jscontainer}->{$reference}->{state} eq "ONLINE") {
      print "Container is already online\n";
      return undef;
    } else {
      my $operation = "resources/json/delphix/jetstream/container/" . $reference . "/enable";
      return $self->runJobOperation($operation, '{}');
    }

}


# Procedure disableContainer
# parameters:
# - reference
# disable container
# return job reference

sub disableContainer {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::disableContainer",1);

    if ($self->{_jscontainer}->{$reference}->{state} eq "OFFLINE") {
      print "Container is already offline\n";
      return undef;
    } else {
      my $operation = "resources/json/delphix/jetstream/container/" . $reference . "/disable";
      return $self->runJobOperation($operation, '{}');
    }
}

# Procedure addOwner
# parameters:
# - reference
# - user ref
# add owner to container
# return job reference

sub addOwner {
    my $self = shift;
    my $reference = shift;
    my $userref = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::addOwner",1);
    my %addownhash = (
      "type" => "JSDataContainerModifyOwnerParameters",
      "owner" => $userref
    );

    my $json_date = to_json(\%addownhash);

    my $operation = "resources/json/delphix/jetstream/container/" . $reference . "/addOwner";
    return $self->runJobOperation($operation, $json_date, 'ACTION');

}

# Procedure removeOwner
# parameters:
# - reference
# - user ref
# remove owner from container
# return job reference

sub removeOwner {
    my $self = shift;
    my $reference = shift;
    my $userref = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::removeOwner",1);
    my %addownhash = (
      "type" => "JSDataContainerModifyOwnerParameters",
      "owner" => $userref
    );

    my $json_date = to_json(\%addownhash);

    my $operation = "resources/json/delphix/jetstream/container/" . $reference . "/removeOwner";
    return $self->runJobOperation($operation, $json_date, 'ACTION');

}

# Procedure createContainer
# parameters:
# - container name
# - template reference
# - container def
# - owner array
# create container
# return job reference

sub createContainer {
    my $self = shift;
    my $name = shift;
    my $template_ref = shift;
    my $container_def = shift;
    my $owners_array = shift;
    my $dontrefresh = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::createContainer",1);

    my $operation = "resources/json/delphix/jetstream/container";


    my @datasources;

    for my $contitem (@{$container_def}) {
      my %conthashitem = (
        "type" => "JSDataSourceCreateParameters",
        "source" => {
            "type" => "JSDataSource",
            "priority" => 1,
            "name" => $contitem->{source}
        },
        "container" => $contitem->{vdb_ref}
      );
      push(@datasources, \%conthashitem);

    }

    my %conthash;

    if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.8.2)) {
      if (defined($dontrefresh)) {
        print "Your Delphix Engine version doesn't allow JS container creation without refresh.\n";
        return undef;
      }
      %conthash = (
        "type" => "JSDataContainerCreateParameters",
        "dataSources" => \@datasources,
        "name" => $name,
        "template" => $template_ref,
        "owners" => $owners_array,
        "timelinePointParameters" => {
            "type" => "JSTimelinePointLatestTimeInput",
            "sourceDataLayout" => $template_ref
        }
      );
    } else {
      if (defined($dontrefresh)) {
        %conthash = (
          "type" => "JSDataContainerCreateWithoutRefreshParameters",
          "dataSources" => \@datasources,
          "name" => $name,
          "template" => $template_ref,
          "owners" => $owners_array
        );
      } else {
        %conthash = (
          "type" => "JSDataContainerCreateWithRefreshParameters",
          "dataSources" => \@datasources,
          "name" => $name,
          "template" => $template_ref,
          "owners" => $owners_array,
          "timelinePointParameters" => {
              "type" => "JSTimelinePointLatestTimeInput",
              "sourceDataLayout" => $template_ref
          }
        );
      }
    }

    my $json_data = to_json(\%conthash);
    return $self->runJobOperation($operation, $json_data);

}

# Procedure deleteContainer
# parameters:
# - container ref
# - dropvdb
# drop container
# return job reference

sub deleteContainer {
    my $self = shift;
    my $container_ref = shift;
    my $dropvdb = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::deleteContainer",1);

    my $operation = "resources/json/delphix/jetstream/container/" . $container_ref . "/delete";

    my $drop;

    if ($dropvdb eq 'yes') {
      $drop = JSON::true
    } else {
      $drop = JSON::false
    }

    my %dropcont = (
        "type" => "JSDataContainerDeleteParameters",
        "deleteDataSources" => $drop
    );

    my $json_data = to_json(\%dropcont);
    return $self->runJobOperation($operation, $json_data);

}

#     "timelinePointParameters": {
#         "type": "JSTimelinePointTimeInput",
#         "branch": "JS_BRANCH-26",
#         "time": "2017-03-18T00:00:00.000Z"
#     }
# }


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

    logger($self->{_debug}, "Entering JS_bookmark_obj::runJobOperation",1);
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
