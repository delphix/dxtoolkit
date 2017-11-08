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
# Program Name : JS_bookmark_obj.pm
# Description  : Delphix Engine JS branch
# Author       : Marcin Przepiorowski
# Created      : Apr 2016 (v2.2.4)
#


package JS_bookmark_obj;

use warnings;
use strict;
use Data::Dumper;
use Date::Manip;
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
    my $container = shift;
    my $debug = shift;
    logger($debug, "Entering JS_bookmark_obj::constructor",1);

    my %jsbookmarks;
    my $self = {
        _jsbookmarks => \%jsbookmarks,
        _dlpxObject => $dlpxObject,
        _template => $template,
        _container => $container,
        _debug => $debug
    };
    
    bless($self,$classname);
    
    $self->loadJSBoomkarkList($debug);
    return $self;
}


# Procedure getJSBookmarkByName
# parameters: 
# - name 
# Return bookmark reference for particular name

sub getJSBookmarkByName {
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering JS_bookmark_obj::getJSBookmarkByName",1);    
    my $ret;

    #print Dumper $$config;

    for my $bookitem ( sort ( keys %{$self->{_jsbookmarks}} ) ) {

        if ( $self->getName($bookitem) eq $name) {
            $ret = $bookitem; 
        }
    }

    return $ret;
}

# Procedure getBookmark
# parameters: 
# - reference
# Return bookmark hash for specific bookmark reference

sub getJSBookmark {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering JS_bookmark_obj::getBookmark",1);    

    my $jsbookmarks = $self->{_jsbookmarks};
    return $jsbookmarks->{$reference};
}


# Procedure existBookmarkTimeForBranch
# parameters:
# - time 
# - branch
# Return true if bookmark exist for specific bookmark reference

sub existJSBookmarkTimeForBranch {
    my $self = shift;
    my $time = shift;
    my $branch = shift;
    logger($self->{_debug}, "Entering JS_bookmark_obj::existJSBookmarkTimeForBranch",1);  
    my $book = grep { (($self->getJSBookmarkTime($_, 1) eq $time ) && ($self->getJSBookmarkBranch($_) eq $branch)) } keys %{$self->{_jsbookmarks}};

    return $book;

}

# Procedure existBookmarkNameForBranch
# parameters:
# - name 
# - branch
# Return true if bookmark exist for specific bookmark reference

sub existBookmarkNameForBranch {
    my $self = shift;
    my $name = shift;
    my $branch = shift;
    logger($self->{_debug}, "Entering JS_bookmark_obj::existBookmarkNameForBranch",1);  
    my $book = grep { (($self->getName($_) eq $name ) && ($self->getJSBookmarkBranch($_) eq $branch)) } keys %{$self->{_jsbookmarks}};

    return $book;

}


# Procedure getBookmarkBranch
# parameters: 
# - reference
# Return bookmark branch for specific bookmark reference

sub getJSBookmarkBranch {
    my $self = shift;
    my $reference = shift;
    my $native = shift;
    
    logger($self->{_debug}, "Entering JS_bookmark_obj::getBookmarkBranch",1);    

    my $jsbookmarks = $self->{_jsbookmarks};

    my $branch = $jsbookmarks->{$reference}->{branch};

    return $branch;
}

# Procedure getBookmarkTemplate
# parameters: 
# - reference
# Return bookmark template for specific bookmark reference

sub getJSBookmarkTemplate {
    my $self = shift;
    my $reference = shift;
    my $native = shift;
    
    logger($self->{_debug}, "Entering JS_bookmark_obj::getBookmarkTemplate",1);    

    my $jsbookmarks = $self->{_jsbookmarks};

    my $branch = $jsbookmarks->{$reference}->{template};

    return $branch;
}


# Procedure getJSBookmarkTemplateName
# parameters: 
# - reference
# Return bookmark template name for specific bookmark reference

sub getJSBookmarkTemplateName {
    my $self = shift;
    my $reference = shift;
    my $native = shift;
    
    logger($self->{_debug}, "Entering JS_bookmark_obj::getJSBookmarkTemplateName",1);    

    my $jsbookmarks = $self->{_jsbookmarks};

    my $template_name = $jsbookmarks->{$reference}->{templateName};

    return defined($template_name) ? $template_name : 'N/A' ;
}


# Procedure getJSBookmarkContainerName
# parameters: 
# - reference
# Return bookmark container name for specific bookmark reference

sub getJSBookmarkContainerName {
    my $self = shift;
    my $reference = shift;
    my $native = shift;
    
    logger($self->{_debug}, "Entering JS_bookmark_obj::getJSBookmarkContainerName",1);    

    my $jsbookmarks = $self->{_jsbookmarks};

    my $container_name = $jsbookmarks->{$reference}->{containerName};

    return defined($container_name) ? $container_name : 'N/A' ;;
}

# Procedure getJSBookmarkContainer
# parameters: 
# - reference
# Return bookmark container name for specific bookmark reference

sub getJSBookmarkContainer {
    my $self = shift;
    my $reference = shift;
    my $native = shift;
    
    logger($self->{_debug}, "Entering JS_bookmark_obj::getJSBookmarkContainer",1);    

    my $jsbookmarks = $self->{_jsbookmarks};

    my $container_ref = $jsbookmarks->{$reference}->{container};
  
    return defined($container_ref) ? $container_ref : 'N/A' ;
}




# Procedure getBookmarkTime
# parameters: 
# - reference
# - native
# Return bookmark time for specific bookmark reference

sub getJSBookmarkTime {
    my $self = shift;
    my $reference = shift;
    my $native = shift;
    
    logger($self->{_debug}, "Entering JS_bookmark_obj::getBookmarkTime",1);    

    my $jsbookmarks = $self->{_jsbookmarks};

    my $timestamp = $jsbookmarks->{$reference}->{timestamp};

    if (!defined($native)) {
        $timestamp =~ s/T/ /;
        $timestamp =~ s/\....Z//;
    }

    return $timestamp;
}




# Procedure getBookmarkTime
# parameters: 
# - reference
# Return bookmark time for specific bookmark reference

sub getJSBookmarkTimeWithTimestamp {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering JS_bookmark_obj::getBookmarkTimeWithTimestamp",1);    

    my $detz = $self->{_dlpxObject}->getTimezone();



    my $jsbookmarks = $self->{_jsbookmarks};
    my $zulutime = $jsbookmarks->{$reference}->{timestamp};

    my $tz = new Date::Manip::TZ;
    my $dt = ParseDate($zulutime);

    my $ret;

    my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($dt, $detz);

    if (! $err) {
        $ret = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d %s",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5], $abbrev);
    } else {
        $ret = 'N/A';
    }

    return $ret;
}


# Procedure getJSBookmarkList
# parameters: 
# Return JS bookmark list

sub getJSBookmarkList {
    my $self = shift;
    my $cont_only = shift;
    
    logger($self->{_debug}, "Entering JS_bookmark_obj::getJSBookmarkList",1);    

    my @arrret = sort { $self->getJSBookmarkTime($a) cmp $self->getJSBookmarkTime($b) } (keys %{$self->{_jsbookmarks}} );

    if (defined($cont_only)) {
        @arrret = grep { $self->getJSBookmarkContainerName($_) ne 'N/A'  } @arrret;
    }

    return \@arrret;
}


# Procedure getName
# parameters: 
# - reference
# Return JS bookmark name for specific branch reference

sub getName {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering JS_bookmark_obj::getName",1);   

    my $jsbookmarks = $self->{_jsbookmarks};
    return $jsbookmarks->{$reference}->{name};
}



# Procedure loadJSBranchList
# parameters: none
# Load a list of branch objects from Delphix Engine

sub loadJSBoomkarkList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering JS_bookmark_obj::loadJSBranchList",1);   

    my $operation = "resources/json/delphix/jetstream/bookmark?";

    if (defined($self->{_template})) {
        $operation = $operation . "template=" . $self->{_template} . "&";
    }

    if (defined($self->{_container})) {
        $operation = $operation . "container=" . $self->{_container} . "&";
    }

    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $jsbookmarks = $self->{_jsbookmarks};

        for my $bookmarkitem (@res) {
            $jsbookmarks->{$bookmarkitem->{reference}} = $bookmarkitem;
        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}

# Procedure createBookmark
# parameters: 
# - name
# - branch
# - datalayout
# - time
# - zulu - is this time in zulu timezone
# - expireat - zulu time when bookmark should expired

sub createBookmark {
    my $self = shift;
    my $name = shift;
    my $branch = shift;
    my $datalayout = shift;
    my $time = shift;
    my $zulu = shift;
    my $expireat = shift;
    
    logger($self->{_debug}, "Entering JS_bookmark_obj::createBookmark",1);   


    my %createbookmark_hash;


    if (lc $time eq 'latest') {
        %createbookmark_hash = (
            "type" => "JSBookmarkCreateParameters",
            "bookmark" => {
                "type" => "JSBookmark",
                "name" => $name,
                "branch" => $branch
            },
            "timelinePointParameters" => {
                "type" => "JSTimelinePointLatestTimeInput",
                "sourceDataLayout" => $datalayout
            }
        );
    } else {

        my $zulutime;

        # print Dumper $time;
        # print Dumper $zulu;

        if (defined($zulu)) {
            $zulutime = $time;
        } else { 
            my $tz = new Date::Manip::TZ;
            my $dt = ParseDate($time);
            my $detz = $self->{_dlpxObject}->getTimezone();

            my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_to_gmt($dt, $detz);

            if (! $err) {
                $zulutime = sprintf("%04.4d-%02.2d-%02.2dT%02.2d:%02.2d:%02.2d.000Z",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);
            } else {
                print "Error in timestamp convertion to GMT. \n";
                return undef;
            }

        }

        if ( $self->existJSBookmarkTimeForBranch($zulutime, $branch) ) {
            print "Bookmark for time $time and branch already exist. \n";
            return undef;
        }

        
        if ($self->{_dlpxObject}->getApi() lt "1.8") {
          %createbookmark_hash = (
              "type" => "JSBookmarkCreateParameters",
              "bookmark" => {
                  "type" => "JSBookmark",
                  "name" => $name,
                  "branch" => $branch
              },
              "timelinePointParameters" => {
                  "type" => "JSTimelinePointTimeInput",
                  "sourceDataLayout" => $datalayout,
                  "time" => $zulutime
              }
          );
        } else {
          %createbookmark_hash = (
              "type" => "JSBookmarkCreateParameters",
              "bookmark" => {
                  "type" => "JSBookmark",
                  "name" => $name,
                  "branch" => $branch
              },
              "timelinePointParameters" => {
                  "type" => "JSTimelinePointTimeInput",
                  "branch" => $branch,
                  "time" => $zulutime
              }
          ); 
        }
        
    }

    if ($self->{_dlpxObject}->getApi() gt "1.6") {
      if (defined($expireat)) {
        $createbookmark_hash{"bookmark"}{expiration} = $expireat;
      }
    }

    if ( $self->existBookmarkNameForBranch($name, $branch) ) {
        print "Bookmark for name $name and branch already exist. \n";
        return undef;
    }


    my $json_data = to_json(\%createbookmark_hash, {pretty=>1});

    #print Dumper $json_data;

    my $operation = "resources/json/delphix/jetstream/bookmark";

    return $self->runJobOperation($operation, $json_data);
}


# Procedure deleteBookmark
# parameters: 
# - reference
# Delete bookmark 
# return job reference

sub deleteBookmark {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::deleteBookmark",1);

    my $operation = "resources/json/delphix/jetstream/bookmark/" . $reference . "/delete";

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