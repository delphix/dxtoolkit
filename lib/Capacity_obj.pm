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
# Program Name : Capacity_obj.pm
# Description  : Delphix Engine Capacity object
# It's include the following classes:
# - Capacity_obj - class which map a Delphix Engine capacity API object
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#
#



package Capacity_obj;

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
    logger($debug, "Entering Capacity_obj::constructor",1);

    my %capacityGroups;
    my %databases;
    my %groups;
    my $self = {
        _capacityGroups => \%capacityGroups,
        _databases => \%databases,
        _groups => \%groups,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);
    
    $self->LoadDatabases();
    return $self;
}


# Procedure getDetailedDBUsage
# parameters: 
# - db reference
# Return detailed usage of database as hash


sub getDetailedDBUsage {
    my $self = shift; 
    my $db_ref = shift;
    my $details = shift;
    logger($self->{_debug}, "Entering Capacity_obj::getDetailedDBUsage",1);    

    my %dbutil_hash;

    if (defined($self->{_databases}->{$db_ref}->{breakdown}->{actualSpace})) {

        $dbutil_hash{totalsize} = $self->{_databases}->{$db_ref}->{breakdown}->{actualSpace}/1024/1024/1024; # whole db 
        $dbutil_hash{currentcopy} = $self->{_databases}->{$db_ref}->{breakdown}->{activeSpace}/1024/1024/1024; # current size

        $dbutil_hash{dblogs} = $self->{_databases}->{$db_ref}->{breakdown}->{logSpace}/1024/1024/1024; # logs
        $dbutil_hash{snapshots_total} = $self->{_databases}->{$db_ref}->{breakdown}->{syncSpace}/1024/1024/1024; #all snaps
        $dbutil_hash{unvirtualized} = $self->{_databases}->{$db_ref}->{breakdown}->{unvirtualizedSpace}/1024/1024/1024; # non delphix

        my $snapsum = 0;


        if (defined($details) && (lc $details eq 'all')) {
            my $all_snaps = $self->LoadSnapshots($db_ref);


            for my $snapitem ( @{$all_snaps} ) {
                $snapsum = $snapsum + $snapitem->{snapshot_usedspace};
            }


            $dbutil_hash{snapshots_shared} = $dbutil_hash{snapshots_total} - $snapsum;
            $dbutil_hash{snapshots_list} = $all_snaps;

        }  

    } else {
        $dbutil_hash{totalsize} = 0;
        $dbutil_hash{currentcopy} = 0;

        $dbutil_hash{dblogs} = 0;
        $dbutil_hash{snapshots_total} = 0;
        $dbutil_hash{unvirtualized} = 0;
        $dbutil_hash{snapshots_shared} = 0;
        $dbutil_hash{snapshots_list} = 0;

    }

    return \%dbutil_hash;
}


# Procedure getDetailedDBUsage
# parameters: 
# - db reference
# Return detailed usage of database as hash


# sub getDetailedDBUsage_old {
#     my $self = shift; 
#     my $db_ref = shift;
#     logger($self->{_debug}, "Entering Capacity_obj::getDetailedDBUsage_old",1);    
#     my $operation = "resources/json/domain/DOMAIN/group/dummy/database/" . $db_ref. "/capacity";
#     my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

#     #build snapshot hash with ts as key
#     my %snaphash;
#     my $snapsize_db = 0;
#     my $snapsize_ext = 0;

#     for my $snapitem (@{$result->{view}->{snapshotList} }) {
#         my %snaphashitem = (
#             dbsnapsize => $snapitem->{actualUsedSize},
#             externalsnapsize => $snapitem->{externalActualUsedSize}
#         );
#         $snaphash{$snapitem->{timestamp}} = \%snaphashitem;
#         $snapsize_db = $snapsize_db + $snapitem->{actualUsedSize};
#         $snapsize_ext = $snapsize_ext + $snapitem->{externalActualUsedSize};
#     }

#     # sort snapshot into array
#     my @snapshot_list;

#     for my $snapitem ( sort ( keys %snaphash ) ) {
#         push (@snapshot_list, $snaphash{$snapitem});
#     }

#     my %dbutil_hash;

#     $dbutil_hash{currentCopySize} = $result->{view}->{currentCopySize};

#     $dbutil_hash{snapshotsdb_total} = $snapsize_db + $result->{view}->{snapshotsSharedSize};
#     $dbutil_hash{snapshotsext_total} = $snapsize_ext + $result->{view}->{externalSharedSize};
#     $dbutil_hash{dblogs} = $result->{view}->{logActualUsedSize};
#     $dbutil_hash{tempfiles} = $result->{view}->{tempActualUsedSize};


#     $dbutil_hash{snapshots_shared} = $result->{view}->{snapshotsSharedSize};
#     $dbutil_hash{external_shared} = $result->{view}->{externalSharedSize};
#     $dbutil_hash{snapshots_list} = \@snapshot_list;
#     $dbutil_hash{totalsize} = $result->{view}->{actualUsedSize}; 


#     return \%dbutil_hash;
# }



# Procedure getDatabaseUsage
# parameters: 
# - reference
# Return usage of database in GB


sub getDatabaseUsage {
    my $self = shift; 
    my $reference = shift;
    logger($self->{_debug}, "Entering Capacity_obj::getDatabaseUsage",1);    
    my $size;
    if (defined($self->{_databases}->{$reference}->{breakdown}->{actualSpace})) {
        $size = sprintf("%2.2f",$self->{_databases}->{$reference}->{breakdown}->{actualSpace}/1024/1024/1024);
    } else {
        $size = 'N/A';
    }
    return $size;
}


# Procedure LoadSnapshots
# parameters: 
# - reference db
# Return usage of database in GB


sub LoadSnapshots {
    my $self = shift; 
    my $reference = shift;
    logger($self->{_debug}, "Entering Capacity_obj::LoadSnapshots",1);    

    my @snapshots_ret;

    my $operation = "resources/json/delphix/capacity/snapshot?container=" . $reference;
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    my %snapshots;

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};

        for my $snapitem (@res) {
            $snapshots{$snapitem->{snapshotTimestamp}} = $snapitem;
        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }

    $operation = "resources/json/delphix/snapshot/space";

    my %snapspace = (
        "type" => "SnapshotSpaceParameters"  
    );

    for my $snapitem ( sort ( keys %snapshots ) ) {
        my @snapshot_list = ( $snapshots{$snapitem}->{snapshot} );

        $snapspace{"objectReferences"} = \@snapshot_list;

        my $json_data = encode_json(\%snapspace);

        my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

        my $space;

        if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
            $space = $result->{result}->{totalSize};
        } else {
            if (defined($result->{error})) {
                print "Problem with space calculation for snapshot " . $result->{error}->{details} . "\n";
                logger($self->{_debug}, "Can't submit job for operation $operation",1);
                logger($self->{_debug}, $result->{error}->{action} ,1);
            } else {
                print "Unknown error. Try with debug flag\n";
            }
        }


        $snapshots{$snapitem}{"snapshot_usedspace"} = $space/1024/1024/1024;
        push (@snapshots_ret, $snapshots{$snapitem});


    }

    return \@snapshots_ret;

}

# Procedure LoadDatabases
# parameters: none
# Load a list of Capacity objects from Delphix Engine

sub LoadDatabases 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Capacity_obj::LoadDatabases",1);
    my $operation = "resources/json/delphix/capacity/consumer";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    my @res;
    my $databases;

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {

        @res = @{$result->{result}};
        $databases = $self->{_databases};
    

        for my $dbitem (@res) {
                $databases->{$dbitem->{container}} = $dbitem;
        } 

    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
    
}


1;