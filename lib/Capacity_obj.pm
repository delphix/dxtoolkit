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
# Copyright (c) 2015-2017 by Delphix. All rights reserved.
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
    
    return $self;
}


# Procedure LoadSnapshots
# parameters: 
# - db reference
# Return snapshot space information


sub LoadSnapshots {
  my $self = shift;
  my $db_ref = shift;
  my $all_snaps;
  logger($self->{_debug}, "Entering Capacity_obj::LoadSnapshots",1);    
  if ($self->{_dlpxObject}->getApi() lt '1.9') {
    $all_snaps = $self->LoadSnapshots_18($db_ref);
  } else {
    $all_snaps = $self->LoadSnapshots_19($db_ref);
  }
  
  return $all_snaps;

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

        $dbutil_hash{group_name} = $self->{_databases}->{$db_ref}->{groupName};
        $dbutil_hash{storageContainer} = $self->{_databases}->{$db_ref}->{storageContainer};
        $dbutil_hash{timestamp} = $self->{_databases}->{$db_ref}->{timestamp};
        $dbutil_hash{parent} = $self->{_databases}->{$db_ref}->{parent};

        my $snapsum = 0;


        if (defined($details) && (lc $details eq 'all')) {
            my $all_snaps = $self->LoadSnapshots($db_ref);
            
            for my $snapitem ( @{$all_snaps} ) {
                $snapsum = $snapsum + $snapitem->{space};
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


# Procedure forcerefesh
# Force refresh for API > 1.9


sub forcerefesh {
    my $self = shift; 
    logger($self->{_debug}, "Entering Capacity_obj::forcerefesh",1);    

    my $ret;

    if ($self->{_dlpxObject}->getApi() lt '1.9') {
      print "Refresh not supported for engine version < 5.2.0\n";
      return 0;
    } else {
      my $operation = "resources/json/delphix/capacity/refresh";
      my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, '{}');
      my $jobno;
      if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        $jobno = $result->{job};
      } else {
        print "No data returned for $operation. Try to increase timeout \n";
      }    
      
      if (defined($jobno)) {
        $ret = Toolkit_helpers::waitForJob($self->{_dlpxObject}, $jobno, "Capacity data refreshed.");
      } else {
        $ret = 1;
      }
    }
    return $ret;
}

# Procedure LoadSnapshots_18
# parameters: 
# - reference db
# Return snapshot information for API < 1.9


sub LoadSnapshots_18 {
    my $self = shift; 
    my $reference = shift;
    logger($self->{_debug}, "Entering Capacity_obj::LoadSnapshots_18",1);    

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


        $snapshots{$snapitem}{"space"} = $space/1024/1024/1024;
        push (@snapshots_ret, $snapshots{$snapitem});


    }


    return \@snapshots_ret;

}


# Procedure LoadSnapshots_19
# parameters: 
# - reference db
# Return snapshot information for API >= 1.9


sub LoadSnapshots_19 {
    my $self = shift; 
    my $reference = shift;
    logger($self->{_debug}, "Entering Capacity_obj::LoadSnapshots_19",1);    

    my @snapshots_ret;

    my $operation = "resources/json/delphix/capacity/snapshot?container=" . $reference;
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    my %snapshots;

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};

        for my $snapitem (@res) {
            $snapitem->{"space"} = $snapitem->{"space"}/1024/1024/1024;
            push (@snapshots_ret, $snapitem);
        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
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
          my $dbref = $dbitem->{container};
          if (defined($dbref)) {
            $databases->{$dbref} = $dbitem;
          } else {
            $databases->{"Heldspace"} = $dbitem;
          }
        } 

    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
    
}

# Procedure LoadSystemHistory
# parameters: 
# - start date
# - end date
# - resolution in sec
# Load system capacity history from Delphix Engine

sub LoadSystemHistory 
{
    my $self = shift;
    my $startDate = shift;
    my $endDate = shift;
    my $resolution = shift;
    logger($self->{_debug}, "Entering Capacity_obj::LoadSystemHistory",1);
    my $operation = "resources/json/delphix/capacity/system/historical?resolution=" . $resolution;
    
    if (defined($startDate)) {
      $operation = $operation . "&startDate=" . $startDate;
    }
    
    if (defined($endDate)) {
      $operation = $operation . "&endDate=" . $endDate;
    }
    
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    my @res;
    my @sorted;

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {

        @res = @{$result->{result}};
        @sorted = sort { $a->{timestamp} cmp $b->{timestamp} } @res;
        $self->{_systemHistory} = \@sorted;

    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
    
}


# Procedure processSystemHistory
# parameters: 
# - output
# - details
# Process capacity history and put into Formatter object

sub processSystemHistory 
{
    my $self = shift;
    my $output = shift;
    my $details = shift;
    logger($self->{_debug}, "Entering Capacity_obj::processSystemHistory",1);

    my $total;
    my $enginename = $self->{_dlpxObject}->getEngineName();
    my $enginezone = $self->{_dlpxObject}->getTimezone();
    
    my $histtime;
    my $usage;

    
    for my $histitem (@{$self->{_systemHistory}}) {

      my $time = Toolkit_helpers::convert_from_utc($histitem->{timestamp}, $enginezone, 1);
      
      if (defined($time)) {
          $histtime = $time;
      } else {
          $histtime = 'N/A';
      }

      $total = ($histitem->{source}->{actualSpace} + $histitem->{virtual}->{actualSpace});
      $usage = $total / $histitem->{totalSpace} * 100;
      
      if (defined($details)) {

        $output->addLine(
          $enginename,
          $histtime,
          sprintf("%15.2f" ,$histitem->{source}->{actualSpace}/1024/1024/1024),
          sprintf("%15.2f" ,$histitem->{source}->{activeSpace}/1024/1024/1024),
          sprintf("%15.2f" ,$histitem->{source}->{logSpace}/1024/1024/1024),
          sprintf("%15.2f" ,$histitem->{source}->{syncSpace}/1024/1024/1024),
          sprintf("%15.2f" ,$histitem->{virtual}->{actualSpace}/1024/1024/1024),
          sprintf("%15.2f" ,$histitem->{virtual}->{activeSpace}/1024/1024/1024),
          sprintf("%15.2f" ,$histitem->{virtual}->{logSpace}/1024/1024/1024),
          sprintf("%15.2f" ,$histitem->{virtual}->{syncSpace}/1024/1024/1024),
          sprintf("%15.2f" ,$total/1024/1024/1024),
          sprintf("%12.2f" ,$usage)
        );
        
      } else {
        
        $output->addLine(
          $enginename,
          $histtime,
          sprintf("%12.2f" ,$histitem->{source}->{actualSpace}/1024/1024/1024),
          sprintf("%12.2f" ,$histitem->{virtual}->{actualSpace}/1024/1024/1024),
          sprintf("%12.2f" ,$total/1024/1024/1024),
          sprintf("%12.2f" ,$usage)
        );
      }
            
    }
}

1;