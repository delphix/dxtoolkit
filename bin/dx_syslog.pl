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
# Copyright (c) 2017 by Delphix. All rights reserved.
# 
# Program Name : dx_syslog.pl
# Description  : Send data to syslog
# Author       : Marcin Przepiorowski

use strict;
use warnings;
use Data::Dumper;
use lib '../lib';
use Syslog_wrap;
use Formater;
use Date::Manip;
use Action_obj;
use Engine;
use Users;
use Try::Tiny;
use URI::Escape;
use Log::Syslog::Fast ':all';
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev); #avoids conflicts with ex host and help
use File::Basename;
use Pod::Usage;
use FindBin;

my $abspath = $FindBin::Bin;

my $version = '0.5';

my $port = 514;
my $protocol = 'tcp';

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'syslog=s' => \(my $syslog),
  'port=s' => (\$port),
  'protocol=s' => (\$protocol),
  'all' => (\my $all),
  'dever=s' => \(my $dever),
  'version' => \(my $print_version),
  'debug:n' => \(my $debug)
) or pod2usage(-verbose => 1,  -input=>\*DATA);

pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;  

if (!defined($syslog)) {
  print "Parameter -syslog is required\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit(1);
}


my %running_actions;
my %save_state;
my $load_state;
my $json_stream;

if (open ($json_stream, 'syslog.dat')) {
  local $/ = undef;
  my $json = JSON->new();
  try {
     $load_state = $json->decode(<$json_stream>) ;
  } catch {
     print "File not in JSON\n";
  };
  close($json_stream);
}

my $engine_obj = new Engine ($dever, $debug);
my $path = $FindBin::Bin;
my $config_file = $path . '/dxtools.conf';

$engine_obj->load_config($config_file);


my $handler = new Syslog_wrap ( $syslog, 514, 'tcp', undef );

if (!defined($handler)) {
  print "Syslog connection error\n";
  exit(1);
}

my $f = new Formater();

$engine_obj->dlpx_connect($dx_host);

my $last_start_time;
my $st_timestamp;
if (defined($load_state->{last_start_time})) {
  my $delta = $load_state->{last_start_time};
  my ($d, $md) = ($delta =~ /(\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.)(\d\d\d)/ );
  my $newtime = sprintf("%s%sZ", $d, $md+1);
  $st_timestamp = uri_escape($newtime);
  $last_start_time = $load_state->{last_start_time};
} else {
  $st_timestamp = Toolkit_helpers::timestamp("-120min", $engine_obj);
}

# REMOVE IT
$st_timestamp = Toolkit_helpers::timestamp("-1day", $engine_obj);
# END


if (defined($load_state->{running_actions})) {
  %running_actions = %{$load_state->{running_actions}};
}



my $actions = new Action_obj($engine_obj, $st_timestamp, undef, undef);
my $users = new Users ($engine_obj, undef);




$f->addHeader(
    {'Appliance',   20},
    {'ActionID',    20},
    {'ActionParentID', 20},
    {'StartTime',   30},
    {'State',       12},
    {'User',        20},
    {'User role',   20},
    {'User auth',   10},
    {'Type',        20},
    {'Details',     80},
    {'Failure Action', 80}
);


$actions->loadActionListbyID(\%running_actions);

my $count = 0;

for my $actionitem ( @{$actions->getActionList('asc', undef, undef)} ) {
  
  my $user = $actions->getUserRef($actionitem);
  my $userole;
  my $userauth; 
  if ($user ne 'N/A') {
    my $userobj = $users->getUser($user);
    if (defined($userobj->isJS())) {
      $userole = 'JetStream Only';
    } elsif (defined($userobj->isAdmin())) {
      $userole = 'Delphix Admin';
    } else {
      $userole = 'User with privs';
    }
    
    $userauth = $userobj->getAuthType();
  
  } else {
    $userole = '';
    $userauth = '';
  }
  
  my $state = $actions->getState($actionitem);
  
  my $action = $actions->getActionType($actionitem);

  if (!defined($running_actions{$actionitem})) {
    $f->addLine(
      $engine_obj->getIP($dx_host),
      $actionitem,
      $actions->getActionParent($actionitem),
      $actions->getStartTimeWithTZ($actionitem),
      'STARTED',
      $actions->getUserName($actionitem),
      $userole,
      $userauth,
      $action,
      $actions->getDetails($actionitem),
      ''
    );
    $count=$count+1;
  }

  



  if (($action ne 'USER_LOGIN') && ($action ne 'USER_LOGOUT')) {
    if ($state ne 'WAITING') {
      $f->addLine(
        $engine_obj->getIP($dx_host),
        $actionitem,
        $actions->getActionParent($actionitem),
        $actions->getEndTimeWithTZ($actionitem),
        $state,
        $actions->getUserName($actionitem),
        $userole,
        $userauth,
        $action,
        $actions->getFailureDescription($actionitem),
        $actions->getFailureAction($actionitem)
      );
      $count=$count+1;
      delete $running_actions{$actionitem};
    } else {
      $running_actions{$actionitem} = 1;
    }
  }
  
  if (!defined($last_start_time)) {
    $last_start_time = $actions->getStartTime($actionitem);
  }
  
  if ($last_start_time lt $actions->getStartTime($actionitem)) {
    $last_start_time = $actions->getStartTime($actionitem);
  }

}

$f->sortbytextcolumn(3);
$f->savecsv();
# print Dumper $last_start_time;
# print Dumper \%running_actions;

if (!defined($last_start_time)) {
  $last_start_time = $load_state->{last_start_time};
}

%save_state = (
  "last_start_time" => $last_start_time,
  "running_actions" => \%running_actions
);


open ($json_stream, '>', 'syslog.dat') or die ("Can't load config file : $!");
local $/ = undef;
my $json = new JSON();
my $json_data =  $json->pretty->encode( \%save_state );
print $json_stream $json_data;
#print Dumper $json_data;
close($json_stream);

print "$count lines sent to syslog\n";



#my $ret = $f->sendtosyslog($handler);

#exit $ret;

__DATA__

=head1 SYNOPSIS

 dx_syslog       [-engine|d <delphix identifier> | -all ] 
                 [-st timestamp] 
                 [-et timestamp] 
                 [-state state] 
                 [-type type] 
                 [-username username]
                 [-format csv|json ]  
                 [-outdir path]
                 [ --help|? ] [ -debug ]

=head1 DESCRIPTION

Get the list of actions from Delphix Engine.

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=head2 Filters

Filter faults using one of the following filters

=over 4

=item B<-state>
Action state - COMPLETED / WAITING / FAILED

=item B<-type>
Action type ex. HOST_UPDATE, SOURCES_DISABLE, etc,

=item B<-username>
Display only action performed by user

=back

=head1 OPTIONS

=over 3


=item B<-st timestamp>
Start time for faults list - default value is 7 days

=item B<-et timestamp>
End time for faults list 

=item B<-format>                                                                                                                                            
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-outdir path>                                                                                                                                            
Write output into a directory specified by path.
Files names will include a timestamp and type name

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=item B<-nohead>
Turn off header output

=back

=head1 EXAMPLES

