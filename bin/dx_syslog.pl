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


# my $f = Log::Syslog::Constants::LOG_USER;
# # 
# my $logger = Log::Syslog::Fast->new(LOG_TCP, "172.16.180.130", 514, $f, Log::Syslog::Constants::LOG_INFO, "servername", "Delphix");
# # 
# # # LOG_RFC5424 LOG_RFC3164
# # 
# # $logger->set_pid(0);
# # $logger->set_format(LOG_RFC3164);
# # $logger->send("11111\n", time);
# # 
# #
# 
# 
# $string = '2017-04-19 20:09:33 EST'; # or a wide range of other date formats
# $unix_time = UnixDate( ParseDate($string), "%s" );
# 
# print Dumper $unix_time;
#  
# $logger->set_pid(10);
# $logger->set_format(LOG_RFC5424);
# $logger->send("2222\n", $unix_time);
# 
# # for (my $i=1; $i<10; $i++ ) {
# # $logger->send("rjkeiojferiofhriufgerjfwhjgrew\n", time);
# # }
# 
# exit(1);

my %running_actions;
my %save_state;
my $load_state;

open (my $json_stream, 'syslog.dat') or die ("Can't load config file $fn : $!");
local $/ = undef;
my $json = JSON->new();
try {
   $load_state = $json->decode(<$json_stream>) ;
} catch {
   print "File not in JSON\n";
};
close($json_stream);



my $engine_obj = new Engine ($dever, 1);
my $path = $FindBin::Bin;
my $config_file = $path . '/dxtools.conf';

$engine_obj->load_config($config_file);
$engine_obj->dlpx_connect('Landshark51');


my $st_timestamp;
if (defined($load_state->{last_start_time})) {
  my $delta = $load_state->{last_start_time};
  print Dumper $delta;
  my ($d, $md) = ($delta =~ /(\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.)(\d\d\d)/ );
  print Dumper $d;
  print Dumper $md+1;
  my $newtime = sprintf("%s%sZ", $d, $md+1);
  print Dumper $newtime;
  $st_timestamp = uri_escape($newtime);
  print Dumper $st_timestamp;
} else {
  $st_timestamp = Toolkit_helpers::timestamp("-120min", $engine_obj);
}


if (defined($load_state->{running_actions})) {
  %running_actions = %{$load_state->{running_actions}};
}



my $actions = new Action_obj($engine_obj, $st_timestamp, undef, undef);
my $users = new Users ($engine_obj, undef);


my $syslog = new Syslog_wrap ( "172.16.180.130", 514, 'udp', undef );

my $f = new Formater();

# $f->addHeader(
#   {"msg", 1000},
#   {"cos",  20},
#   {"StartTime", 20}
# );
# 
# $f->addLine("message from dark side","aaaaaa","2017-04-26 11:31:59");

$f->addHeader(
    {'Appliance',   20},
    {'StartTime',   30},
    {'State',       12},
    {'User',        20},
    {'User role',   20},
    {'Type',        20},
    {'Details',     80},
    {'Failure Action', 80}
);


$actions->loadActionListbyID(\%running_actions);


my $last_start_time;

for my $actionitem ( @{$actions->getActionList('asc', undef, undef)} ) {
  
  my $user = $actions->getUserRef($actionitem);
  my $userole; 
  if ($user ne 'N/A') {
    my $userobj = $users->getUser($user);
    if (defined($userobj->isJS())) {
      $userole = 'JetStream Only';
    } elsif (defined($userobj->isAdmin())) {
      $userole = 'Delphix Admin';
    } else {
      $userole = 'User with privs';
    }
  
  } else {
    $userole = '';
  }
  
  my $state = $actions->getState($actionitem);
  
  my $action = $actions->getActionType($actionitem);

  if (!defined($running_actions{$actionitem})) {
    $f->addLine(
      $engine_obj->getIP($engine),
      $actions->getStartTimeWithTZ($actionitem),
      'STARTED',
      $actions->getUserName($actionitem),
      $userole,
      $action,
      $actions->getDetails($actionitem),
      ''
    );
  }

  



  if ($action ne 'USER_LOGIN') {
    if ($state ne 'WAITING') {
      $f->addLine(
        $engine_obj->getIP($engine),
        $actions->getEndTimeWithTZ($actionitem),
        $state,
        $actions->getUserName($actionitem),
        $userole,
        $action,
        $actions->getFailureDescription($actionitem),
        $actions->getFailureAction($actionitem)
      );
      delete $running_actions{$actionitem};
    } else {
      $running_actions{$actionitem} = 1;
    }
  }
  
  $last_start_time = $actions->getStartTime($actionitem);

}

$f->savecsv();
print Dumper $last_start_time;
print Dumper \%running_actions;

if (!defined($last_start_time)) {
  $last_start_time = $load_state->{last_start_time};
}

%save_state = (
  "last_start_time" => $last_start_time,
  "running_actions" => \%running_actions
);

open (my $json_stream, '>', 'syslog.dat') or die ("Can't load config file $fn : $!");
local $/ = undef;
my $json = new JSON();
my $json_data =  $json->pretty->encode( \%save_state );
print $json_stream $json_data;
print Dumper $json_data;
close($json_stream);

#$f->sendtosyslog($syslog);