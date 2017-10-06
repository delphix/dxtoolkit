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

my $version = $Toolkit_helpers::version;

my $port = 514;
my $protocol = 'tcp';

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'syslog=s' => \(my $syslog),
  'port=s' => (\$port),
  'severity=s' => \(my $severity),
  'facility=s' => \(my $facility), 
  'protocol=s' => (\$protocol),
  'all' => (\my $all),
  'dever=s' => \(my $dever),
  'version' => \(my $print_version),
  'debug:n' => \(my $debug),
  'configfile|c=s' => \(my $config_file)
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
$engine_obj->load_config($config_file);


my $handler = new Syslog_wrap ( $syslog, $port, $protocol, $debug );



if (!defined($handler)) {
  print "Syslog connection error\n";
  exit(1);
}

if (defined($facility)) {
  if ($handler->set_facility($facility)) {
    print "Problem with setting facility\n";
    exit(1);
  }
}

if (defined($severity)) {
  if ($handler->set_severity($severity)) {
    print "Problem with setting severity\n";
    exit(1);
  }
}


my $count = 0;

my $f = new Formater();

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $ret = 0;


for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $engine\n\n";
    $ret = $ret + 1;
    if (defined($load_state->{$engine})) {
      $save_state{$engine} = (
        {
          "last_start_time" => $load_state->{$engine}->{last_start_time},
          "running_actions" => $load_state->{$engine}->{running_actions}
        }
      );
    }
    next;
  };


  my $last_start_time;
  my $st_timestamp;
  if (defined($load_state->{$engine}) && defined($load_state->{$engine}->{last_start_time})) {
    my $delta = $load_state->{$engine}->{last_start_time};
    my ($d, $md) = ($delta =~ /(\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.)(\d\d\d)/ );
    my $newtime = sprintf("%s%sZ", $d, $md+1);
    $st_timestamp = uri_escape($newtime);
    $last_start_time = $load_state->{$engine}->{last_start_time};
  } else {
    $st_timestamp = Toolkit_helpers::timestamp("-7days", $engine_obj);
  }


  if (defined($load_state->{$engine}) && defined($load_state->{$engine}->{running_actions})) {
    %running_actions = %{$load_state->{$engine}->{running_actions}};
  }



  my $actions = new Action_obj($engine_obj, $st_timestamp, undef, undef);
  my $users = new Users ($engine_obj, undef);




  $f->addHeader(
      {'Appliance',   20},
      {'ActionID',    20},
      {'ActionParentID', 20},
      {'StartTime',   30},
      {'State',       12},
      {'User name',   20},
      {'User role',   20},
      {'User auth',   10},
      {'Worksource',  10},
      {'Type',        20},
      {'Details',     80},
      {'Failure Action', 80}
  );


  $actions->loadActionListbyID(\%running_actions);



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
        $actions->getWorksource($actionitem),
        $action,
        $actions->getDetails($actionitem),
        ''
      );
      $count=$count+1;
    }

    



    if (($action ne 'USER_LOGIN') && ($action ne 'USER_LOGOUT') && ($action ne 'USER_FAILED_LOGIN') ) {
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
          $actions->getWorksource($actionitem),
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
  
  if (!defined($last_start_time)) {
    $last_start_time = $load_state->{$engine}->{last_start_time};
  }

  $save_state{$engine} = (
    {
      "last_start_time" => $last_start_time,
      "running_actions" => \%running_actions
    }
  );
  
}

#$f->sortbytextcolumn(3);
# $f->savecsv();
# print Dumper $last_start_time;
# print Dumper \%running_actions;




open ($json_stream, '>', 'syslog.dat') or die ("Can't load config file : $!");
local $/ = undef;
my $json = new JSON();
my $json_data =  $json->pretty->encode( \%save_state );
print $json_stream $json_data;
#print Dumper $json_data;
close($json_stream);


print "$count lines sent to syslog\n";
$ret = $ret + $f->sendtosyslog($handler);

#$f->savecsv();

exit $ret;

__DATA__

=head1 SYNOPSIS

 dx_syslog       [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                 -syslog syslog_server 
                 [ -port syslog_port] 
                 [ -protocol tcp|udp] 
                 [ -facility facility_name]
                 [ -severity severity_name]
                 [ -help|? ] 
                 [ -debug ]

=head1 DESCRIPTION

Get the list of actions from Delphix Engine and send to external syslog

=head1 ARGUMENTS

=over 4

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Run script for all Delphix Engines from config file

=item B<-configfile file>
Location of the configuration file.
A config file search order is as follow:
- configfile parameter
- DXTOOLKIT_CONF variable
- dxtools.conf from dxtoolkit location

=back

=head1 OPTIONS

=over 4

=item B<-syslog syslog_server>
IP or name of syslog server


=back

=head1 OPTIONS

=over 3


=item B<-port syslog_port>
Port of syslog server - default 514

=item B<-protocol tcp|udp>
Protocol for syslog server communication - default TCP

=item B<-facility facility_name>
Setting a syslog facility. Default value user.
Allowed names:
kern, user, mail, daemon, auth, syslog, lpr,
news, uucp, cron, authpriv, ftp, local0, local1, local2, local3, 
local4, local5, local6, local7

=item B<-severity severity_name>
Setting a syslog severity. Default value info.
Allowed names:
emerg, alert, crit, err, warning, notice, info,debug

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging


=back

=head1 EXAMPLES

