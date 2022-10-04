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

# Program Name : dx_get_config.pl
# Description  : Get engine configuration
# Author       : Marcin Przepiorowski
# Created      : 15 Sep 2016 (v2.2.7)
#


use strict;
use warnings;
use JSON;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev); #avoids conflicts with ex host and help
use File::Basename;
use Pod::Usage;
use FindBin;
use Data::Dumper;
use File::Spec;

my $abspath = $FindBin::Bin;

use lib '../lib';
use Engine;
use Formater;
use Toolkit_helpers;
use System_obj;
use Storage_obj;
use Action_obj;


my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'format=s' => \(my $format),
  'debug:i' => \(my $debug),
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'nohead' => \(my $nohead),
  'version' => \(my $print_version),
  'configfile|c=s' => \(my $config_file)
) or pod2usage(-verbose => 1,  -input=>\*DATA);

pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;

my $engine_obj = new Engine ($dever, $debug);
$engine_obj->load_config($config_file);

if (defined($all) && defined($dx_host)) {
   print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
   pod2usage(-verbose => 1,  -input=>\*DATA);
   exit (1);
}



my $output = new Formater();



$output->addHeader(
  {'engine name',          35},
  {'parameter name',       30},
  {'value',                30}
);


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
   # main loop for all work
   if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $engine\n\n";
    $ret = $ret + 1;
    next;
   };

   if ($engine_obj->getCurrentUserType() ne 'SYSTEM') {
     print "User with sysadmin role is required for this script to run. Please check config file entry for $engine\n";
     next;
   }

   my $filename = '/tmp/dxtestsys.json';

   open (my $FD, '<', "$filename") or die ("Can't open file $filename : $!");

   local $/ = undef;
   my $json = JSON->new();
   my $config = $json->decode(<$FD>);
 
   close $FD;

   print Dumper $config;


   my $system = new System_obj ($engine_obj, $debug);
   my $storage = new Storage_obj ($engine_obj, $debug);


  # initialize engine

  #  print("Initialize engine\n");

  #  if ($system->configEngine($engine_obj, $engine, $storage, "marcin\@delphix.com", "slon")) {
  #   print Dumper "engine config failed";
  #   $ret = $ret + 1;
  #   next;
  #  } else {
  #   print("Engine initialization completed\n");
  #  }

#    if ((defined($config->{"type"})) && (defined($config->{"type"}->{"engine"}))) {
#     if (!((uc $config->{"type"}->{"engine"} eq 'VIRTUALIZATION') || (uc $config->{"type"}->{"engine"} eq 'MASKING'))) {
#       print("Engine type is wrong: " . $config->{"type"}->{"engine"});
#       $ret = $ret + 1;
#     } 
#    } else {
#     print("Engine type not found in config file\n");
#     $ret = $ret + 1;
#     next;
#    }

#    print("Setting engine type\n");

#    my $type_action = $system->setEngineType($config->{"type"}->{"engine"});
#    if (defined($type_action)) {
#     $ret = $ret + Toolkit_helpers::waitForAction($engine_obj, $type_action,'OK','Error with setting type action');
#    } else {
#     print("Can't set type - is it already set");
#    }


# # - servers - array of comma separeted
# # - source 
# # - domain

#    my $dns_servers;
#    my $dns_source;
#    my $dns_domain;

#    if (! defined($config->{"dns"})) {
#     print Dumper "dns is required";
#     $ret = $ret + 1;
#     next;
#    } else {
#     print("Setting DNS\n");
#     if (defined($config->{"dns"}->{"source"})) {
#       $dns_source = uc $config->{"dns"}->{"source"};
#       if ($dns_source eq 'STATIC') {
#         if (!defined($config->{"dns"}->{"dns_domain"})) {
#           print Dumper "dns domain is required";
#           exit;
#         } else {
#           $dns_domain = $config->{"dns"}->{"dns_domain"};
#         }

#         if (!defined($config->{"dns"}->{"dns_server"})) {
#           print Dumper "dns domain is required";
#           exit;
#         } else {
#           $dns_servers = $config->{"dns"}->{"dns_server"};
#         }

#       } elsif ($dns_source eq 'DHCP') {
#         if (!defined($config->{"dns"}->{"dns_domain"})) {
#           print Dumper "dns domain is required";
#           exit;
#         } else {
#           $dns_domain = $config->{"dns"}->{"dns_domain"};
#         }
#       } else {
#         print Dumper "dns source can be STATIC or DHCP";
#         exit;
#       }
#     } else {
#       print Dumper "dns source is required";
#       exit;
#     }
#    }

#    $system->setDNSServers($dns_servers, $dns_source, $dns_domain);

  #  if (defined($config->{"syslog"})) {
  #   my $syslog_servers;
  #   my $syslog_status = 'Disabled';
  #   my $syslog_severity;
  #   print("Setting syslog\n");
  #   if (defined($config->{"syslog"}->{"status"})) {
  #     $syslog_status = $config->{"syslog"}->{"status"};
  #   }
  #   if (defined($config->{"syslog"}->{"servers"})) {
  #     $syslog_servers = $config->{"syslog"}->{"servers"};
  #   }
  #   if (defined($config->{"syslog"}->{"severity"})) {
  #     $syslog_severity = $config->{"syslog"}->{"severity"};
  #   }
  #   $system->setSyslog($syslog_servers, $syslog_status, $syslog_severity);
  #  }

   if (defined($config->{"ldap"})) {
    my $ldap_server;
    my $ldap_status = 'Disabled';
    print("Setting LDAP\n");
    if (defined($config->{"ldap"}->{"status"})) {
      $ldap_status = $config->{"ldap"}->{"status"};
    }

    if (uc $ldap_status eq 'ENABLED') {
      if (defined($config->{"ldap"}->{"server"})) {
        $ldap_server = $config->{"ldap"}->{"server"};
      } else {
        print("LDAP server definition is missing\n");
        $ret = $ret + 1;
        next;
      }

      $system->setLDAP($ldap_server, $ldap_status);
    }
   }

  #  if (defined($config->{"time"})) {
  #   my $ntp_servers;
  #   my $ntp_status = 'Disabled';
  #   my $timezone;
  #   print("Setting timezone and NTP\n");
  #   if (defined($config->{"time"}->{"ntp_status"})) {
  #     $ntp_status = $config->{"time"}->{"ntp_status"};
  #   }
  #   if (defined($config->{"time"}->{"ntp_server"})) {
  #     $ntp_servers = $config->{"time"}->{"ntp_server"};
  #   }
  #   if (defined($config->{"time"}->{"timezone"})) {
  #     $timezone = $config->{"time"}->{"timezone"};
  #   }

  #   $system->setNTP($ntp_servers, $ntp_status, $timezone);
  #   $system->wait_for_restart($engine_obj, $engine);
  #  }

}


exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_get_config [-engine|d <delphix identifier> | -all ]
               [-format csv|json]
               [-help|? ]
               [-debug ]

=head1 DESCRIPTION

Display Delphix Engine configuration

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.
Warning - this scripts require a sysadmin user to run

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=item B<-configfile file>
Location of the configuration file.
A config file search order is as follow:
- configfile parameter
- DXTOOLKIT_CONF variable
- dxtools.conf from dxtoolkit location

=back

=head1 OPTIONS

=over 3

=item B<-format>
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Display a Delphix Engine configuration

  dx_get_config -d Landshark5sys

  engine name                         parameter name                 value
  ----------------------------------- ------------------------------ ------------------------------
  Landshark5sys                       DNS server                     172.16.180.2
  Landshark5sys                       DNS Domain                     localdomain
  Landshark5sys                       SNMP Status                    Disabled
  Landshark5sys                       SNMP Servers
  Landshark5sys                       SNMP Severity                  WARNING
  Landshark5sys                       NTP Servers                    Europe.pool.ntp.org
  Landshark5sys                       NTP Status                     Enabled
  Landshark5sys                       SMTP Server                    N/A
  Landshark5sys                       SMTP Status                    Disabled
  Landshark5sys                       Syslog Status                  Disabled
  Landshark5sys                       Syslog Servers
  Landshark5sys                       Syslog severity                WARNING
  Landshark5sys                       LDAP status                    Enabled
  Landshark5sys                       LDAP server 1 name             1.2.3.4
  Landshark5sys                       LDAP server 1 port             389
  Landshark5sys                       LDAP server 1 use SSL          false
  Landshark5sys                       LDAP server 1 authentication   SIMPLE


=cut
