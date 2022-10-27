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

# Program Name : dx_ctl_config.pl
# Description  : Configure engine
# Author       : Marcin Przepiorowski
# Created      : Oct 2022 (v2.4.17)
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
  'filename=s' => \(my $filename),
  'email=s' => \(my $email),
  'password=s' => \(my $password),
  'initializeonly' => \(my $initialize),
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

if (defined($filename) && defined($initialize)) {
   print "Option filename and initializeonly are mutually exclusive \n";
   pod2usage(-verbose => 1,  -input=>\*DATA);
   exit (1);
}


if (!defined($initialize)) {
  if (defined($filename)) {
    if (! -f $filename) {
      print "File $filename is not accessiable \n";
      exit (1);
    }
  } else {
    print "Filename option is mandatory\n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }
}


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

   my $system = new System_obj ($engine_obj, $debug);
   my $storage = new Storage_obj ($engine_obj, $debug);

  # initialize engine

   print("Initialize engine\n");

   my $config;
   if (defined($filename)) {
    open (my $FD, '<', "$filename") or die ("Can't open file $filename : $!");

    local $/ = undef;
    my $json = JSON->new();
    $config = $json->decode(<$FD>);
  
    close $FD;

    if ((defined($config->{"engine"})) && (defined($config->{"engine"}->{"email"}))) {
      $email = $config->{"engine"}->{"email"};
    } else {
      print("Missing engine->email entry in config file");
      $ret = $ret + 1;
      next;
    }

    if ((defined($config->{"engine"})) && (defined($config->{"engine"}->{"password"}))) {
      $password = $config->{"engine"}->{"password"};
    } else {
      print("Missing engine->password entry in config file");
      $ret = $ret + 1;
      next;
    }

   } else {
    if (!defined($email)) {
      print("if no configuration file is used email needs to be provided as parameter\n");
      $ret = $ret + 1;
      next;
    }
    if (!defined($password)) {
      print("if no configuration file is used password needs to be provided as parameter\n");
      $ret = $ret + 1;
      next;
    }

   }

   if ($system->configEngine($engine_obj, $engine, $storage, $email, $password)) {
    print("engine config failed\n");
    $ret = $ret + 1;
    next;
   } else {
    print("Engine initialization completed\n");
   }

   if (!defined($initialize)) {

    if ((defined($config->{"engine"})) && (defined($config->{"engine"}->{"type"}))) {
      if (!((uc $config->{"engine"}->{"type"} eq 'VIRTUALIZATION') || (uc $config->{"engine"}->{"type"} eq 'MASKING'))) {
        print("Engine type is wrong: " . $config->{"engine"}->{"type"});
        $ret = $ret + 1;
      } 
    } else {
      print("Engine type not found in config file\n");
      $ret = $ret + 1;
      next;
    }

    print("Setting engine type\n");

    my $type_action = $system->setEngineType($config->{"engine"}->{"type"});
    if (defined($type_action)) {
      $ret = $ret + Toolkit_helpers::waitForAction($engine_obj, $type_action,'OK','Error with setting type action');
    } else {
      print("Can't set type - is it already set");
    }

    if (defined($config->{"dns"})) {
      my $dns_servers;
      my $dns_source;
      my $dns_domain;
      print("Setting DNS\n");
      if (defined($config->{"dns"}->{"source"})) {
        $dns_source = uc $config->{"dns"}->{"source"};
        if ($dns_source eq 'STATIC') {
          if (!defined($config->{"dns"}->{"dns_domain"})) {
            print "dns domain is required";
            $ret = $ret + 1;
            next;
          } else {
            $dns_domain = $config->{"dns"}->{"dns_domain"};
          }

          if (!defined($config->{"dns"}->{"dns_server"})) {
            print "dns domain is required";
            $ret = $ret + 1;
            next;
          } else {
            $dns_servers = $config->{"dns"}->{"dns_server"};
          }

        } elsif ($dns_source eq 'DHCP') {
          if (!defined($config->{"dns"}->{"dns_domain"})) {
            print "dns domain is required";
            $ret = $ret + 1;
            next;
          } else {
            $dns_domain = $config->{"dns"}->{"dns_domain"};
          }
        } else {
          print "dns source can be STATIC or DHCP";
          $ret = $ret + 1;
          next;
        }
      } else {
        print "dns source is required\n";
        $ret = $ret + 1;
        next;
      }
      $system->setDNSServers($dns_servers, $dns_source, $dns_domain);
    }



    if (defined($config->{"syslog"})) {
      my $syslog_servers;
      my $syslog_status = 'Disabled';
      my $syslog_severity;
      print("Setting syslog\n");
      if (defined($config->{"syslog"}->{"status"})) {
        $syslog_status = $config->{"syslog"}->{"status"};
      }
      if (defined($config->{"syslog"}->{"servers"})) {
        $syslog_servers = $config->{"syslog"}->{"servers"};
      }
      if (defined($config->{"syslog"}->{"severity"})) {
        $syslog_severity = $config->{"syslog"}->{"severity"};
      }
      $system->setSyslog($syslog_servers, $syslog_status, $syslog_severity);
    }

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

    if (defined($config->{"sso"})) {
      my $sso_status = 'Disabled';
      print("Setting SSO\n");
      if (defined($config->{"sso"}->{"status"})) {
        $sso_status = $config->{"sso"}->{"status"};
      }

      my $entityId;
      my $samlMetadata;
      my $maxAuthenticationAge;
      my $responseSkewTime;

      if (defined($config->{"sso"}->{"entityId"})) {
        $entityId = $config->{"sso"}->{"entityId"};
      } else {
        print("entityID is required to set SSO");
        $ret = $ret + 1;
        next;
      }

      if (defined($config->{"sso"}->{"samlMetadata"})) {
        $samlMetadata = $config->{"sso"}->{"samlMetadata"};
      } else {
        print("samlMetadata is required to set SSO");
        $ret = $ret + 1;
        next;
      }

      if (defined($config->{"sso"}->{"maxAuthenticationAge"})) {
        $maxAuthenticationAge = $config->{"sso"}->{"maxAuthenticationAge"};
      } 

      if (defined($config->{"sso"}->{"responseSkewTime"})) {
        $responseSkewTime = $config->{"sso"}->{"responseSkewTime"};
      } 

      if (uc $sso_status eq 'ENABLED') {
        $system->setSSO($entityId, $samlMetadata, $responseSkewTime, $maxAuthenticationAge);
      }

    }

    if (defined($config->{"time"})) {
      my $ntp_servers;
      my $ntp_status = 'Disabled';
      my $timezone;
      print("Setting timezone and NTP\n");
      if (defined($config->{"time"}->{"ntp_status"})) {
        $ntp_status = $config->{"time"}->{"ntp_status"};
      }
      if (defined($config->{"time"}->{"ntp_server"})) {
        $ntp_servers = $config->{"time"}->{"ntp_server"};
      }
      if (defined($config->{"time"}->{"timezone"})) {
        $timezone = $config->{"time"}->{"timezone"};
      }

      $system->setNTP($ntp_servers, $ntp_status, $timezone);
      $system->wait_for_restart($engine_obj, $engine);
    }
   }
   if ($ret eq 0) {
    print "Engine $engine configured without problems\n";
   } else {
    print "Engine $engine configuration issues\n";
   }

}


exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_ctl_config [-engine|d <delphix identifier> | -all ]
                -filename name | -initializeonly 
               [-email email ]
               [-password password ]
               [-help|? ]
               [-debug ]

=head1 DESCRIPTION

Configure or initialize Delphix engine. Configuration file is JSON based and can be created by dx_get_config 
with backup option

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.
Warning - this scripts require a sysadmin user to run

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Run on all Delphix Engines configured in dxtoos.conf

=item B<-configfile file>
Location of the configuration file.
A config file search order is as follow:
- configfile parameter
- DXTOOLKIT_CONF variable
- dxtools.conf from dxtoolkit location

=item B<-filename name>
Initialize engine and apply configuration file

=item B<-initializeonly>
Initialize engine only

=back

=head1 OPTIONS

=over 3

=item B<-email email>
For initialize only - set admin user email address

=item B<-password password>
For initialize only - set admin user password

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Initialize a new engine and apply configuration with config file.

  dx_ctl_config -d dxtestsys1  -filename /tmp/dxtestsys.json
  Initialize engine
  Waiting for all actions to complete. Parent action is ACTION-2
  Engine initialized
  wait for restart
  Engine initialization completed
  Setting engine type
  Waiting for all actions to complete. Parent action is ACTION-4
  OK
  Setting DNS
  Setting syslog
  Setting LDAP
  Setting SSO
  Setting timezone and NTP
  wait for restart
  Engine dxtestsys1 configured without problems


Initialize with default settings

  dx_ctl_config -d dxtestsys1 -initializeonly -email test@delphix.com -password delphix
  Initialize engine
  Waiting for all actions to complete. Parent action is ACTION-2
  Engine initialized
  wait for restart
  Engine initialization completed
  Engine dxtestsys1 configured without problems

=cut
