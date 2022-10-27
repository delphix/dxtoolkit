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
  'backup=s' => \(my $backup),
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


if (defined($backup)) {
  if (! -d $backup) {
    print "Path $backup is not a directory \n";
    exit (1);
  }
  if (! -w $backup) {
    print "Path $backup is not writtable \n";
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

   my %config = (
    "engine" => {},
    "dns" => {},
    "storage" => {},
    "snmp" => {},
    "time" => {},
    "smpt" => {},
    "syslog" => {},
    "ldap" => {},
    "storage" => {},
    "ssl" => {}
   );

   my $system = new System_obj ($engine_obj, $debug);
   my $storage = new Storage_obj ($engine_obj, $debug);
   $storage->LoadStorageDevices();

   $config{"engine"}{"type"} = $system->getEngineType();
   $config{"engine"}{"password"} = "adminpass_changeme";
   $config{"engine"}{"email"} = "admin\@delphix.com";

   $config{"dns"}{"dns_server"} = join(',' ,@{$system->getDNSServers()}) ;
   $config{"dns"}{"dns_domain"} = join(',' ,@{$system->getDNSDomains()}) ;
   $config{"dns"}{"source"} = $system->getDNSSource() ;   
   $config{"snmp"}{"status"} = $system->getSNMPStatus() ;  
   $config{"snmp"}{"snmp_servers"} = join(',', @{$system->getSNMPServers() });
   $config{"snmp"}{"snmp_severity"} = $system->getSNMPSeverity();
   $config{"time"}{"ntp_server"} = join(',', @{$system->getNTPServer() }); 
   $config{"time"}{"ntp_status"} = $system->getNTPStatus(); 
   $config{"time"}{"timezone"} = $engine_obj->getTimezone();
   my $smtpserver = $system->getSMTPServer();

   if ($smtpserver ne 'N/A') {
    $config{"smtp"}{"server"} = $smtpserver;
    $config{"smtp"}{"status"} = $system->getSMTPStatus();
   }

   $config{"syslog"}{"status"} = $system->getSyslogStatus();
   $config{"syslog"}{"servers"} = $system->getSyslogServers();
   $config{"syslog"}{"severity"} = $system->getSyslogSeverity();

   $config{"ldap"}{"status"} = $system->getLDAPStatus();

   if ($config{"ldap"}{"status"} eq 'Enabled') {

    my $ser = $system->getLDAPServers();
    my %ldap_entry = (
      "server" => $ser->{host},
      "port"   => $ser->{port},
      "ssl"    => $ser->{useSSL},
      "authentication" => $ser->{authMethod}
    );

    $config{"ldap"}{"server"} = \%ldap_entry;

   }

   $config{"storage"} = $storage->getDisks(0);

   $config{"sso"}{"status"} = $system->getSSOStatus();
   if ($config{"sso"}{"status"} eq 'Enabled') {
    $config{"sso"}{"entityId"} = $system->getSSOEntityId();
    $config{"sso"}{"samlMetadata"} = $system->getSSOsamlMetadata();
    $config{"sso"}{"maxAuthenticationAge"} = $system->getSSOmaxAuthenticationAge();
    $config{"sso"}{"responseSkewTime"} = $system->getSSOresponseSkewTime();
   }



   if (defined($backup)) {
    my $filename = File::Spec->catfile($backup,$engine_obj->getEngineName() . '.json'); 
    open (my $FD, '>', "$filename") or die ("Can't open file $filename : $!");
    print "Exporting configuration into file $filename \n";
    print $FD to_json(\%config, {pretty => 1});
    close $FD;
   } else {
    for my $confclass (sort(keys(%config))) {
      if ($confclass eq 'storage') {
        next;
      }
      for my $par (sort(keys(%{$config{$confclass}}))) {
        $output->addLine(
          $engine,
          $confclass . '_' . $par,
          $config{$confclass}{$par}
      );
      }
    }
   }

}

if (!defined($backup)) {
  Toolkit_helpers::print_output($output, $format, $nohead);
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
  dxtestsys                           dns_dns_domain                 delphix.com
  dxtestsys                           dns_dns_server                 172.16.105.2,172.16.101.11
  dxtestsys                           ldap_status                    Disabled
  dxtestsys                           snmp_snmp_servers
  dxtestsys                           snmp_snmp_severity             WARNING
  dxtestsys                           snmp_status                    Disabled
  dxtestsys                           syslog_servers
  dxtestsys                           syslog_severity                WARNING
  dxtestsys                           syslog_status                  Disabled
  dxtestsys                           time_ntp_server
  dxtestsys                           time_ntp_status                Disabled
  dxtestsys                           time_timezone                  US/Pacific


=cut
