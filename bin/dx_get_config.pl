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
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj, 'sysadmin');

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
   # main loop for all work
   if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
   };
   
   if (lc $engine_obj->getUsername() ne 'sysadmin') {
     print "User sysadmin is required for this script to run. Please check dxtools.conf entry for $engine\n";
     next;
   }
  
   my $system = new System_obj ($engine_obj, $debug);
   
   $output->addLine(
    $engine,
    'DNS server',
    join(',' ,@{$system->getDNSServers()} )
   );

   $output->addLine(
    $engine,
    'DNS Domain',
    join(',', @{$system->getDNSDomains()} )
   );

   $output->addLine(
    $engine,
    'SNMP Status',
    $system->getSNMPStatus()
   );

   $output->addLine(
    $engine,
    'SNMP Servers',
    join(',', @{$system->getSNMPServers() })
   );

   $output->addLine(
    $engine,
    'SNMP Severity',
    $system->getSNMPSeverity() 
   ); 
   
   $output->addLine(
    $engine,
    'NTP Servers',
    join(',', @{$system->getNTPServer() })
   );

   $output->addLine(
    $engine,
    'NTP Status',
    $system->getNTPStatus() 
   ); 

   my $smtpserver = $system->getSMTPServer() ? $system->getSMTPServer() : "N/A";

   $output->addLine(
    $engine,
    'SMTP Server',
    $smtpserver
   );

   $output->addLine(
    $engine,
    'SMTP Status',
    $system->getSMTPStatus() 
   ); 

   $output->addLine(
    $engine,
    'Syslog Status',
    $system->getSyslogStatus() 
   );
   
   $output->addLine(
    $engine,
    'Syslog Servers',
    join(',', @{$system->getSyslogServers() })
   );

   $output->addLine(
    $engine,
    'Syslog severity',
    $system->getSyslogSeverity() 
   );

   $output->addLine(
    $engine,
    'LDAP status',
    $system->getLDAPStatus() 
   );
   
   my $n = 1;
   
   for my $ser (@{$system->getLDAPServers()}) {
     my $servername='LDAP server ' . $n . ' ';
     $output->addLine(
      $engine,
      $servername .'name',
      $ser->{address}
     );
     $output->addLine(
      $engine,
      $servername . 'port',
      $ser->{port}
     );
     $output->addLine(
      $engine,
      $servername . 'use SSL',
      $ser->{useSSL} ? 'true' : 'false'
     );
     $output->addLine(
      $engine,
      $servername . 'authentication',
      $ser->{authMethod}
     );
     $n++;
   }
   

}


Toolkit_helpers::print_output($output, $format, $nohead);


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
