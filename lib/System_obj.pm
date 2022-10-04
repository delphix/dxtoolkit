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
# Program Name : System_obj.pm
# Description  : Delphix Engine Capacity object
# It's include the following classes:
# - System_obj - class which map a Delphix Engine system API object
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#



package System_obj;

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
    logger($debug, "Entering System_obj::constructor",1);

    my %system;
    my $self = {
        _system => \%system,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };

    bless($self,$classname);

    $self->LoadSystem();
    return $self;
}


# Procedure getSSHPublicKey
# parameters: none
# Return an Engine SSH key

sub getSSHPublicKey
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSSHPublicKey",1);
    return $self->{_system}->{sshPublicKey};
}

# Procedure getStorage
# parameters: none
# Return an Engine storage hash (Used, Free, Total, pct used) GB

sub getStorage
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getStorage",1);
    my %stor;

    if (version->parse($self->{_dlpxObject}->getApi()) <= version->parse(1.11.6)) {
      %stor = (
          Total => sprintf("%2.2f",$self->{_system}->{storageTotal}/1024/1024/1024),
          Used => sprintf("%2.2f",$self->{_system}->{storageUsed}/1024/1024/1024),
          Free => sprintf("%2.2f",($self->{_system}->{storageTotal} - $self->{_system}->{storageUsed})/1024/1024/1024),
          pctused => sprintf("%2.2f",$self->{_system}->{storageUsed} / $self->{_system}->{storageTotal} * 100)
      );

    } else {
      # now Delphix is adding reserved space to used
      my $reserved = $self->{_system}->{storageTotal} * 0.1;

      if ($reserved>1024 * 1024 * 1024 * 1024) {
        # max reserverd is 1 TB
        $reserved = 1024 * 1024 * 1024 * 1024;
      }

      my $used = $self->{_system}->{storageUsed} + $reserved;

      %stor = (
          Total => sprintf("%2.2f",$self->{_system}->{storageTotal}/1024/1024/1024),
          Used => sprintf("%2.2f", $used/1024/1024/1024),
          Free => sprintf("%2.2f",($self->{_system}->{storageTotal} - $used)/1024/1024/1024),
          pctused => sprintf("%2.2f",$used / $self->{_system}->{storageTotal} * 100)
      );

    }

    return \%stor;
}

# Procedure getVersion
# parameters: none
# Return an Engine version

sub getVersion
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getVersion",1);
    my $verhash = $self->{_system}->{buildVersion};
    return $verhash->{major} . '.' . $verhash->{minor} . '.' . $verhash->{micro} . '.' . $verhash->{patch};
}

# Procedure getUUID
# parameters: none
# return UUID of Delphix engine

sub getUUID {
   my $self = shift;
   logger($self->{_debug}, "Entering System_obj::getUUID",1);
   my $uuid = $self->{_system}->{uuid};

   return $uuid;

}

# Procedure getvCPU
# parameters: none
# return number of vCPU allocated to Delphix engine

sub getvCPU {
   my $self = shift;
   logger($self->{_debug}, "Entering System_obj::getvCPU",1);
   my $vCPU = $self->{_system}->{processors};
   return scalar(@{$vCPU});
}

# Procedure getvMem
# parameters: none
# return number of vMem allocated to Delphix engine

sub getvMem {
   my $self = shift;
   logger($self->{_debug}, "Entering System_obj::getvMem",1);
   my $vMem = $self->{_system}->{memorySize}/1024/1024/1024;
   return $vMem;
}

# Procedure LoadSystem
# parameters: none
# Load a list of System objects from Delphix Engine

sub LoadSystem
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::LoadSystem",1);
    my $operation = "resources/json/delphix/system";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
      $self->{_system} = $result->{result};
    } else {
      print "No data returned for $operation. Try to increase timeout \n";
    }
}

# Procedure getDNS
# parameters: none
# Load a DNS settings of Delphix Engine

sub getDNS
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getDNS",1);

    if (!defined($self->{_dns})) {

      my $operation = "resources/json/delphix/service/dns";
      my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

      if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        $self->{_dns} = $result->{result};
      } else {
        print "No data returned for $operation. Try to increase timeout \n";
      }

    }

    return $self->{_dns};

}

# Procedure getDNSServers
# parameters: none
# Load a DNS servers setup in Delphix Engine

sub getDNSServers
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getDNSServers",1);
    my $servers = $self->getDNS()->{servers};
    return $servers;
}

# Procedure getDNSDomains
# parameters: none
# Load a DNS domains setup in Delphix Engine

sub getDNSDomains
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getDNSDomains",1);
    my $domain = $self->getDNS()->{domain};
    return $domain;
}

# Procedure getDNSSource
# parameters: none
# return source of dns

sub getDNSSource
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getDNSSource",1);
    my $domain;
    if (defined($self->getDNS()->{source})) {
      $domain = $self->getDNS()->{source};
    } else {
      $domain = 'STATIC';
    }
    return $domain;
}

# Procedure setDNSServers
# parameters: 
# - servers - comma separeted
# - source 
# - domain
# Set a DNS servers

sub setDNSServers
{
    my $self = shift;
    my $servers = shift;
    my $source = shift;
    my $domain = shift;
    logger($self->{_debug}, "Entering System_obj::setDNSServers",1);


    my @dns_servers;
    
    my @dns_domains;

    my %dns_hash = (
      "type" => "DNSConfig",
      "source" => $source
    );

    if (defined($servers)) {
     @dns_servers = split(',', $servers);
     $dns_hash{"servers"} = \@dns_servers;
    }

    if (defined($domain)) {
     @dns_domains = split(',', $domain);
     $dns_hash{"domain"} = \@dns_domains;
    }

    my $json = to_json(\%dns_hash);
    my $operation = 'resources/json/delphix/service/dns';

    my ($result,$result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json);
    my $ret;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $ret = 0;
    } else {
        if (defined($result->{error})) {
            print "Problem with starting action\n";
            print "Error: " . Toolkit_helpers::extractErrorFromHash($result->{error}->{details}) . "\n";
            logger($self->{_debug}, "Can't submit job action operation $operation",1);
            logger($self->{_debug}, "error " . Dumper $result->{error}->{details},1);
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
        $ret = 1;
    }

    return $ret;



}

# Procedure getSNMP
# parameters: none
# Load a SNMP settings of Delphix Engine

sub getSNMP
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSNMP",1);

    if (!defined($self->{_snmp})) {

      my $operation = "resources/json/delphix/service/snmp";
      my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

      if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        $self->{_snmp} = $result->{result};
      } else {
        print "No data returned for $operation. Try to increase timeout \n";
      }

    }

    return $self->{_snmp};

}


# Procedure getSNMPStatus
# parameters: none
# Return a SNMP status in Delphix Engine

sub getSNMPStatus
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSNMPStatus",1);
    my $status = $self->getSNMP()->{enabled} ? "Enabled" : "Disabled";
    return $status;
}

# Procedure getSNMPSeverity
# parameters: none
# Return a SNMP severity in Delphix Engine

sub getSNMPSeverity
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSNMPSeverity",1);
    my $severity = $self->getSNMP()->{severity};
    return $severity;
}

# Procedure getSNMPManager
# parameters: none
# Load a SNMP servers settings of Delphix Engine

sub getSNMPManager
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSNMPManager",1);

    if (!defined($self->{_snmpmanager})) {

      my $operation = "resources/json/delphix/service/snmp/manager";
      my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

      if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        $self->{_snmpmanager} = $result->{result};
      } else {
        print "No data returned for $operation. Try to increase timeout \n";
      }

    }

    return $self->{_snmpmanager};
}

# Procedure getSNMPStatus
# parameters: none
# Return a SNMP status in Delphix Engine

sub getSNMPServers
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSNMPServers",1);
    my $servers = $self->getSNMPManager();
    my @retarray;
    for my $seritem (@{$servers}) {
      my %serhash;
      $serhash{address} = $seritem->{address};
      $serhash{communityString} = $seritem->{communityString};
      push(@retarray, \%serhash);
    }

    return \@retarray;
}

# Procedure getNTP
# parameters: none
# Load a NTP settings of Delphix Engine

sub getNTP
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getNTP",1);

    if (!defined($self->{_time})) {

      my $operation = "resources/json/delphix/service/time";
      my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

      if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        $self->{_time} = $result->{result};
      } else {
        print "No data returned for $operation. Try to increase timeout \n";
      }

    }

    return $self->{_time};

}

# Procedure getNTPServer
# parameters: none
# Return a NTP server in Delphix Engine

sub getNTPServer
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getNTPServer",1);
    my $servers = $self->getNTP()->{ntpConfig}->{servers};
    return $servers;
}

# Procedure getNTPStatus
# parameters: none
# Return a NTP status in Delphix Engine

sub getNTPStatus
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getNTPStatus",1);
    my $servers = $self->getNTP()->{ntpConfig}->{enabled} ? "Enabled" : "Disabled";
    return $servers;
}


# Procedure setNTP
# parameters: 
# - servers - comma separeted
# - timezone 
# Set a NTP servers

sub setNTP
{
    my $self = shift;
    my $servers = shift;
    my $status = shift;
    my $timezone = shift;
    logger($self->{_debug}, "Entering System_obj::setNTP",1);

    my $ntpstatus;

    if (uc $status eq 'DISABLED') {
      $ntpstatus = JSON::false;
    } elsif (uc $status eq 'ENABLED') {
      $ntpstatus = JSON::true;
    } else {
      return 1;
    }

    my @ntp_servers;
    my %ntp_hash = (
      "type" => "TimeConfig",
      "ntpConfig" => {
        "type" => "NTPConfig",
        "enabled" => $ntpstatus
      },
      "systemTimeZone" => $timezone
    );

    if (defined($servers)) {
     @ntp_servers = split(',', $servers);
     $ntp_hash{"ntpConfig"}{"servers"} = \@ntp_servers;
    }

    my $json = to_json(\%ntp_hash);
    # it's is causing a restart of management stack
    my $operation = 'resources/json/delphix/service/time';

    my ($result,$result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json);
    my $ret;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $ret = 0;
    } else {
        if (defined($result->{error})) {
            print "Problem with starting action\n";
            print "Error: " . Toolkit_helpers::extractErrorFromHash($result->{error}->{details}) . "\n";
            logger($self->{_debug}, "Can't submit job action operation $operation",1);
            logger($self->{_debug}, "error " . Dumper $result->{error}->{details},1);
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
        $ret = 1;
    }

    return $ret;
}


# Procedure getSMTP
# parameters: none
# Load a SMTP settings of Delphix Engine

sub getSMTP
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSMTP",1);

    if (!defined($self->{_smtp})) {

      my $operation = "resources/json/delphix/service/smtp";
      my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

      if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        $self->{_smtp} = $result->{result};
      } else {
        print "No data returned for $operation. Try to increase timeout \n";
      }

    }

    return $self->{_smtp};

}

# Procedure getSMTPServer
# parameters: none
# Return a SMTP server in Delphix Engine

sub getSMTPServer
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSMTPServer",1);
    my $servers = $self->getSMTP()->{server};
    if (defined($servers)) {
      return $servers
    } else {
      return "N/A";
    }
}

# Procedure getSMTPStatus
# parameters: none
# Return a SMTP status in Delphix Engine

sub getSMTPStatus
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSMTPStatus",1);
    my $status = $self->getSMTP()->{enabled} ? "Enabled" : "Disabled";
    return $status;
}

# Procedure getSyslog
# parameters: none
# Load a Syslog settings of Delphix Engine

sub getSyslog
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSyslog",1);

    if (!defined($self->{_syslog})) {

      my $operation = "resources/json/delphix/service/syslog";
      my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

      if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        $self->{_syslog} = $result->{result};
      } else {
        print "No data returned for $operation. Try to increase timeout \n";
      }

    }

    return $self->{_syslog};

}


# Procedure setSyslog
# parameters: 
# - servers - comma separeted
# - timezone 
# Set a NTP servers

sub setSyslog
{

    my $self = shift;
    my $servers = shift;
    my $status = shift;
    my $severity = shift;
    logger($self->{_debug}, "Entering System_obj::setSyslog",1);

    my $syslogstatus;

    if (uc $status eq 'DISABLED') {
      $syslogstatus = JSON::false;
    } elsif (uc $status eq 'ENABLED') {
      $syslogstatus = JSON::true;
    } else {
      return 1;
    }

    my @syslog_servers;
    my %syslog_hash = (
      "type" => "SyslogConfig",
      "enabled" => $syslogstatus,
      "severity" => $severity
    );

    if (defined($servers)) {
     for my $ser (split(',', $servers)) {
      my @serentry = split(':', $ser);
      if (scalar(@serentry) ne 3) {
        print("Config entry for syslog server is wrong: ". $ser);
        return 1;
      }
      my %serhash = (
        "type" => "SyslogServer",
        "protocol" => $serentry[2],
        "port" => $serentry[1] + 0,
        "address" => $serentry[0]
      );
      push(@syslog_servers, \%serhash);
     }
     $syslog_hash{"servers"} = \@syslog_servers;
    }

    my $json = to_json(\%syslog_hash);
    # it's is causing a restart of management stack
    my $operation = 'resources/json/delphix/service/syslog';

    my ($result,$result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json);
    my $ret;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $ret = 0;
    } else {
        if (defined($result->{error})) {
            print "Problem with starting action\n";
            print "Error: " . Toolkit_helpers::extractErrorFromHash($result->{error}->{details}) . "\n";
            logger($self->{_debug}, "Can't submit job action operation $operation",1);
            logger($self->{_debug}, "error " . Dumper $result->{error}->{details},1);
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
        $ret = 1;
    }

    return $ret;
}



# Procedure getSyslogServers
# parameters: none
# Return a Syslog servers in Delphix Engine in the list of the following format
# server:port:protocol[,server:port:protocol]

sub getSyslogServers
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSyslogServers",1);
    my $servers = $self->getSyslog()->{servers};
    return join(',', map { "$_->{address}:$_->{port}:$_->{protocol}"  } @{$servers});
}

# Procedure getSyslogStatus
# parameters: none
# Return a Syslog status in Delphix Engine

sub getSyslogStatus
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSyslogStatus",1);
    my $status = $self->getSyslog()->{enabled} ? "Enabled" : "Disabled";
    return $status;
}

# Procedure getSyslogSeverity
# parameters: none
# Return a Syslog severity in Delphix Engine

sub getSyslogSeverity
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSyslogSeverity",1);
    my $severity = $self->getSyslog()->{severity};
    return $severity;
}

# Procedure getLDAP
# parameters: none
# Load a LDAP settings of Delphix Engine

sub getLDAP
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getLDAP",1);

    if (!defined($self->{_ldap})) {

      my $operation = "resources/json/delphix/service/ldap";
      my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

      if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        $self->{_ldap} = $result->{result};
      } else {
        print "No data returned for $operation. Try to increase timeout \n";
      }

    }

    return $self->{_ldap};

}

# Procedure setLDAP
# parameters: 
# - status
# - servers
# Set LDAP 

sub setLDAP
{
    my $self = shift;
    my $server = shift;
    my $status = shift;

    logger($self->{_debug}, "Entering System_obj::setLDAP",1);

    my $ldapstatus;

    if (uc $status eq 'DISABLED') {
      $ldapstatus = JSON::false;
    } elsif (uc $status eq 'ENABLED') {
      $ldapstatus = JSON::true;
    } else {
      return 1;
    }

    my $usessl;
    if ($server->{ssl}) {
      $usessl = JSON::true;
    } else {
      $usessl = JSON::false;
    } 

    my %ldap_hash = (
      "type" => "LdapServer",
      "host" => $server->{server},
      "port" => $server->{port},
      "authMethod" => $server->{authentication},
      "useSSL" => $usessl
    );



    my $json = to_json(\%ldap_hash);
    # it's is causing a restart of management stack
    my $operation = 'resources/json/delphix/service/ldap/server';

    my ($result,$result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json);
    my $ret;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $ret = 0;
    } else {
        if (defined($result->{error})) {
            print "Problem with starting action\n";
            print "Error: " . Toolkit_helpers::extractErrorFromHash($result->{error}->{details}) . "\n";
            logger($self->{_debug}, "Can't submit job action operation $operation",1);
            logger($self->{_debug}, "error " . Dumper $result->{error}->{details},1);
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
        $ret = 1;
    }

    return $ret;

}

# Procedure getLDAPStatus
# parameters: none
# Return a LDAP status in Delphix Engine

sub getLDAPStatus
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getLDAPStatus",1);
    my $status = $self->getLDAP()->{enabled} ? "Enabled" : "Disabled";
    return $status;
}

# Procedure getLDAPServerConf
# parameters: none
# Load a LDAP config in Delphix Engine

sub getLDAPServerConf
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getLDAPServerConf",1);

    if (!defined($self->{_ldapserver})) {

      my $operation = "resources/json/delphix/service/ldap/server";
      my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

      if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        $self->{_ldapserver} = $result->{result};
      } else {
        print "No data returned for $operation. Try to increase timeout \n";
      }

    }

    return $self->{_ldapserver};

}

# Procedure getLDAPServers
# parameters: none
# Return a LDAP servers in Delphix Engine

sub getLDAPServers
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getLDAPServers",1);

    my $servers = $self->getLDAPServerConf();
    # there is only one server - no point to return an array
    return $servers->[0];
}

# Procedure getEngineType
# parameters: none
# return a engine type - masking / virtualization

sub getEngineType
{
   my $self = shift;
   logger($self->{_debug}, "Entering System_obj::getEngineType",1);
   my $vMem = $self->{_system}->{engineType};
   return $vMem;
}

# Procedure setEngineType
# parameters: 
# - type
# return a action

sub setEngineType
{
   my $self = shift;
   my $type = shift;
   logger($self->{_debug}, "Entering System_obj::setEngineType",1);

   if (! ((uc $type eq 'VIRTUALIZATION') || (uc $type eq 'MASKING'))) {
    return undef;
   }
   my %type_hash = (
      "type"=> "SystemInfo",
      "engineType"=> uc $type
   );

   my $json = to_json(\%type_hash);
   my $operation = 'resources/json/delphix/system';

   my ($result,$result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json);
   my $jobno;

   if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
       $jobno = $result->{action};
   } else {
       if (defined($result->{error})) {
           print "Problem with starting action\n";
           print "Error: " . Toolkit_helpers::extractErrorFromHash($result->{error}->{details}) . "\n";
           logger($self->{_debug}, "Can't submit job action operation $operation",1);
           logger($self->{_debug}, "error " . Dumper $result->{error}->{details},1);
           logger($self->{_debug}, $result->{error}->{action} ,1);
       } else {
           print "Unknown error. Try with debug flag\n";
       }
   }

   return $jobno;

}

# Procedure configEngine
# parameters:
# - engine_obj 
# - storage

sub configEngine {
   my $self = shift;
   my $engine_obj = shift;
   my $engine_name = shift;
   my $storage = shift;
   my $email = shift;
   my $password = shift;

   $storage->LoadStorageDevices();

   my $ret = 0;
  
   my @init_disks;
 
   for my $disk (@{$storage->getDisks(0)}) {
     push(@init_disks, $disk->{"reference"});   
   }


   my $job = $engine_obj->initializeEngine('admin',\@init_disks,$email,$password);
   $ret = $ret + Toolkit_helpers::waitForAction($engine_obj, $job,'Engine initialized','Problem with engine initialization');
   # wait for boot to complete

   $ret = $ret + $self->wait_for_restart($engine_obj, $engine_name);

   return $ret;
}


sub wait_for_restart {
   my $self = shift;
   my $engine_obj = shift;
   my $engine_name = shift;
   my $ret = 0;

   print("wait for restart\n");
   sleep 15;
   my $booting = 1;

   while($booting eq 1) {
    my ($ret, $ret_for, $boot) = $engine_obj->getJSONResult('resources/json/delphix/session');
    $booting = $boot;
   }

   # try at least 5 times
   my $retry = 5; 
   my $connected = 0;
   
   while(($retry>0) && ($connected eq 0)) {
    sleep 15;
    if ($engine_obj->dlpx_connect($engine_name)) {
      print "Can't reconnect to Dephix Engine " . $engine_name . "\n\n";
      $retry = $retry - 1;
    } else {
      $connected = 1;
    }
   }

   if ($connected eq 1) {
    $ret = 0;
   } else {
    $ret = 1;
   }

   return $ret;
}


# Procedure getSSO
# parameters: none
# Load SSO settings from engine

sub getSSO
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSSO",1);

    if (!defined($self->{_sso})) {

      my $operation = "resources/json/delphix/service/sso";
      my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

      if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        $self->{_sso} = $result->{result};
      } else {
        print "No data returned for $operation. Try to increase timeout \n";
      }

    }

    return $self->{_sso};
}

# Procedure getSSOEntityId
# parameters: none
# Return a SSO EntityId 

sub getSSOEntityId
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSSOEntityId",1);

    my $sso = $self->getSSO();
    return $sso->{"entityId"};
}

# Procedure getSSOsamlMetadata
# parameters: none
# Return a SSO samlMetadata 

sub getSSOsamlMetadata
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSSOsamlMetadata",1);

    my $sso = $self->getSSO();
    return $sso->{"samlMetadata"};
}

# Procedure getSSOmaxAuthenticationAge
# parameters: none
# Return a SSO maxAuthenticationAge 

sub getSSOmaxAuthenticationAge
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSSOmaxAuthenticationAge",1);

    my $sso = $self->getSSO();
    return $sso->{"maxAuthenticationAge"};
}

# Procedure getSSOresponseSkewTime
# parameters: none
# Return a SSO responseSkewTime 

sub getSSOresponseSkewTime
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSSOresponseSkewTime",1);

    my $sso = $self->getSSO();
    return $sso->{"responseSkewTime"};
}

1;
