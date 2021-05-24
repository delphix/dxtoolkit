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
# Copyright (c) 2014,2016 by Delphix. All rights reserved.
#
# Program Name : Engine.pm
# Description  : Delphix Engine common procedures
# Author       : Edward de los Santos
# Created      : 26 Jan 2014 (v1.0.0)
#
# Updated      : 13 Apr 2015 (v2.0.0)
#
package Engine;

BEGIN {

   #print $^O . "\n";

   if ( $^O eq 'MSWin32' ) {
      require WINDOWS_osuser;
      import WINDOWS_osuser qw(:DEFAULT);
   }  elsif ( $^O eq 'darwin' ) {
      require MAC_osuser;
      import MAC_osuser qw(:DEFAULT);
   } else {
      require POSIX_osuser;
      import POSIX_osuser qw (:DEFAULT);
   }
}

use warnings;
use strict;
use POSIX;
use Data::Dumper;
use LWP::UserAgent;
use HTTP::Cookies;
use Toolkit_helpers qw (logger);
use JSON;
use Crypt::CBC;
use Date::Manip;
use FindBin;
use File::Spec;
use File::Basename;
use Try::Tiny;
use Term::ReadKey;
use dbutils;
use Digest::MD5;
use Sys::Hostname;
use open qw(:std :utf8);
use HTTP::Request::Common;
use IO::Socket::SSL;


use LWP::Protocol::http;
push(@LWP::Protocol::http::EXTRA_SOCK_OPTS, MaxLineLength => 0);


# constructor
# parameters
# - debug - debug flag (debug on if defined)

sub new
{
   my $class = shift;
   my $dever = shift;
   my $debug = shift;
   my $ua;

   logger($debug,"Dxtoolkit version " . $Toolkit_helpers::version);
   logger($debug,"Entering Engine::constructor",1);
   $ua = LWP::UserAgent->new;
   #$ua = LWP::UserAgent->new(keep_alive => 1);
   $ua->agent("Delphix Perl Agent/0.1");
   $ua->ssl_opts(
    SSL_verify_mode => IO::Socket::SSL::SSL_VERIFY_NONE,
    verify_hostname => 0
   );
   $ua->timeout(15);

   my $self = {
      _debug => $debug,
      _ua => $ua,
      _dever => $dever,
      _currentuser => ''
   };

   bless $self, $class;

   return $self;
}

# Procedure load_config
# parameters:
# - fn - configuration file name
# load configuration file (dxtools.conf) into internal structure

sub load_config {
   my $self = shift;
   my $fn = shift;
   my $nodecrypt = shift;
   logger($self->{_debug}, "Entering Engine::load_config",1);

   my $data;
   my %engines;

   my $config_file;

   if (defined($fn)) {
     $config_file=$fn;
   } elsif (defined($ENV{'DXTOOLKIT_CONF'})) {
     $config_file=$ENV{'DXTOOLKIT_CONF'};
   } else {
     my $path = $FindBin::Bin;
     $config_file = $path . '/dxtools.conf';
   }

   logger($self->{_debug}, "Loading engines from $config_file");

   open (my $json_stream, $config_file) or die ("Can't load config file $config_file : $!");
   local $/ = undef;
   my $json = JSON->new();
   try {
      $data = $json->decode(<$json_stream>) ;
   } catch {
      die ('Error in JSON configuration file. Please check it. ' . $_);
   };
   close($json_stream);


   for my $host ( @{$data->{data}} ) {
      my $name = $host->{hostname};
      logger($self->{_debug}, "Loading engine $name",2);

      if (defined($host->{username}) && (defined($host->{clientid}))) {
        print "Username and clientid in config entry " . $name . " are mutually exclusive\n";
        next;
      }

      if ((!defined($host->{username})) && (!defined($host->{clientid}))) {
        print "Username or clientid in config entry " . $name . " is required\n";
        next;
      }

      if ((defined($host->{clientid})) && (!defined($host->{clientsecret}))) {
        print "clientid needs clientsecret to be set in config entry " . $name . "\n";
        next;
      }

      if (defined($host->{username})) {
        $engines{$name}{username}   = $host->{username};
        $engines{$name}{password}   = defined($host->{password}) ? $host->{password} : '';
      }

      if (defined($host->{clientid})) {
        $engines{$name}{clientid} = $host->{clientid};
        $engines{$name}{clientsecret} = $host->{clientsecret};
      }

      if (defined($host->{ip_address})) {
        $engines{$name}{ip_address} = $host->{ip_address};
      } else {
        print "ip_address has to be set in config entry " . $name . "\n";
        next;
      }


      $engines{$name}{port}       = defined($host->{port}) ? $host->{port} : 80 ;
      $engines{$name}{default}    = defined($host->{default}) ? $host->{default} : 'false';
      $engines{$name}{protocol}   = defined($host->{protocol}) ? $host->{protocol} : 'http';
      $engines{$name}{encrypted}  = defined($host->{encrypted}) ? $host->{encrypted} : 'false';
      $engines{$name}{timeout}    = defined($host->{timeout}) ? $host->{timeout} : 60;

      # for external password support

      $engines{$name}{passwordvar}    = $host->{passwordvar};
      $engines{$name}{passwordscript} = $host->{passwordscript};
      $engines{$name}{additionalopt}  = $host->{additionalopt};

      if (!defined($nodecrypt)) {
        if ($engines{$name}{encrypted} eq "true") {
            if (defined($engines{$name}{password})) {
              $engines{$name}{password} = $self->decrypt($engines{$name});
            }
            if (defined($engines{$name}{clientsecret})) {
              $engines{$name}{clientsecret} = $self->decrypt($engines{$name});
            }
        }
      }
   }

   $self->{_config_file} = $fn;
   $self->{_engines} = \%engines;
   return 0;
}


# Procedure encrypt_config
# parameters:
# - fn - configuration file name
# - shared - not use hostname in password encryption
# save configuration file (dxtools.conf) from internal structure

sub encrypt_config {
   my $self = shift;
   my $fn = shift;
   my $shared = shift;
   logger($self->{_debug}, "Entering Engine::encrypt_config",1);

   my $engines = $self->{_engines};
   my @engine_list;

   for my $eng ( keys %{$engines} ) {
      if ($engines->{$eng}->{encrypted} eq 'true') {
        if (defined($engines->{$eng}->{password})) {
          $engines->{$eng}->{password} = $self->encrypt($engines->{$eng}, $shared, "password");
        } elsif (defined($engines->{$eng}->{clientsecret})) {
          $engines->{$eng}->{clientsecret} = $self->encrypt($engines->{$eng}, $shared, "key");
        }
      }
      $engines->{$eng}->{hostname} = $eng;
      push (@engine_list, $engines->{$eng});
   }

   my %engine_json = (
       data => \@engine_list
   );

   open (my $fh, ">", $fn) or die ("Can't open new config file $fn for write");
   print $fh to_json(\%engine_json, {pretty=>1});
   close $fh;
   print "New config file $fn created.\n";

}


# Procedure encrypt
# parameters:
# - config hash
# Return encrypted password

sub encrypt {
   my $self = shift;
   my $engine = shift;
   my $shared = shift;
   my $what = shift;
   logger($self->{_debug}, "Entering Engine::encrypt",1);
   my $key;


   my $keypart;
   my $encfield;

   if ($what eq 'password') {
      $keypart = 'username';
      $encfield = 'password';
   } elsif ($what eq 'key') {
      $keypart = 'clientid';
      $encfield = 'clientsecret';
   } else {
     print "something is wrong with ecrypt\n";
     exit 1;
   }

   if (defined($shared)) {
     $key = $engine->{ip_address} . $dbutils::delkey . $engine->{$keypart};
   } else {
     my $host = hostname;
     $key = $engine->{ip_address} . $dbutils::delkey . $engine->{$keypart} . $host;
   }

   my $cipher = Crypt::CBC->new(
      -key    => $key,
      -cipher => 'Blowfish',
      -iv => substr($engine->{ip_address} . $engine->{$keypart},0,8),
      -header=>'none'
   );

   my $passmd5 = Digest::MD5::md5_hex($engine->{$encfield});
   my $wholeenc = $engine->{$encfield} . $passmd5;

   my $ciphertext = $cipher->encrypt_hex($wholeenc);
   return $ciphertext;
}

# Procedure decrypt
# parameters:
# - config hash
# Return decrypted password

sub decrypt {
   my $self = shift;
   my $engine = shift;
   logger($self->{_debug}, "Entering Engine::decrypt",1);

   my $host = hostname;

   # decrypt with crc and hostname

   my $keypart;
   my $encfield;

   if (defined($engine->{password})) {
      $keypart = 'username';
      $encfield = 'password';
   } elsif (defined($engine->{clientid})) {
      $keypart = 'clientid';
      $encfield = 'clientsecret';
   } else {
     print "something is wrong with decrypt\n";
     exit 1;
   }

   my $key = $engine->{ip_address} . $dbutils::delkey . $engine->{$keypart} . $host;
   my $cipher = Crypt::CBC->new(
      -key    => $key,
      -cipher => 'Blowfish',
      -iv => substr($engine->{ip_address} . $engine->{$keypart},0,8),
      -header=>'none'
   );

   my $fulldecrypt  = $cipher->decrypt_hex($engine->{$encfield});
   my $plainpass = substr($fulldecrypt,0,length($fulldecrypt)-32);
   my $checksum = substr($fulldecrypt,length($fulldecrypt)-32);
   my $afterdecrypt = Digest::MD5::md5_hex($plainpass);

   if ($afterdecrypt eq $checksum) {
     return $plainpass;
   } else {
     logger($self->{_debug}, "Decryption with host name doesn't work - moving forward",2);
   }

   # decrypt with crc and no hostname

   $key = $engine->{ip_address} . $dbutils::delkey . $engine->{$keypart};
   my $cipher_nohost = Crypt::CBC->new(
      -key    => $key,
      -cipher => 'Blowfish',
      -iv => substr($engine->{ip_address} . $engine->{$keypart},0,8),
      -header=>'none'
   );

   $fulldecrypt  = $cipher_nohost->decrypt_hex($engine->{$encfield});
   $plainpass = substr($fulldecrypt,0,length($fulldecrypt)-32);
   $checksum = substr($fulldecrypt,length($fulldecrypt)-32);
   $afterdecrypt = Digest::MD5::md5_hex($plainpass);

   if ($afterdecrypt eq $checksum) {
     return $plainpass;
   } else {
     logger($self->{_debug}, "Decryption without host name doesn't work - moving forward to old method",2);
   }

   # old method decryption
   $key = $engine->{ip_address} . $dbutils::delkey . $engine->{$keypart};
   $cipher = Crypt::CBC->new(
      -key    => $key,
      -cipher => 'Blowfish',
      -iv => substr($engine->{ip_address} . $engine->{$keypart},0,8),
      -header=>'none'
   );
   my $password = substr $engine->{$encfield}, 1;
   my $plaintext  = $cipher->decrypt_hex($password);
   return $plaintext;
}


# Procedure getAllNonSysadminEngines
# parameters:
# Return names of all engines loaded

sub getAllNonSysadminEngines {
   my $self = shift;
   logger($self->{_debug}, "Entering Engine::getAllEngines",1);
   my @nonsysadmin = grep { lc $self->{_engines}->{$_}->{username} ne lc 'sysadmin' } sort ( keys %{$self->{_engines} } );
   return @nonsysadmin;
}

# Procedure getAllSysadminEngines
# parameters:
# Return names of all engines loaded

sub getAllSysadminEngines {
   my $self = shift;
   logger($self->{_debug}, "Entering Engine::getAllEngines",1);
   my @nonsysadmin = grep { lc $self->{_engines}->{$_}->{username} eq lc 'sysadmin' } sort ( keys %{$self->{_engines} } );
   return @nonsysadmin;
}

# Procedure getAllEngines
# parameters:
# Return names of all engines loaded

sub getAllEngines {
   my $self = shift;
   logger($self->{_debug}, "Entering Engine::getAllEngines",1);
   return sort ( keys %{$self->{_engines} } );
}

# Procedure getDefaultEngines
# parameters:
# Return names of all defaults engines

sub getDefaultEngines {
   my $self = shift;
   logger($self->{_debug}, "Entering Engine::getDefaultEngines",1);
   my @default;
   for my $engine ( sort ( keys %{$self->{_engines}} ) ) {
      if ($self->{_engines}->{$engine}->{default} eq 'true') {
         push (@default, $engine);
      }
   }
   return @default;
}


# Procedure getEngine
# parameters
# - name
# Return engine config for engine

sub getEngine {
   my $self = shift;
   my $name = shift;
   logger($self->{_debug}, "Entering Engine::getEngine",1);
   return $self->{_engines}->{$name};
}

# Procedure getIP
# parameters:
# Return IP/name of engine

sub getIP {
   my $self = shift;
   logger($self->{_debug}, "Entering Engine::getIP",1);
   return $self->{_host};
}

# Procedure getEngineName
# parameters:
# Return name of engine connected to

sub getEngineName {
   my $self = shift;
   logger($self->{_debug}, "Entering Engine::getEngineName",1);
   if (defined($self->{_enginename})) {
     return $self->{_enginename};
   } else {
     return "unknown";
   }

}


# Procedure getUsername
# parameters:
# Return username

sub getUsername {
   my $self = shift;
   logger($self->{_debug}, "Entering Engine::getUsername",1);
   my $ret;
   if ($self->{_user} =~ /@/) {
     ($ret) = ($self->{_user} =~ /(.*?)@.*/);
   } else {
     $ret = $self->{_user};
   }
   return $ret;
}

# Procedure getApi
# parameters:
# Return api version

sub getApi {
   my $self = shift;
   logger($self->{_debug}, "Entering Engine::getApi",1);
   return $self->{_api};
}

# Procedure dlpx_connect
# parameters:
# - engine - name of engine
# - token - if API token should be used
# return 0 if OK, 1 if failed

sub dlpx_connect {
   my $self = shift;
   my $engine = shift;
   logger($self->{_debug}, "Entering Engine::dlpx_connect",1);

   my $dlpxObject;
   my $rc = 0;

   my %api_list = ( '4.1' => '1.4.0',
                    '4.2' => '1.5.0',
                    '4.3' => '1.6.0',
                    '5.0' => '1.7.0',
                    '5.1' => '1.8.0',
                    '5.2' => '1.9.0',
                    '5.3' => '1.10.0'
                  );

   my $engine_config = $self->{_engines}->{$engine};

   if (! defined($engine_config) ) {
      print "Can't find $engine in config file.\n";
      return 1;
   }




   my $osname = $^O;





   $self->{_host} = $engine_config->{ip_address};
   $self->{_port} = $engine_config->{port};
   $self->{_protocol} = $engine_config->{protocol};
   $self->{_enginename} = $engine;

   my $APIKEYS;

   if (defined($engine_config->{username})) {
     $self->{_user} = $engine_config->{username};
     $self->{_password} = $engine_config->{password};
     $APIKEYS = 0;
   } elsif (defined($engine_config->{clientid})) {
     $self->{_clientid} = $engine_config->{clientid};
     $self->{_clientsecret} = $engine_config->{clientsecret};
     $APIKEYS = 1;
   } else {
     print "Username and clientid are missing from config file\n";
     return 1;
   }


   if ((defined($engine_config->{passwordvar})) && ($engine_config->{passwordvar} ne ""))  {
     logger($self->{_debug}, "Password variable $engine_config->{passwordvar} used to get password");
     if (defined($ENV{$engine_config->{passwordvar}})) {
       $self->{_password} = $ENV{$engine_config->{passwordvar}};
     } else {
       print "Password variable $engine_config->{passwordvar} not set\n";
       logger($self->{_debug}, "Password variable $engine_config->{passwordvar} not set");
       return 1;
     }
   } elsif ((defined($engine_config->{passwordscript})) && ($engine_config->{passwordscript} ne "")) {
     logger($self->{_debug}, "Password script  $engine_config->{passwordscript} used to get password");
     my $line = $engine_config->{passwordscript} . " " . $self->{_enginename} . " " . $engine_config->{username} . " " . $engine_config->{ip_address};
     if ((defined($engine_config->{additionalopt})) && ($engine_config->{additionalopt} ne "")) {
       $line = $line . " " . $engine_config->{additionalopt};
     }
     logger($self->{_debug}, "Script command line $line");
     if (! -f "$engine_config->{passwordscript}") {
       print "Password script $engine_config->{passwordscript} doesn't exist\n";
       logger($self->{_debug}, "Password script $engine_config->{passwordscript} doesn't exist");
       return 1;
     }

     my $out = qx|$line|;
     $out =~ s/^\s+|\s+$//g;
     $self->{_password} = $out;
   }

   my $cookie_dir = File::Spec->tmpdir();
   my $cookie_file = File::Spec->catfile($cookie_dir, "cookies." . getOSuser() . "." . $engine  );

   my $cookie_jar;

   if ($APIKEYS eq 0) {
     $cookie_jar = HTTP::Cookies->new(file => $cookie_file, autosave => 1, ignore_discard=>1);
   } else {
     $cookie_jar = HTTP::Cookies->new(file => $cookie_file, autosave => 1);
   }

   $self->{_ua}->cookie_jar($cookie_jar);

   $self->{_ua}->cookie_jar->save();

   logger($self->{_debug},"Cookie file " . $cookie_file,2);

   if ( $osname ne 'MSWin32' ) {
      chmod 0600, $cookie_file or die("Can't make cookie file secure.");
   } else {
      logger($self->{_debug},"Can't secure cookie. Windows machine");
   }

   undef $self->{timezone};

   logger($self->{_debug},"connecting to: $engine ( IP/name : " . $self->{_host} . " )");

   if (defined($self->{_debug})) {
      $self->{_ua}->show_progress( 1 );
   }

   # if we are using API keys, session from cookies should be clean up
   # so dxtoolkit will try to login using keys over and over

   my $ses_status;
   my $ses_version;
   my $token;

   if ($APIKEYS) {
     $cookie_jar->clear();
     $ses_status = 1;
     $token = $self->getSSOToken($self->{_clientid}, $self->{_clientsecret});
     if (!defined($token)) {
       print "Can't get token from token provider\n";
       return 1;
     }
   } else {
     ($ses_status, $ses_version) = $self->getSession();
     if ($ses_status > 1) {
        print "Can't check session status. Engine $engine (IP: " . $self->{_host} . " ) could be down.\n";
        #logger($self->{_debug},"Can't check session status. Engine could be down.");
        return 1;
     }
   }

   if ($ses_status) {
      # there is no session in cookie
      # new session needs to be established

      if (defined($self->{_dever})) {
            if (defined($api_list{$self->{_dever}})) {
               $ses_version = $api_list{$self->{_dever}};
               logger($self->{_debug}, "Using Delphix Engine version defined by user " . $self->{_dever} . " . API " . $ses_version , 2);
               $self->{_api} = $ses_version;
            } else {
               logger($self->{_debug}, "Delphix version " . $self->{_dever} . " unknown");
               return 1;
            }
       } else {
          # use an Engine API
          $self->session('1.3.0');
          my $operation = "resources/json/delphix/about";
          my ($result,$result_fmt, $retcode) = $self->getJSONResult($operation);
          if ($result->{status} eq "OK") {
             $ses_version = $result->{result}->{apiVersion}->{major} . "." . $result->{result}->{apiVersion}->{minor}
                            . "." . $result->{result}->{apiVersion}->{micro};
             $self->{_api} = $ses_version;
          } else {
             logger($self->{_debug}, "Can't determine Delphix API version" );
             return 1;
          }

       }

       if (defined($token)) {
         # create session for token login
         if ( $self->token_login($token, $ses_version)) {
           print "token login to " . $self->{_host} . "  failed. \n";
           $cookie_jar->clear();
           $rc = 1;
         } else {
           logger($self->{_debug}, "token login to " . $self->{_host} . "  succeeded.");
           $rc = 0;
         }
       } else {
           # create a session first for user / password
           if ( $self->session($ses_version) ) {
              logger($self->{_debug}, "session authentication to " . $self->{_host} . " failed.");
              $rc = 1;
           } else {
             # session is established - now login
             if ($self->{_password} eq '') {
               # if no password provided and there is no open session
               $self->{_password} = $self->read_password();
             }
             if ( $self->login() ) {
                 print "login to " . $self->{_host} . "  failed. \n";
                 $cookie_jar->clear();
                 $rc = 1;
             } else {
                 logger($self->{_debug}, "login to " . $self->{_host} . "  succeeded.");
                 $rc = 0;
             }
           }
        }
   } else {
      # there is a valid session in cookie
      logger($self->{_debug}, "Session exists.");
      # check if session user is same like config user or someone is messing around

      if ( $self->getCurrentUser() eq $self->getUsername() ) {
        $self->{_api} = $ses_version;
        $rc = 0;
      } else {
        logger($self->{_debug}, "Something is wrong. Session from cookie doesn't match config file");
        print "Something is wrong. Session from cookie doesn't match config file. Clearing cookie file\n";
        my $cookie_jar = $self->{_ua}->cookie_jar;
        $cookie_jar->clear();
        $rc = 1;
      }
   }

   $self->{_ua}->timeout($engine_config->{timeout});
   return $rc;
}

# Procedure session
# parameters: none
# open a session with Delphix Engine
# return 0 if OK, 1 if failed

sub session {
   my $self = shift;
   my $version = shift;
   logger($self->{_debug}, "Entering Engine::session",1);

   my $major;
   my $minor;
   my $micro;

   if (defined($version)) {
         ($major,$minor,$micro) = split(/\./,$version);
   }
   else {
         $major = 1;
         $minor = 2;
         $micro = 0;
   }

   my %mysession =
   (
      "session" => {
         "type" => "APISession",
         "version" => {
            "type" => "APIVersion",
            "major" => $major + 0,
            "minor" => $minor + 0,
            "micro" => $micro + 0
         }
      }
   );

   logger($self->{_debug}, "API Version: $major\.$minor");
   my $operation = "resources/json/delphix/session";
   my $json_data = encode_json($mysession{'session'});
   logger($self->{_debug}, $json_data, 2);
   my ($result,$result_fmt, $retcode) = $self->postJSONData($operation,$json_data);

   my $ret;

   if ($retcode || ($result->{status} eq 'ERROR') ) {
      $ret = 1;
   } else {
      $ret = 0;
   }

   return $ret;
}


# procedure getSession
# parameters: none
# check if there is still a session saved in cookies file
# return 0 if OK, 1 if failed

sub getSession {
   my $self = shift;
   my $operation = "resources/json/delphix/session";
   my ($result,$result_fmt, $retcode) = $self->getJSONResult($operation);

   my $ret;
   my $ver_api;

   if ($retcode || ($result->{status} eq 'ERROR') ) {
      $ret = 1 + $retcode;
   } else {
      $ver_api = $result->{result}->{version}->{major} . "." . $result->{result}->{version}->{minor} .
                 "." . $result->{result}->{version}->{micro};
      $ret = 0;
   }

   return ($ret, $ver_api);

}


# Procedure getCurrentUser
# parameters:
# Return current logged user

sub getCurrentUser {
    my $self = shift;
    my $ret;

    logger($self->{_debug}, "Entering Engine::getCurrentUser",1);

    my $operation = "resources/json/delphix/user/current";
    my ($result, $result_fmt) = $self->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        $ret = $result->{result};
        $self->{_currentuser} = $ret->{name};
        $self->{_currentusertype} = $ret->{userType};
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }

    return $self->{_currentuser};

}


# Procedure getCurrentUserType
# parameters:
# Return current logged user type

sub getCurrentUserType {
    my $self = shift;
    my $ret;

    logger($self->{_debug}, "Entering Engine::getCurrentUserType",1);

    $self->getCurrentUser();

    return $self->{_currentusertype};

}



# Procedure session
# parameters: none
# login user with Delphix Engine
# return 0 if OK, 1 if failed

sub login {
   my $self = shift;
   my $user = $self->{_user};
   my $password = $self->{_password};
   my $result_fmt;
   my $retcode;
   my $result;
   logger($self->{_debug}, "Entering Engine::login",1);

   my $domain;
   my %mylogin;

   if (($domain) = $user =~ /(\w+)@(\w+)/) {
     if (uc $2 eq "DOMAIN") {
       $domain = "DOMAIN";
       $user = $1;
     } elsif (uc $2 eq "SYSTEM") {
       $domain = "SYSTEM";
       $user = $1;
     } else {
       print "User can have only target - DOMAIN or SYSTEM";
       return 1;
     }
     %mylogin =
     (
        "user" => {
           "type" => "LoginRequest",
           "username" => "$user",
           "password" => "$password",
           "target" => $domain
        }
     );
   } else {
     #  keep this for backward compability of dxtools.conf file
     #  if sysadmin is defined there we need to be able to login
     %mylogin =
     (
        "user" => {
           "type" => "LoginRequest",
           "username" => "$user",
           "password" => "$password"
        }
     );
   }


   my $operation = "resources/json/delphix/login";
   my $json_data = encode_json($mylogin{'user'});
   ($result,$result_fmt, $retcode) = $self->postJSONData($operation,$json_data);

   my $ret;

   if ($retcode || ($result->{status} eq 'ERROR') ) {
      $ret = 1;
   } else {
      $ret = 0;
   }

   return $ret;


}

sub token_login {
  my $self = shift;
  my $token = shift;
  my $version = shift;


  my ($major,$minor,$micro) = split(/\./,$version);


  my %mysession =
  (
    "type" => "APISession",
    "version" => {
       "type" => "APIVersion",
       "major" => $major + 0,
       "minor" => $minor + 0,
       "micro" => $micro + 0
     }
  );

  my $h = HTTP::Headers->new(
    Content_Type        => 'application/json',
    Authorization       => 'Bearer ' . $token
  );

  my $operation = "sso/virtualization/api/login";
  my $url = $self->{_protocol} . '://' . $self->{_host} . ':' . $self->{_port};
  my $api_url = "$url/$operation";

  my $request = HTTP::Request->new(
    POST => $api_url,
    $h
  );

  $request->content(to_json(\%mysession));

  my $response = $self->{_ua}->request($request);

  my $decoded_response;
  my $result;
  my $result_fmt;
  my $retcode;

  if ( $response->is_success ) {
     $decoded_response = $response->decoded_content;
     $result = decode_json($decoded_response);
     $result_fmt = to_json($result, {pretty=>1});
     logger($self->{_debug}, "Response message: " . $result_fmt, 2);
     $retcode = 0;
  }
  else {
     logger($self->{_debug}, "HTTP POST error code: " . $response->code, 2);
     logger($self->{_debug}, "HTTP POST error message: " . $response->message, 2);
     if (($response->code == 401) || ($response->code == 403)) {
       my $cookie_jar = $self->{_ua}->cookie_jar;
       $cookie_jar->clear();
     }
     $retcode = 1;
  }


}


# Procedure logout
# parameters: none
# login user with Delphix Engine
# return 0 if OK, 1 if failed

sub logout {
   my $self = shift;
   my $result_fmt;
   my $retcode;
   my $result;
   logger($self->{_debug}, "Entering Engine::logout",1);

   my $operation = "resources/json/delphix/logout";
   ($result,$result_fmt, $retcode) = $self->postJSONData($operation,'{}');

   my $ret;

   if ($retcode || ($result->{status} eq 'ERROR') ) {
      $ret = 1;
   } else {
      $ret = 0;
   }

   my $cookie_jar = $self->{_ua}->cookie_jar;
   $cookie_jar->clear();
   return $ret;


}




# Procedure getTimezone
# parameters: none
# return timezone of Delphix engine

sub getTimezone {
   my $self = shift;
   logger($self->{_debug}, "Entering Engine::getTimezone",1);
   my $timezone;
   if (defined($self->{timezone})) {
      $timezone = $self->{timezone};
   } else {
      my $operation = "resources/json/service/configure/currentSystemTime";
      my ($result,$result_fmt, $retcode) = $self->getJSONResult($operation);
      if ($result->{result} eq "ok") {
         $timezone = $result->{systemTime}->{localTimeZone};
         $self->{timezone} = $timezone;
      } else {
         $timezone = 'N/A';
      }
   }

   return $timezone;

}


# Procedure getTime
# parameters:
# - minus - date current date minus minus minutes
# return timezone of Delphix engine

sub getTime {
   my $self = shift;
   my $minus = shift;

   logger($self->{_debug}, "Entering Engine::getTime",1);
   my $time;
   my $operation = "resources/json/delphix/service/time";
   my ($result,$result_fmt, $retcode) = $self->getJSONResult($operation);
   if ($result->{status} eq "OK") {
      $time = $result->{result}->{currentTime};
      my $tz = $result->{result}->{systemTimeZone};

      $time = Toolkit_helpers::convert_from_utc($time, $tz);

      if (defined($minus)) {
        my $date = new Date::Manip::Date;

        if ($date->parse($time)) {
          print "Date parsing error\n";
          return 'N/A';
        }


        my $delta = $date->new_delta();
        my $deltastr = $minus . ' minutes ago';
        if ($delta->parse($deltastr)) {
          print "Delta time parsing error\n";
          return 'N/A';
        }
        my $d = $date->calc($delta);
        $time = $d->printf("%Y-%m-%d %H:%M:%S");

      }

   } else {
      $time = 'N/A';
   }

   return $time;

}



# Procedure checkSSHconnectivity
# parameters:
# -username - user name to check
# -password - password
# -hostip - ip of host to check
# -port - port of host to check
# return array (0 if OK, 1 if not OK, reason)

sub checkSSHconnectivity {
   my $self = shift;
   my $username = shift;
   my $password = shift;
   my $host = shift;
   my $port = shift;

   logger($self->{_debug}, "Entering Engine::checkSSHconnectivity",1);

   if (!defined($port)) {
     $port = 22;
   }

   my %conn_hash = (
       "type" => "SSHConnectivity",
       "address" => $host,
       "port" => $port,
       "credentials" => {
           "type" => "PasswordCredential",
           "password" => $password
       },
       "username" => $username
   );

   my $json_data = to_json(\%conn_hash, {pretty=>1});

   my $operation = "resources/json/delphix/connectivity/ssh";
   my ($result,$result_fmt, $retcode) = $self->postJSONData($operation,$json_data);

   my $ret;

   if ($retcode || ($result->{status} eq 'ERROR') ) {
      logger($self->{_debug}, $result_fmt, 2);
      $ret = 1;
   } else {
      $ret = 0;
   }
   return ($ret, $result);
}

# Procedure checkConnectorconnectivity
# parameters:
# - username
# - password
# - host
# return 0 if credentials are OK

sub checkConnectorconnectivity {
   my $self = shift;
   my $username = shift;
   my $password = shift;
   my $host = shift;

   logger($self->{_debug}, "Entering Engine::checkConnectorconnectivity",1);

   my %conn_hash = (
       "type" => "ConnectorConnectivity",
       "address" => $host,
       "credentials" => {
           "type" => "PasswordCredential",
           "password" => $password
       },
       "username" => $username
   );

   my $json_data = to_json(\%conn_hash, {pretty=>1});

   my $operation = "resources/json/delphix/connectivity/connector";
   my ($result,$result_fmt, $retcode) = $self->postJSONData($operation,$json_data);

   my $ret;

   if ($retcode || ($result->{status} eq 'ERROR') ) {
      logger($self->{_debug}, $result_fmt, 2);
      $ret = 1;
   } else {
      $ret = 0;
   }

   return $ret;
}

# Procedure checkJDBCconnectivity
# parameters:
# username
# password
# jdbc string
# return 0 if credentials are OK

sub checkJDBCconnectivity {
   my $self = shift;
   my $username = shift;
   my $password = shift;
   my $jdbc = shift;

   logger($self->{_debug}, "Entering Engine::checkJDBCconnectivity",1);

   my %conn_hash = (
       "type" => "JDBCConnectivity",
       "url" => $jdbc,
       "user" => $username,
       "password" => $password,
   );

   my $json_data = to_json(\%conn_hash, {pretty=>1});

   my $operation = "resources/json/delphix/connectivity/jdbc";
   my ($result,$result_fmt, $retcode) = $self->postJSONData($operation,$json_data);

   my $ret;

   if ($retcode || ($result->{status} eq 'ERROR') ) {
      logger($self->{_debug}, $result_fmt, 2);
      $ret = 1;
   } else {
      $ret = 0;
   }

   return $ret;
}


# Procedure getJSONResult
# parameters:
# - operation - API url
# Send GET request to Delphix engine with url defined in operation parameter
# return
# - response
# - pretty formated response
# - rc - 0 if OK, 1 if failed

sub getJSONResult {
   my $self = shift;
   my $operation = shift;

   my $result;
   my $result_fmt;
   my $decoded_response;
   my $retcode;
   logger($self->{_debug}, "Entering Engine::getJSONResult",1);


   my $url = $self->{_protocol} . '://' . $self->{_host} . ':' . $self->{_port};
   my $api_url = "$url/$operation";


   logger($self->{_debug}, "GET: $api_url");

   my $request = HTTP::Request->new(GET => $api_url);
   $request->content_type("application/json");

   my $response = $self->{_ua}->request($request);

   if ( $response->is_success ) {
      $decoded_response = $response->decoded_content;
      $result = decode_json($decoded_response);
      if (defined($self->{_debug}) && ( $self->{_debug} eq 3) ) {
         my $enginename = $self->getEngineName();
         my $debug_dir = "debug_" . $enginename;
         if (! -e $debug_dir) {
            mkdir $debug_dir or die("Can't create root directory for debug ");
         }
         my $tempname = $operation;
         $tempname =~ s|resources/json/delphix/||;
         $tempname =~ s|resources/json/service/||;
         my @filenames = split('/', $tempname);
         if (scalar(@filenames) > 1) {
            my @dirname;
            for (my $i=0; $i<scalar(@filenames)-1; $i++) {
               @dirname = @filenames[0..$i];
               my $md = $debug_dir . "/" . join('/',@dirname);
               if (! -e $md) {
                  mkdir $md or die("Can't create directory for debug " . $md);
               }
            }

         }
         my $filename = $tempname . ".json";
         $filename =~ s|\?|_|;
         $filename =~ s|\&|_|g;
         $filename =~ s|\:|_|g;
         #print Dumper $filename;
         open (my $fh, ">", $debug_dir . "/" . $filename) or die ("Can't open new debug file $filename for write");
         print $fh to_json($result, {pretty=>1});
         close $fh;
      }
      $result_fmt = to_json($result, {pretty=>1});
      $retcode = 0;
   }
   else {
      logger($self->{_debug}, "HTTP GET error code: " . $response->code, 2);
      logger($self->{_debug}, "HTTP GET error message: " . $response->message,2 );
      logger($self->{_debug}, "Response message: " . Dumper $result_fmt, 2);
      $retcode = 1;
   }

   return ($result,$result_fmt, $retcode);
}


# Procedure generateSupportBundle
# parameters:
# - file
# Generate a support bundle

sub generateSupportBundle {
   my $self = shift;
   my $file = shift;
   my $type = shift;
   my $analytics = shift;

   logger($self->{_debug}, "Entering Engine::generateSupportBundle",1);
   my $timeout =    $self->{_ua}->timeout();
   $self->{_ua}->timeout(60*60*24);

  #  === POST /resources/json/delphix/service/support/bundle/upload ===
  #  {
  #      "type": "SupportBundleUploadParameters",
  #      "includeAnalyticsData": true,
  #      "caseNumber": 666666
  #  }


  my %allowed_types = (
    "PHONEHOME" => 1,
    "MDS" => 1,
    "OS" => 1,
    "CORE" => 1,
    "LOG" => 1,
    "PLUGIN_LOG" => 1,
    "DROPBOX" => 1,
    "STORAGE_TEST" => 1,
    "MASKING" => 1,
    "ALL" => 1
  );

  my $bundle_type;

  if (defined($type)) {
    if (defined($allowed_types{uc $type})) {
      $bundle_type = uc $type;
    } else {
      print "Unknown type $type. Please specify a proper one. \n";
      return 1;
    }
  } else {
    $bundle_type = 'ALL';
  }

  my %bundle_hash = (
    "type" => "SupportBundleGenerateParameters",
    "bundleType" => $bundle_type
  );

  if (defined($analytics)) {
    $bundle_hash{"includeAnalyticsData"} = JSON::true;
  }

  my $json = to_json(\%bundle_hash);

   my $operation = "resources/json/delphix/service/support/bundle/generate";
   my ($result,$result_fmt, $retcode) = $self->postJSONData($operation,$json);

   my $ret;
   my $token;

   if ($retcode || ($result->{status} eq 'ERROR') ) {
      logger($self->{_debug}, 'bundle response - ' . $result_fmt, 2);
      $ret = 1;
   } else {
      $token = $result->{result};
      logger($self->{_debug}, 'token ' . $token, 2);

      my $url = $self->{_protocol} . '://' . $self->{_host} . ':' . $self->{_port} . '/resources/json/delphix/data/download?token='. $token;
      logger($self->{_debug}, $url , 2);

      my $response = $self->{_ua}->get($url, ':content_file' => $file);

      if ($response->is_success) {
         $ret = 0;
      } else {
         logger($self->{_debug}, 'data response - ' . $response, 2);
         $ret = 1;
      }
   }

   $self->{_ua}->timeout($timeout);
   return $ret;
}

# Procedure uploadSupportBundle
# parameters:
# - caseNumber
# Upload a support bundle

sub uploadSupportBundle {
   my $self = shift;
   my $caseNumber = shift;
   my $type = shift;
   my $analytics = shift;

   logger($self->{_debug}, "Entering Engine::uploadSupportBundle",1);

   my %allowed_types = (
     "PHONEHOME" => 1,
     "MDS" => 1,
     "OS" => 1,
     "CORE" => 1,
     "LOG" => 1,
     "PLUGIN_LOG" => 1,
     "DROPBOX" => 1,
     "STORAGE_TEST" => 1,
     "MASKING" => 1,
     "ALL" => 1
   );

   my $bundle_type;

   if (defined($type)) {
     if (defined($allowed_types{uc $type})) {
       $bundle_type = uc $type;
     } else {
       print "Unknown type $type. Please specify a proper one. \n";
       return undef;
     }
   } else {
     $bundle_type = 'ALL';
   }

   my %case_hash = (
       "type" => "SupportBundleUploadParameters",
       "bundleType" => $bundle_type
   );

   if (defined($analytics)) {
     $case_hash{"includeAnalyticsData"} = JSON::true;
   }

   if (defined($caseNumber)) {
      $case_hash{caseNumber} = 0 + $caseNumber;
   }

   my $to_json = to_json(\%case_hash);
   my $operation = "resources/json/delphix/service/support/bundle/upload";
   my ($result,$result_fmt, $retcode) = $self->postJSONData($operation,$to_json);

   my $ret;


   if ($retcode || ($result->{status} eq 'ERROR') ) {
      print "Error with submitting a new job - " . $result->{error}->{details} . "\n";
      logger($self->{_debug}, $result_fmt, 2);
      $ret = undef;
   } else {
      $ret = $result->{job};
      logger($self->{_debug}, 'jobno ' . $ret, 2);
   }

   return $ret;
}


# Procedure postJSONData
# parameters:
# - operation - API url
# - post_data - json data to send
# Send POST request to Delphix engine with url defined in operation parameter
# and data defined in post_data
# return
# - response
# - pretty formated response
# - rc - 0 if OK, 1 if failed

sub postJSONData {
   my $self = shift;
   my $operation = shift;
   my $post_data = shift;
   my $result;
   my $result_fmt;
   my $decoded_response;
   my $retcode;

   logger($self->{_debug}, "Entering Engine::postJSONData",1);

   my $url = $self->{_protocol} . '://' . $self->{_host} . ':' . $self->{_port};
   my $api_url = "$url/$operation";

   #logger($self->{_debug}, "$api_url");

   my $request = HTTP::Request->new(POST => $api_url);
   $request->content_type("application/json");

   if (defined($post_data)) {
      $request->content($post_data);
   }

   my $post_data_logger;

   if ( $post_data =~ /password/ ) {
     $post_data_logger = $post_data;
     $post_data_logger =~ s/"password":"(.*?)"/"password":"xxxxx"/;
   } else {
     $post_data_logger = $post_data;
   }


   logger($self->{_debug}, $post_data_logger, 1);

   my $response = $self->{_ua}->request($request);

   if ( $response->is_success ) {
      $decoded_response = $response->decoded_content;
      $result = decode_json($decoded_response);
      $result_fmt = to_json($result, {pretty=>1});
      logger($self->{_debug}, "Response message: " . $result_fmt, 2);
      $retcode = 0;
   }
   else {
      logger($self->{_debug}, "HTTP POST error code: " . $response->code, 2);
      logger($self->{_debug}, "HTTP POST error message: " . $response->message, 2);
      if (($response->code == 401) || ($response->code == 403)) {
        my $cookie_jar = $self->{_ua}->cookie_jar;
        $cookie_jar->clear();
      }
      $retcode = 1;
   }

   if (defined($self->{_debug}) && ( $self->{_debug} eq 3) ) {
      my $enginename = $self->getEngineName();
      my $debug_dir = "debug_" . $enginename;
      if (! -e $debug_dir) {
         mkdir $debug_dir or die("Can't create root directory for debug ");
      }
      my $tempname = $operation;
      $tempname =~ s|resources/json/delphix/||;
      $tempname =~ s|resources/json/service/||;
      my @filenames = split('/', $tempname);
      if (scalar(@filenames) > 1) {
         my @dirname;
         for (my $i=0; $i<scalar(@filenames)-1; $i++) {
            @dirname = @filenames[0..$i];
            my $md = $debug_dir . "/" . join('/',@dirname);
            if (! -e $md) {
               mkdir $md or die("Can't create directory for debug " . $md);
            }
         }

      }

      if (defined($self->{_debugfiles}) && defined($self->{_debugfiles}->{$tempname})) {
        $self->{_debugfiles}->{$tempname} = $self->{_debugfiles}->{$tempname} + 1;
      } else {
        my %debug_hash;
        $self->{_debugfiles} = \%debug_hash;
        $self->{_debugfiles}->{$tempname} = 1;
      }

      #print Dumper $tempname . " " . $self->{_debugfiles}->{$tempname};

      my $filename = $tempname . ".json." . $self->{_debugfiles}->{$tempname};
      $filename =~ s|\?|_|;
      $filename =~ s|\&|_|g;
      $filename =~ s|\:|_|g;
      if (!defined($result)) {
        $result = {};
      }
      open (my $fh, ">", $debug_dir . "/" . $filename) or die ("Can't open new debug file $filename for write");
      print $fh to_json($result, {pretty=>1});
      close $fh;

      $filename = $tempname . ".json.req." . $self->{_debugfiles}->{$tempname};
      $filename =~ s|\?|_|;
      $filename =~ s|\&|_|g;
      $filename =~ s|\:|_|g;
      #print Dumper $filename;
      open ($fh, ">", $debug_dir . "/" . $filename) or die ("Can't open new debug file $filename for write");
      print $fh $post_data_logger;
      close $fh;

   }

   return ($result,$result_fmt, $retcode);
}


sub read_password {
    Term::ReadKey::ReadMode('noecho');
    print "Password: ";
    my $pass = Term::ReadKey::ReadLine(0);
    Term::ReadKey::ReadMode('restore');
    $pass =~ s/\R$//;
    return $pass;
}


# Procedure getLicenseUsage
# parameters:
# Return a hash with license usage { total: size, databases: [ database:size, ] }

sub getLicenseUsage {
    my $self = shift;

    logger($self->{_debug}, "Entering Engine::getLicenseUsage",1);

    my %res;

    my $operation = "resources/json/delphix/usage/aggregateIngestedSize";
    my ($result,$result_fmt, $retcode) = $self->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        $res{"total"} = $result->{"result"}->{"aggregateIngestedSize"};
        my @dblist;
        my $type;
        for my $db (@{$result->{"result"}->{"sourceIngestionData"}}) {
          $db->{"containerType"} =~ s/_DB_CONTAINER//;
          push(@dblist, { "name"=> $db->{"sourceName"}, "type"=>$db->{"containerType"}, "size"=> $db->{"ingestedSize"} });
        }
        $res{"databases"} = \@dblist;
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }


    return \%res;

}

# Procedure getOSversionList
# parameters:
# Return an array of hash with of OS version deployed

sub getOSversions {
    my $self = shift;

    logger($self->{_debug}, "Entering Engine::getOSversions",1);

    my %res;

    my $operation = "resources/json/delphix/system/version";
    my ($result,$result_fmt, $retcode) = $self->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        for my $osver (@{$result->{result}}) {
          $res{$osver->{name}} = $osver;
        }
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }


    return \%res;

}


# Procedure verifyOSversion
# parameters:
# - OS version name
# return jobid or undef

sub verifyOSversion {
    my $self = shift;
    my $name = shift;

    logger($self->{_debug}, "Entering Engine::verifyOSversion",1);

    my $versions = $self->getOSversions();

    if (!defined($versions->{$name})) {
      print "Version with osname $name not found in Delphix Engine. No verification will be performed\n";
      return undef;
    };

    my $osref = $versions->{$name}->{reference};
    my $operation = 'resources/json/delphix/system/version/' . $osref . '/verify';
    my ($result,$result_fmt, $retcode) = $self->postJSONData($operation, '{}');
    my $jobno;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $jobno = $result->{job};
    } else {
        if (defined($result->{error})) {
            print "Problem with starting job\n";
            print "Error: " . Toolkit_helpers::extractErrorFromHash($result->{error}->{details}) . "\n";
            logger($self->{_debug}, "Can't submit job for operation $operation",1);
            logger($self->{_debug}, "error " . Dumper $result->{error}->{details},1);
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
    }

    return $jobno;
}


# Procedure applyOSversion
# parameters:
# - OS version name
# return jobid or undef

sub applyOSversion {
    my $self = shift;
    my $name = shift;

    logger($self->{_debug}, "Entering Engine::applyOSversion",1);

    my $versions = $self->getOSversions();

    if (!defined($versions->{$name})) {
      print "Version with osname $name not found in Delphix Engine. Apply will not be performed\n";
      return undef;
    };

    my $osref = $versions->{$name}->{reference};
    my $operation = 'resources/json/delphix/system/version/' . $osref . '/apply';
    my ($result,$result_fmt, $retcode) = $self->postJSONData($operation, '{}');
    my $jobno;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $jobno = $result->{job};
    } else {
        if (defined($result->{error})) {
            print "Problem with starting job\n";
            print "Error: " . Toolkit_helpers::extractErrorFromHash($result->{error}->{details}) . "\n";
            logger($self->{_debug}, "Can't submit job for operation $operation",1);
            logger($self->{_debug}, "error " . Dumper $result->{error}->{details},1);
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
    }

    return $jobno;
}



# Procedure uploadupdate
# parameters:
# - filename
# return result of upload
# 0 is all OK

sub uploadupdate {
    my $self = shift;
    my $filename = shift;

    logger($self->{_debug}, "Entering Engine::uploadupdate",1);
    #local $HTTP::Request::Common::DYNAMIC_FILE_UPLOAD = 1;

    my $url = $self->{_protocol} . '://' . $self->{_host} ;
    my $api_url = "$url/resources/json/system/uploadUpgrade";

    my $size = -s $filename;
    my $boundary = HTTP::Request::Common::boundary(10);

    my $fsize = $size;
    # 6 for - char
    # 63 is Content-Disposition plus end of lines
    $size = $size + 2 * (length $boundary) + 6 + (length basename($filename)) + 63;


    my $h = HTTP::Headers->new(
      Content_Length      => $size,
      Connection          => 'keep-alive',
      Content_Type        => 'multipart/form-data; boundary=' . $boundary
    );

    my $request = HTTP::Request->new(
      POST => $api_url, $h
    );


    # Perl $HTTP::Request::Common::DYNAMIC_FILE_UPLOAD = 1 allows to load any file size
    # without loading all in memory but it's very slow as it's using 2k chunks
    # content provider procedure is develop instead of using DYNAMIC_FILE_UPLOAD
    # and it's providing a content of multipart/form-data request
    # it's simple implementation and probably not a best one


    my $content_provider_ref = &content_provider($filename, $size, $boundary, $self, $fsize);
    $request->content($content_provider_ref);


    sub content_provider {
      my $filename = shift;
      my $size = shift;
      my $boundary = shift;
      my $self = shift;
      my $fsize = shift;
      # we need to send 4 parts - a boundary start, content description, file content, boundary end
      my @content_part = ( 'b', 'c', 'f', 'e' );
      my $total = 0;
      my $report = 0;
      my $end = 0;
      my $end2 = 0;
      my $real  = 0;
      $| = 1;

      my $fh;
      open $fh, $filename;
      binmode $fh;

      return sub {
        my $buf;

        # print Dumper $content_part[0];
        # print Dumper "----";
        # print Dumper $total;

        if (!defined($content_part[0])) {
          if ($end eq 0) {
            printf "%5.1f\n", 100;
            $end = 1;
          }
          return undef;
        }

        if ($content_part[0] eq 'e') {
          $buf = "\r\n--" . $boundary . "--\r\n";
          shift @content_part;
          #print Dumper $buf;
        } elsif ($content_part[0] eq 'b') {
          $buf = "--" . $boundary . "\r\n";
          shift @content_part;
          #print Dumper $buf;
          # my $buf2;
          # my $rc = sysread($fh, $buf2, 1048576);
          # $buf = $buf . $buf2;
          # $total = $total + length $buf2;
          # print Dumper $total;
        } elsif ($content_part[0] eq 'c') {
          $buf = "Content-Disposition: form-data; name=\"file\"; filename=\"" . basename($filename) . "\"\r\n\r\n";
          shift @content_part;
          #print Dumper $buf;
        } elsif ($content_part[0] eq 'f') {
          my $rc = sysread($fh, $buf, 1048576);
          $total = $total + length $buf;
          #print Dumper $total;
          # print Dumper $rc;
          if (($total / $fsize * 100) > $report) {
            if (($total / $fsize * 100) eq 100) {
              printf "%5.1f\n ", 100;
              $end = 1;
            } else {
              printf "%5.1f - ", $total / $fsize * 100;
              $report = $report + 10;
            }

            }
            if ($rc ne 1048576) {
              shift @content_part;
            }

          #print Dumper $buf;
        }
        $real = $real + length $buf;
        #print Dumper $buf;
        return $buf;
      }
    }

    my $response = $self->{_ua}->request($request);

    my $decoded_response;
    my $result_fmt;
    my $retcode;
    my $result;

    if ( $response->is_success ) {

       $decoded_response = $response->decoded_content;
       $result = decode_json($decoded_response);
       $result_fmt = to_json($result, {pretty=>1});
       logger($self->{_debug}, "Response message: " . $result_fmt, 2);
       if (defined($result->{status}) && ($result->{status} eq 'OK')) {
         print "\nFile upload completed without issues.\n";
         $retcode = 0;
       } elsif (defined($result->{result}) && ($result->{result} eq 'failed')) {
         print "\nFile upload issues\n";
         print "Try the operation again. If the problem persists, contact Delphix support.\n";
         $retcode = 1;
       }


    }
    else {
       logger($self->{_debug}, "HTTP POST error code: " . $response->code, 2);
       logger($self->{_debug}, "HTTP POST error message: " . $response->message, 2);
       $retcode = 1;
    }

}


# Procedure uploadupdate
# parameters:
# - filename
# return result of upload
# 0 is all OK

sub uploadupdate60 {
    my $self = shift;
    my $filename = shift;

    logger($self->{_debug}, "Entering Engine::uploadupdate",1);
    #local $HTTP::Request::Common::DYNAMIC_FILE_UPLOAD = 1;

    my $url = $self->{_protocol} . '://' . $self->{_host} ;
    my $api_url = "$url/resources/json/system/uploadUpgrade";
    #
    # $self->{_ua}->proxy([
    #      [ 'http' ] => 'http://127.0.0.1:8888/',
    # ]);



    #$self->{_ua}->add_handler("request_send",  sub { shift->dump; return });
    #$self->{_ua}->add_handler("response_done", sub { shift->dump; return });

    my ($r,$r_fmt, $rcode) = $self->getJSONResult("resources/json/system/uploadUpgrade?file=$filename");
    print Dumper $r;



    my $fsize = -s $filename;
    my $boundary = HTTP::Request::Common::boundary(8);
    $boundary = "-------------------------------" . $boundary;

    my $fh;
    open $fh, $filename;
    binmode $fh;

    my $maxchunksize = 1024000000;
    #my $maxchunksize = 10240;

    #my $maxpartsize = 3;

    my $noofparts = $fsize / $maxchunksize;

    my $size;

    my $total = 0;


    my $maxread = 1024000;
    #my $maxread = 1024;


    for my $part (0..$noofparts) {

        # example = 825

        # 299 content

        if (($fsize-$total) > $maxchunksize) {
          # $size = $maxchunksize + 6 * (length $boundary) + 6 * (2) + 21 * 2 + 2 + (length basename($filename)) + 299 + length ($maxchunksize) + length ($maxchunksize) + length($fsize) + 18;
          $size = $maxchunksize + 6 * (length $boundary) + 6 * 2 + 21 * 2 + 2 + (length basename($filename)) + 299 + length ($maxchunksize) + length ($maxchunksize) + length($fsize) + length($part);
        } else {
          $size = ($fsize-$total) + 6 * (length $boundary) + 6 * 2 + 21 * 2 + 2 + (length basename($filename)) + 299 + length ($maxchunksize) + length ($fsize-$total) + length($fsize) + length($part);
        }





        # for manual test
        #$size = 826;

        print Dumper "sizes";
        print Dumper "#######";
        print Dumper $size;
        print Dumper $total;
        print Dumper "#######";

        #$boundary = "-------------------------------36497293608260705052636266395";

        my $h = HTTP::Headers->new(
          Content_Length      => $size,
          Connection          => 'keep-alive',
          Content_Type        => 'multipart/form-data; boundary=' . $boundary
        );

        my $request = HTTP::Request->new(
          POST => $api_url, $h
        );



        $request->protocol('HTTP/1.1');

        # Perl $HTTP::Request::Common::DYNAMIC_FILE_UPLOAD = 1 allows to load any file size
        # without loading all in memory but it's very slow as it's using 2k chunks
        # content provider procedure is develop instead of using DYNAMIC_FILE_UPLOAD
        # and it's providing a content of multipart/form-data request
        # it's simple implementation and probably not a best one

        print Dumper "HIPCIO";

        my $content_provider_ref = &content_provider_60($fh, $filename, $size, $boundary, $self, $fsize, $part+1, $maxchunksize, \$total);
        $request->content($content_provider_ref);


        sub content_provider_60 {
          my $fh = shift;
          my $filename = shift;
          my $size = shift;
          my $boundary = shift;
          my $self = shift;
          my $fsize = shift;
          my $part = shift;
          my $maxchunksize = shift;
          my $total = shift;
          # we need to send 4 parts - a boundary start, content description, file content, boundary end
          my @content_part = ( 'a', 'b', 'c' );
          my $report = 0;
          my $end = 0;
          my $end2 = 0;
          my $real  = 0;
          my $chunksize = 0;
          $| = 1;

          # print Dumper "entry to content_provider_60";
          # print Dumper $part;
          # print Dumper $maxchunksize;

          return sub {
            my $buf;

            #print Dumper \@content_part;

            if (!defined($content_part[0])) {
              print Dumper "Kaniex";
              return undef;
            }

            # print Dumper $content_part[0];
            # print Dumper "----";
            # print Dumper $total;

            if ($content_part[0] eq 'a') {
              # each bondary has -- at the beginning - not sure why
              $buf = "--" . $boundary. "\r\n";
              $buf = $buf . "Content-Disposition: form-data; name=\"file\"; filename=\"" . basename($filename) . "\"\r\n";
              $buf = $buf . "Content-Type: application/octet-stream\r\n\r\n";
              # print Dumper $buf;
              # print Dumper "HIPCIO PO A";
              shift @content_part;
            } elsif ($content_part[0] eq 'b') {
              # set file position
              my $rc = sysread($fh, $buf, $maxread);
              ${$total} = ${$total} + length $buf;
              $chunksize = $chunksize + length $buf;
              # print Dumper $rc;
              if ((${$total} / $fsize * 100) > $report) {
                if ((${$total} / $fsize * 100) eq 100) {
                  #printf "%5.1f\n ", 100;
                  $end = 1;
                } else {
                  #printf "%5.1f - ", $total / $fsize * 100;
                  $report = $report + 10;
                }

              }

              if (${$total} eq ($part*$maxchunksize)) {
                # print Dumper "total chunk size reach";
                # print Dumper "#######";
                # print Dumper $part;
                # print Dumper ${$total};
                # print Dumper "#######";
                shift @content_part;
              }

              if ($rc ne $maxread) {
                # print Dumper "rc not maxread";
                shift @content_part;
              }
            } elsif ($content_part[0] eq 'c') {
              $buf = "\r\n" . "--" . $boundary. "\r\n";
              $buf = $buf . "Content-Disposition: form-data; name=\"_chunkSize\"\r\n\r\n";
              $buf = $buf . $maxchunksize . "\r\n";
              $buf = $buf . "--" . $boundary. "\r\n";
              $buf = $buf . "Content-Disposition: form-data; name=\"_currentChunkSize\"\r\n\r\n";
              $buf = $buf . $chunksize. "\r\n";
              $buf = $buf . "--" . $boundary. "\r\n";
              $buf = $buf . "Content-Disposition: form-data; name=\"_chunkNumber\"\r\n\r\n";
              $buf = $buf . ($part - 1) . "\r\n";
              $buf = $buf . "--" . $boundary. "\r\n";
              $buf = $buf . "Content-Disposition: form-data; name=\"_totalSize\"\r\n\r\n";
              $buf = $buf . $fsize . "\r\n";
              $buf = $buf . "--" . $boundary. "--\r\n";
              shift @content_part;
            }

            $real = $real + length $buf;
            if ($buf eq '') {
              return undef;
            }
            # print Dumper "wyscie";
            #print Dumper $buf;
            return $buf;
          }
        }

        #print Dumper "HIPPO2";
        my $response = $self->{_ua}->request($request);
        #print Dumper "HIPPO3";
        #print Dumper $response;
        my $decoded_response;
        my $result_fmt;
        my $retcode;
        my $result;

        if ( $response->is_success ) {

           $decoded_response = $response->decoded_content;
           $result = decode_json($decoded_response);
           $result_fmt = to_json($result, {pretty=>1});
           logger($self->{_debug}, "Response message: " . $result_fmt, 2);
           if (defined($result->{status}) && ($result->{status} eq 'OK')) {
             print "\nFile upload completed without issues.\n";
             $retcode = 0;
           } elsif (defined($result->{result}) && ($result->{result} eq 'failed')) {
             print "\nFile upload issues\n";
             print "Try the operation again. If the problem persists, contact Delphix support.\n";
             $retcode = 1;
           }


        }
        else {
           logger($self->{_debug}, "HTTP POST error code: " . $response->code, 2);
           logger($self->{_debug}, "HTTP POST error message: " . $response->message, 2);
           $retcode = 1;
        }
    }
}



# Procedure getSSOToken
# parameters:
# - client_id
# - client_secret
# Connect to SSO service and get a SSO token
# 0 is all OK

sub getSSOToken {
    my $self = shift;
    my $client_id = shift;
    my $client_secret = shift;

    my $sso_provider = 'https://delphix.okta.com/oauth2/default/v1/token';

    logger($self->{_debug}, "Entering Engine::getSSOToken",1);


    my $ua = LWP::UserAgent->new(keep_alive => 1);
    $ua->agent("Delphix Perl Agent/0.1");
    $ua->ssl_opts( verify_hostname => 0 );
    $ua->timeout(15);

    my $h = HTTP::Headers->new(
      Accept              => 'application/json',
      Connection          => 'keep-alive',
      Content_Type        => 'application/x-www-form-urlencoded',
      Cache_control       => 'no-cache'
    );


    my $request = HTTP::Request->new(
      POST => $sso_provider,
      $h
    );



    $request->authorization_basic($client_id, $client_secret);
    $request->content("grant_type=client_credentials&scope=groups");


    logger($self->{_debug}, "calling " . Dumper $request->{_uri}, 2);


    my $decoded_response;
    my $result;
    my $result_fmt;
    my $token;

    my $response = $ua->request($request);

    if ( $response->is_success ) {
       $decoded_response = $response->decoded_content;
       $result = decode_json($decoded_response);
       $result_fmt = to_json($result, {pretty=>1});
       logger($self->{_debug}, "Response message: " . $result_fmt, 2);
       $token = $result->{access_token};
    }
    else {
       logger($self->{_debug}, "HTTP POST error code: " . $response->code, 2);
       logger($self->{_debug}, "HTTP POST error message: " . $response->message, 2);
    }

    return $token;

}

# this is good test
# sub content_provider_60_manual {
#   my $filename = shift;
#   my $size = shift;
#   my $boundary = shift;
#   my $self = shift;
#   my $fsize = shift;
#   $| = 1;
#   my $buf = "---------------------------------36497293608260705052636266395\r
# Content-Disposition: form-data; name=\"file\"; filename=\"Delphix_6.0.8.0_2021-05-07-22-28_Standard_Upgrade.tar\"\r
# Content-Type: application/octet-stream\r
# \r
# 123
# \r
# ---------------------------------36497293608260705052636266395\r
# Content-Disposition: form-data; name=\"_chunkSize\"\r
# \r
# 1024000000\r
# ---------------------------------36497293608260705052636266395\r
# Content-Disposition: form-data; name=\"_currentChunkSize\"\r
# \r
# 4\r
# ---------------------------------36497293608260705052636266395\r
# Content-Disposition: form-data; name=\"_chunkNumber\"\r
# \r
# 0\r
# ---------------------------------36497293608260705052636266395\r
# Content-Disposition: form-data; name=\"_totalSize\"\r
# \r
# 4\r
# ---------------------------------36497293608260705052636266395--\r\n";
#   return $buf;
# }

# End of package
1;
