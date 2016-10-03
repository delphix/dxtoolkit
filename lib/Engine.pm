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
use Try::Tiny;
use dbutils;

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
   $ua->agent("Delphix Perl Agent/0.1");
   $ua->ssl_opts( verify_hostname => 0 );
   $ua->timeout(15);
   #$ua->cookie_jar( {} );




   my $self = {
      _debug => $debug,
      _ua => $ua,
      _dever => $dever
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
   logger($self->{_debug}, "Entering Engine::load_config",1);

   my $data;
   my %engines;

   logger($self->{_debug}, "Loading engines from $fn",2);

   open (my $json_stream, $fn) or die ("Can't load config file $fn : $!");
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
      $engines{$name}{username}   = defined($host->{username}) ? $host->{username} : '';
      $engines{$name}{ip_address} = defined($host->{ip_address}) ? $host->{ip_address} : '';
      $engines{$name}{port}       = defined($host->{port}) ? $host->{port} : 80 ;
      $engines{$name}{default}    = defined($host->{default}) ? $host->{default} : 'false';
      $engines{$name}{protocol}   = defined($host->{protocol}) ? $host->{protocol} : 'http';
      $engines{$name}{encrypted}  = defined($host->{encrypted}) ? $host->{encrypted} : 'false'; 
      $engines{$name}{password}   = defined($host->{password}) ? $host->{password} : '';
      $engines{$name}{timeout}    = defined($host->{timeout}) ? $host->{timeout} : 60;

      if ($engines{$name}{encrypted} eq "true") {
         if ($engines{$name}{password} =~ /^#/ ) { # check if password if really encrypted
            $engines{$name}{password} = $self->decrypt($engines{$name});
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
# save configuration file (dxtools.conf) from internal structure

sub encrypt_config {
   my $self = shift;
   my $fn = shift;   
   logger($self->{_debug}, "Entering Engine::encrypt_config",1);

   my $engines = $self->{_engines};
   my @engine_list;

   for my $eng ( keys %{$engines} ) {
      if ($engines->{$eng}->{encrypted} eq 'true') {
         $engines->{$eng}->{password} = '#' . $self->encrypt($engines->{$eng});
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
   logger($self->{_debug}, "Entering Engine::encrypt",1);
   my $key = $engine->{ip_address} . $dbutils::delkey . $engine->{username};
   my $cipher = Crypt::CBC->new( 
      -key    => $key,
      -cipher => 'Blowfish',
      -iv => substr($engine->{ip_address} . $engine->{username},0,8),
      -header=>'none'
   );

   my $ciphertext = $cipher->encrypt_hex($engine->{password});
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

   my $key = $engine->{ip_address} . $dbutils::delkey . $engine->{username};
   my $cipher = Crypt::CBC->new( 
      -key    => $key,
      -cipher => 'Blowfish',
      -iv => substr($engine->{ip_address} . $engine->{username},0,8),
      -header=>'none'
   );
   my $password = substr $engine->{password}, 1;
   my $plaintext  = $cipher->decrypt_hex($password);
   return $plaintext;
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
   return $self->{_enginename};
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
# return 0 if OK, 1 if failed

sub dlpx_connect {
   my $self = shift;
   my $engine = shift;
   logger($self->{_debug}, "Entering Engine::dlpx_connect",1);

   my $dlpxObject;
   my $rc = 0;

   my %api_list = ( '4.1' => '1.4',
                    '4.2' => '1.5',
                    '4.3' => '1.6',
                    '5.0' => '1.7'
                  );

   my $engine_config = $self->{_engines}->{$engine};

   if (! defined($engine_config) ) {
      print "Can't find $engine in config file.\n";
      return 1;
   } 


   my $cookie_dir = File::Spec->tmpdir();
   my $cookie_file = File::Spec->catfile($cookie_dir, "cookies." . getOSuser() . "." . $engine  );

   my $cookie_jar = HTTP::Cookies->new(file => $cookie_file, autosave => 1, ignore_discard=>1);

   $self->{_ua}->cookie_jar($cookie_jar);

   $self->{_ua}->cookie_jar->save();

   my $osname = $^O;

   logger($self->{_debug},"Cookie file " . $cookie_file,2);

   if ( $osname ne 'MSWin32' ) {
      chmod 0600, $cookie_file or die("Can't make cookie file secure.");
   } else {
      logger($self->{_debug},"Can't secure cookie. Windows machine");
   }

   $self->{_host} = $engine_config->{ip_address};
   $self->{_user} = $engine_config->{username};
   $self->{_password} = $engine_config->{password};
   $self->{_port} = $engine_config->{port};   
   $self->{_protocol} = $engine_config->{protocol};
   $self->{_enginename} = $engine;

   undef $self->{timezone};

   logger($self->{_debug},"connecting to: $engine ( IP/name : " . $self->{_host} . " )");

   if (defined($self->{_debug})) {
      $self->{_ua}->show_progress( 1 );
   }


   my ($ses_status, $ses_version) = $self->getSession();

   if ($ses_status > 1) {
      print "Can't check session status. Engine $engine (IP: " . $self->{_host} . " ) could be down.\n";
      #logger($self->{_debug},"Can't check session status. Engine could be down.");
      return 1;
   }

   if ($ses_status) {


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
            $self->session('1.3');
            my $operation = "resources/json/delphix/about";
            my ($result,$result_fmt, $retcode) = $self->getJSONResult($operation);
            if ($result->{status} eq "OK") {
               $ses_version = $result->{result}->{apiVersion}->{major} . "." . $result->{result}->{apiVersion}->{minor};
               $self->{_api} = $ses_version;
            } else {
               logger($self->{_debug}, "Can't determine Delphix API version" );
               return 1;
            } 

         }



         if ( $self->session($ses_version) ) {
            logger($self->{_debug}, "session authentication to " . $self->{_host} . " failed.");
            $rc = 1;
         }
         else {
            if ( $self->login() ) {
               print "login to " . $self->{_host} . "  failed. \n";
               #logger($self->{_debug}, "login to " . $self->{_host} . "  failed.");
               $cookie_jar->clear();
               $rc = 1;
            } 
            else {
               logger($self->{_debug}, "login to " . $self->{_host} . "  succeeded.");
               $rc = 0;

            }
         }
   } else {
      logger($self->{_debug}, "Session exists.");  
      $self->{_api} = $ses_version; 
      $rc = 0;
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

   if (defined($version)) {
         ($major,$minor) = split(/\./,$version);
   }
   else {
         $major = 1;
         $minor = 2;
   }

   my %mysession =   
   (
      "session" => {
         "type" => "APISession",
         "version" => {
            "type" => "APIVersion",
            "major" => $major + 0,
            "minor" => $minor + 0,
            "micro" => 0
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
# check if there is a session
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
      $ver_api = $result->{result}->{version}->{major} . "." . $result->{result}->{version}->{minor};
      $ret = 0;
   }

   return ($ret, $ver_api);

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

   my %mylogin = 
   (
      "user" => {
         "type" => "LoginRequest",
         "username" => "$user",
         "password" => "$password"
      }
   );

   my $operation = "resources/json/delphix/login";
   my $json_data = encode_json($mylogin{'user'});
   logger($self->{_debug}, $json_data ,2);
   ($result,$result_fmt, $retcode) = $self->postJSONData($operation,$json_data);

   my $ret;

   if ($retcode || ($result->{status} eq 'ERROR') ) {
      $ret = 1;
   } else {
      $ret = 0;
   }

   return $ret;


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
   my $operation = "resources/json/service/configure/currentSystemTime";
   my ($result,$result_fmt, $retcode) = $self->getJSONResult($operation);
   if ($result->{result} eq "ok") {
      $time = $result->{systemTime}->{localTime};

      $time =~ s/\s[A-Z]{1,3}$//;

      if (defined($minus)) {
         
         $time = DateCalc(ParseDate($time), ParseDateDelta('- ' . $minus . ' minutes'));

      }

   } else {
      $time = 'N/A';
   } 

   return $time;

}



# Procedure checkSSHconnectivity
# parameters: 
# - minus - date current date minus minus minutes
# return timezone of Delphix engine

sub checkSSHconnectivity {
   my $self = shift;
   my $username = shift;
   my $password = shift;
   my $host = shift;

   logger($self->{_debug}, "Entering Engine::checkSSHconnectivity",1);

   my %conn_hash = (
       "type" => "SSHConnectivity",
       "address" => $host,
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

   return $ret;
}

# Procedure checkConnectorconnectivity
# parameters: 
# - minus - date current date minus minus minutes
# return timezone of Delphix engine

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
# return timezone of Delphix engine

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
   $request->content_type("text/html");

   my $response = $self->{_ua}->request($request);
   
   if ( $response->is_success ) {
      $decoded_response = $response->decoded_content;
      $result = decode_json($decoded_response);
      if (defined($self->{_debug}) && ( $self->{_debug} eq 3) ) {
         if (! -e "debug") {
            mkdir "debug" or die("Can't create root directory for debug ");
         }
         my $tempname = $operation;
         $tempname =~ s|resources/json/delphix/||;
         my @filenames = split('/', $tempname);
         if (scalar(@filenames) > 1) {
            my @dirname;
            for (my $i=0; $i<scalar(@filenames)-1; $i++) {
               @dirname = @filenames[0..$i];
               my $md = "debug/" . join('/',@dirname);
               if (! -e $md) {
                  mkdir $md or die("Can't create directory for debug " . $md);
               }
            }
            
         }
         my $filename = $tempname . ".json";
         $filename =~ s|\?|_|;
         $filename =~ s|\&|_|g;
         $filename =~ s|\:|_|g;
         print Dumper $filename;
         open (my $fh, ">", "debug/" . $filename) or die ("Can't open new debug file $filename for write");
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

   logger($self->{_debug}, "Entering Engine::generateSupportBundle",1);
   my $timeout =    $self->{_ua}->timeout();
   $self->{_ua}->timeout(60*60*24);
      

   my $operation = "resources/json/delphix/service/support/bundle/generate";
   my ($result,$result_fmt, $retcode) = $self->postJSONData($operation,'{}');

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

   logger($self->{_debug}, "Entering Engine::uploadSupportBundle",1);

   
   my %case_hash = (
       "type" => "SupportBundleUploadParameters"
   );
   
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

   logger($self->{_debug}, $post_data, 1);   

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
      $retcode = 1;
   }

   return ($result,$result_fmt, $retcode);
}





# End of package
1;