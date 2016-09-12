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
#
# Program Name : MC.pm
# Description  : Mission control common procedures
# Author       : Marcin Przepiorowski
# Created      : 20 Apr 2016 (v2.2.4)
#




package MissionControl;
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



use lib '../lib';

# constructor
# parameters 
# - debug - debug flag (debug on if defined)

sub new
{
   my $class = shift;
   my $url = shift;
   my $user = shift;
   my $password = shift;
   my $debug = shift;
   my $ua;

   logger($debug,"Entering MC::constructor",1);
   $ua = LWP::UserAgent->new;
   $ua->agent("Delphix Perl Agent/0.1");
   $ua->ssl_opts( verify_hostname => 0 );
   $ua->timeout(15);
   #$ua->cookie_jar( {} );

   my $self = {
      _debug => $debug,
      _url => $url,
      _ua => $ua
   };

   bless $self, $class;


   $self->login($user, $password);


   return $self;
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


   my $api_url =  $self->{_url} . "/$operation";
  

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
      logger($self->{_debug}, "Response message: " . $result_fmt, 2);
      $retcode = 1;
   }

   return ($result,$result_fmt, $retcode);
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

   my $api_url =  $self->{_url} . "/$operation";

   logger($self->{_debug}, "$api_url");

   my $request = HTTP::Request->new(POST => $api_url);
   $request->content_type("application/json");

   if (defined($post_data)) {
      $request->content($post_data);
   }

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
      logger($self->{_debug}, "Response message: " . $result_fmt, 2);
      $retcode = 1;
   }


   return ($result,$result_fmt, $retcode);
}


sub login {
   my $self = shift;
   my $user = shift;
   my $password = shift;

   my $operation = "api/login";

   my %cred = (
       'password'=>$password,
       'user'=>$user
     );

   my $post_data = to_json(\%cred, {pretty=>1});

   my ($result,$result_fmt, $retcode) = $self->postJSONData($operation, $post_data);

   if ($retcode) {
      print "Problem with login. Check url, user and password. \n";
      exit -1;
   }

   my $userId;
   my $loginToken;

   if (defined($result->{userId})) {
      $userId = $result->{userId};
      $loginToken = $result->{loginToken};
      $self->{_ua}->default_header( 'X-User-Id' => $userId );
      $self->{_ua}->default_header( 'X-Login-Token' => $loginToken );
   } else {
      print "Problem with setting up userId and token. \n";
      exit -1;   
   }

}


sub generate_config {
   my $self = shift;
   my $de_user = shift;
   my $de_password = shift;

   my @engine_list;

   my ($result,$result_fmt, $retcode) = $self->getJSONResult('api/list_engines');
   if (defined($result)) {

      for my $engitem (@{$result}) {
         if (defined($engitem->{_id})) {
            print Dumper $engitem->{_id};
      
            my %engine = (
             hostname => $engitem->{_id},
             username => $de_user,
             ip_address => $engitem->{_id},
             password => $de_password,
             port => 80,
             default => JSON::false,
             timeout => 60,
             protocol => 'http',
             encrypted => JSON::false
            );

            push (@engine_list, \%engine);



         }

      }

   }

   print Dumper \@engine_list;

   
}


# End of package
1;