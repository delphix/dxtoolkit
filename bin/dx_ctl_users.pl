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
# Program Name : dx_ctl_users.pl
# Description  : Get database and host information
# Author       : Marcin Przepiorowski
# Created      : 22 Apr 2015 (v2.0.0)
#


use strict;
use warnings;
use JSON;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev); #avoids conflicts with ex host and help
use File::Basename;
use Pod::Usage;
use FindBin;
use Data::Dumper;
use Text::CSV;

my $abspath = $FindBin::Bin;

use lib '../lib';
use Engine;
use User_obj;
use Formater;
use Toolkit_helpers;
use Users;

sub process_profile;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'file|f=s' => \(my $file),
  'profile=s' => \(my $profile),
  'all' => (\my $all),
  'version' => \(my $print_version),
  'dever=s' => \(my $dever),
  'debug:n' => \(my $debug),
) or pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);

pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   

my $engine_obj = new Engine ($dever, $debug);
my $path = $FindBin::Bin;
my $config_file = $path . '/dxtools.conf';

$engine_obj->load_config($config_file);

if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
  exit (1);
}


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $FD;



for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  if (defined($file)) {
    process_user($engine_obj, $file);
  }

  if (defined($profile)) {
    process_profile($engine_obj, $profile);
  }


}


######################################################

sub process_user {
  my $engine_obj = shift;
  my $file = shift;

  my @csv;

  if (defined($file)) {
    open($FD,$file) or die("Can't open file $file $!" );
    @csv = <$FD>;
    close $FD;
  }

  # load objects for current engine
  my $users_obj = new Users ($engine_obj, undef, $debug);

  my $csv_obj = Text::CSV->new({sep_char=>','});

  for my $line (@csv) {
    
    if ($line =~ /^#/) {
      next;
    }

    my ($command, $username,$firstname,$lastname,$email,$workphone,$homephone,$mobilephone,$authtype,$principal,$password,$is_admin, $is_JS);

    if ($csv_obj->parse($line)) {
      ($command, $username,$firstname,$lastname,$email,$workphone,$homephone,$mobilephone,$authtype,$principal,$password,$is_admin, $is_JS) = $csv_obj->fields();
    } else {
      print "Can't parse line : $line \n";
      next;
    }

    my $user = $users_obj->getUserByName($username);

    if (lc $command eq 'c') { 
    
      if ( ! defined($user) ) {

        if ((uc $is_admin eq 'Y') && (uc $is_JS eq 'Y')) {
          print "User $username can't be Delphix Admin and Jet Stream user only at same time. Skipping \n";
          next;
        }

        my $newuser = new User_obj($engine_obj, $users_obj, $debug);
        $newuser->setNames($firstname, $lastname);

        $newuser->setContact($email, $workphone, $homephone, $mobilephone);
        if (lc $authtype eq 'native') {
          $newuser->setAuthentication('NATIVE',$password);
        }
        if (lc $authtype eq 'ldap') {
          $newuser->setAuthentication('LDAP',$principal);
        }

        if ($newuser->createUser($username)) {
          print "User $username not created. Run with -debug flag. \n";
        } else {
          print "User $username created. ";
          $newuser->setAdmin(uc ($is_admin));
          $newuser->setJS(uc ($is_JS));
          print "\n";
        }

      } else {
          print "User $username exist. Skipping \n";
      }

    }

    if (lc $command eq 'u') {
      if (defined($user) ) {    

        if ((uc $is_admin eq 'Y') && (uc $is_JS eq 'Y')) {
          print "User $username can't be Delphix Admin and Jet Stream user only at same time. Skipping \n";
          next;
        }

        $user->setNames($firstname, $lastname);
        $user->setContact($email, $workphone, $homephone, $mobilephone);

        my $isadminYN = $user->isAdmin() ? 'Y' : 'N';

        if ($isadminYN ne (uc $is_admin)) {
          print "Set Delphix Admin to $is_admin .";
          $user->setAdmin(uc ($is_admin));
        }

        my $isJSYN = $user->isJS() ? 'Y' : 'N';

        if ($isJSYN ne (uc $is_JS)) {
          print "Set Jet Stream user to $is_JS .";
          $user->setJS(uc ($is_JS));
        }

        if ($user->updateUser() ) {
          print "Problem with update. \n";
        } else {
          print "User $username updated. ";
        }
        if ($password ne '') {
          if ($user->updatePassword($password)) {
            print "Problem with password update. \n";
          } else {
            print "Password for user $username updated. ";
          }
        }
        print "\n";
      } else {
        print "User $username doens't exist. Can't update\n";
      }
    }
    if (lc $command eq 'd') {
      if (defined($user) ) {    
        if ($user->deleteUser() ) {
          print "Problem with delete. \n";
        } else {
          print "User $username deleted. \n";
        }

      }
      else {
        print "User $username doens't exist. Can't delete\n";
      }
    }

  }

}

sub process_profile {
  my $engine_obj = shift;
  my $profile = shift;

  my @csv;

  if (defined($profile) && ($profile ne '')) {
    my $FDPROF;
    open($FDPROF,$profile) or die("Can't open file $profile $!" );
    @csv = <$FDPROF>;
    close $FDPROF;
  }

  # load objects for current engine
  my $users_obj = new Users ($engine_obj);

  my $csv_obj = Text::CSV->new({sep_char=>','});

  for my $line (@csv) {
    
    if ($line =~ /^#/) {
      next;
    }

    my ($username,$target_type,$target_name,$role);

    if ($csv_obj->parse($line)) {
      ($username,$target_type,$target_name,$role) = $csv_obj->fields();
    } else {
      print "Can't parse line : $line \n";
      next;
    }

    my $user = $users_obj->getUserByName($username);

    if (defined($user)) {
      if ($user->setProfile($target_type,$target_name,$role)) {
        print "Problem with setting profile for user $username \n";
      } else {
        print "Role $role for target $target_name set for $username\n";
      }
    } else {
      print "User $username doesn't exist. Can't set profile.\n";
    }

  }
  


}

__DATA__


=head1 SYNOPSIS

 dx_ctl_users.pl [ -engine|d <delphix identifier> | -all ] [-file filename] [-profile filename] [ --help|? ] [ -debug ]

=head1 DESCRIPTION

Control an users in Delphix Engine using a CSV file

=head1 ARGUMENTS

=head2 Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=head2 Options

=over 4

=item B<-file filename >
CSV file name with user definition and actions
Field list
command, username, firstname, lastname, email, workphone, homephone, mobilephone, authtype, principal, password, is_admin

=item B<-profile filename >
CSV file name with user profile definition. It can be generated using dx_get_users profile option. 
Field list
username, target_type, target_name, role


=back

=head1 OPTIONS

=over 2

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging


=back


=cut



