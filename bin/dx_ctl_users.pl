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

my $action = "import";

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'file|f=s' => \(my $file),
  'profile=s' => \(my $profile),
  'action=s'  => \($action),
  'username=s' => \(my $username),
  'password=s' => \(my $password),
  'timeout=n'  => \(my $timeout),
  'sshkeyfile=s'  => \(my $sshkeyfile),
  'force' => \(my $force),
  'all' => (\my $all),
  'version' => \(my $print_version),
  'dever=s' => \(my $dever),
  'debug:n' => \(my $debug),
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

my $line;

if ((lc $action ne 'import') && (defined($file) || defined($profile))) {
  print "Parameter -file or -profile are supported only with action import or without -action parameter\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (lc $action eq 'import') {

  if ((!defined($file)) && (!defined($profile))) {
    print "Parameter -file or -profile is required\n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }

} elsif ((lc $action eq 'lock') || (lc $action eq 'unlock') || (lc $action eq 'password')  || (lc $action eq 'timeout')
        || (lc $action eq 'sshkey') ) {

  if (!defined($username)) {
    print "Parameter -username is required for action $action\n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }

  if ((lc $action eq 'timeout') && !defined($timeout)) {
    print "Parameter -timeout is required for action $action\n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }

  if ((lc $action eq 'sshkey') && !defined($sshkeyfile)) {
    print "Parameter -sshkeyfile is required for action $action\n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }

} else {
  print "Unknown action $action\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $FD;

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    $ret = $ret + 1;
    next;
  };

  my $jscontainers;

  if (defined($force)) {
    # remove JS container ownership
    $jscontainers = new JS_container_obj ( $engine_obj, undef, $debug);
  }

  if (defined($file) || defined($username)) {
    if (lc $action eq 'sshkey') {
      my $users_obj = new Users ($engine_obj, undef, $debug);
      my $loginuser = $users_obj->getCurrentUser();
      $ret = $ret + $users_obj->setSSHkey($username, $loginuser->{userType}, $sshkeyfile);
    } else {
      $ret = $ret + process_user($engine_obj, $file, $action, $username, $jscontainers, $timeout);
    }
  }

  if (defined($profile)) {
    process_profile($engine_obj, $profile);
  }


}

exit $ret;


######################################################

sub process_user {
  my $engine_obj = shift;
  my $file = shift;
  my $action = shift;
  my $username = shift;
  my $jscontainers = shift;
  my $timeout = shift;

  my @csv;

  my $ret=0;



  # load objects for current engine
  my $users_obj = new Users ($engine_obj, undef, $debug);


  if (defined($file)) {
    open($FD,$file) or die("Can't open file $file $!" );
    @csv = <$FD>;
    close $FD;
  } else {

    my @userarray;

    if (lc $username eq 'all') {
      @userarray = map { $users_obj->getUser($_)->getName() } $users_obj->getEditableUsers();
    } else {
      push(@userarray, $username);
    }

    for my $useritem (@userarray) {
      my $line;
      if (lc $action eq 'lock') {
        $line = 'L,' . $useritem;
      } elsif (lc $action eq 'unlock') {
        $line = 'E,' . $useritem;
      } elsif (lc $action eq 'password') {
        if (!defined($password)) {
          $password = $engine_obj->read_password();
          print "\n";
        }
        $line = 'U,' . $useritem . ',,,,,,,,,' . $password . ',,';
      } elsif (lc $action eq 'timeout') {
        $line = 'U,' . $useritem . ',,,,,,,,,,,,' . $timeout;
      }
      push(@csv, $line);
    }
  }



  my $csv_obj = Text::CSV->new({sep_char=>',', allow_whitespace => 1});

  for my $line (@csv) {

    if ($line =~ /^#/) {
      next;
    }

    my ($command, $username,$firstname,$lastname,$email,$workphone,$homephone,$mobilephone,$authtype,$principal,$password,$is_admin, $is_JS, $timeout);

    if ($csv_obj->parse($line)) {
      ($command, $username,$firstname,$lastname,$email,$workphone,$homephone,$mobilephone,$authtype,$principal,$password,$is_admin, $is_JS, $timeout) = $csv_obj->fields();
    } else {
      print "Can't parse line : $line \n";
      $ret = $ret + 1;
      next;
    }

    if (!defined($is_admin)) {
      $is_admin = '';
    }

    if (!defined($is_JS)) {
      $is_JS = '';
    }

    my $usertype;
    my $loginuser = $users_obj->getCurrentUser();

    if ((uc $is_admin eq 'Y') || (uc $is_admin eq 'N')) {
      $usertype = 'DOMAIN';
    } elsif ($is_admin eq 'S') {
      $usertype = 'SYSTEM';
    }


    if (defined($usertype) && ($usertype ne $loginuser->{userType})) {
      print "User $username domain $usertype is differtent than login user domain " . $loginuser->{userType} . ". Skipping\n";
      $ret = $ret + 1;
      next;
    }


    if (lc $command eq 'c') {
      $ret = $ret + $users_obj->addUser($username, $usertype, $firstname,$lastname,$email,$workphone,$homephone,$mobilephone,$authtype,$principal,$password,$is_admin, $is_JS, $timeout);
    }

    if (lc $command eq 'u') {
      $usertype = $loginuser->{userType};
      $ret = $ret + $users_obj->updateUser($username, $usertype, $firstname,$lastname,$email,$workphone,$homephone,$mobilephone,$authtype,$principal,$password,$is_admin, $is_JS, $timeout);
    }
    if (lc $command eq 'd') {
      $usertype = $loginuser->{userType};
      if (defined($jscontainers)) {
        # remove JS container ownership

        my $object_list = $users_obj->getDatabasesByUser($username, $usertype);

        my @contlist = grep { $_->{obj_ref} =~ /JS_DATA_CONTAINER/ } @{$object_list};

        my $jobno;

        for my $con (@contlist) {
          $jobno = $jscontainers->removeOwner($con->{obj_ref}, $con->{userref});
          Toolkit_helpers::waitForAction($engine_obj, $jobno, "Owner $username removed", "There were problems with removing owner");
        }

      }
      $ret = $ret + $users_obj->deleteUser($username, $usertype);
    }

    if (lc $command eq 'l') {
      $ret = $ret + $users_obj->lockUser($username, $usertype);
    }

    if (lc $command eq 'e') {
      $ret = $ret + $users_obj->unlockUser($username, $usertype);
    }

  }

  return $ret;
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
  my $users_obj = new Users ($engine_obj, undef, $debug);

  my $csv_obj = Text::CSV->new({sep_char=>',', allow_whitespace => 1});

  for my $line (@csv) {

    if ($line =~ /^[\s]*#/) {
      next;
    }

    my ($username,$target_type,$target_name,$role);

    if ($csv_obj->parse($line)) {
      ($username,$target_type,$target_name,$role) = $csv_obj->fields();
      #trim objects

    } else {
      print "Can't parse line : $line \n";
      next;
    }

    my $user = $users_obj->getUserByName($username);

    if (defined($user)) {
      if ($user->setProfile($target_type,$target_name,$role)) {
        print "Problem with granting or revoking role for/from user $username \n";
      } else {
        if (lc $role eq 'none') {
          print "Role on target $target_name revoked from $username\n";
        } else {
          print "Role $role for target $target_name set for $username\n";
        }
      }
    } else {
      print "User $username doesn't exist. Can't set profile.\n";
    }

  }



}

__DATA__


=head1 SYNOPSIS

 dx_ctl_users    [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                 [-action import] <-file filename | -profile filename >
                 -action lock|unlock|password|timeout|sshkey
                 -username name|all
                 [-password password]
                 [-timeout timeout]
                 [-sshkeyfile filename]
                 [-help|?]
                 [-debug]

=head1 DESCRIPTION

Control an users in Delphix Engine using a CSV file

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

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

=head2 Options

=over 4

=item B<-action import|lock|unlock|password|sshkey>
Action for a particular user or file
Actions:

 - import (default) - read input file and run action from it parameters -file or -profile or both are required
 - lock - disable (lock) user account
 - unlock - enable (unlock) user account
 - password - change user password
 - timeout - change user timeout
 - sshkey - set a user SSH key

=item B<-username user|all>
Username for particular action.
If word 'all' is specified action will run for all users from same domain as username defined in configuration file

=item B<-password pass>
New password for user. If not specified prompt will be displayed.

=item B<-timeout time>
Update a timeout for an user. Timeout is set in minutes

=item B<-sshkeyfile filename>
File with one or more SSH public key to set for user

=item B<-file filename>
CSV file name with user definition and actions. Field list as follow:

command, username, firstname, lastname, email, workphone, homephone, mobilephone, authtype, principal, password, admin_priv, js_user, timeout

Allowed command values:

 C - create user
 D - delete user
 U - update user
 L - lock user
 E - enable user

Allowed admin_priv values:

 Y - Delphix admin user
 N - Standard User
 S - Sysadmin user

Allowed js_user values:

  Y - Self service (Jet Stream) user
  N - Standard User


=item B<-profile filename>
CSV file name with user profile definition. It can be generated using dx_get_users profile option.
To revoke existing role from user, role name should be set to None in profile file.
Allowed role names:

 - none
 - read
 - data
 - provisioner
 - owner

Field list as follow:

username, target_type, target_name, role


=back

=head1 OPTIONS

=over 2

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging


=back

=head1 EXAMPLES

Add user to one engine using example users file

 dx_ctl_users -d Landshark5 -file dxusers.csv.example
 User testuser exist. Skipping
 User testuser2 created.
 User user11 doens't exist. Can't update
 User testuser updated. Password for user testuser updated.
 User testuser2 deleted.

Add user to one engine using users file and profile file

 dx_ctl_users -d Landshark5 -file /tmp/users.csv -profile /tmp/profile.csv
 User sysadmin exist. Skipping
 User delphix_admin exist. Skipping
 User dev_admin exist. Skipping
 User qa_admin created.
 User dev exist. Skipping
 User qa exist. Skipping
 Role OWNER for target Dev Copies set for dev_admin
 Role PROVISIONER for target Sources set for dev_admin
 Role PROVISIONER for target Dev Copies set for qa_admin
 Role OWNER for target QA Copies set for qa_admin

Example csv user file:

 # operation,username,first_name,last_name,email address,work_phone,home_phone,cell_phone,type(NATIVE|LDAP),principal_credential,password,admin_priv,js_user
 # comment - create a new user with Delphix authentication
 C,testuser,Test,User,test.user@test.mail.com,,555-222-222,,NATIVE,,password,Y
 # comment - create a new user with LDAP
 C,testuser2,Test,User2,test.user@test.mail.com,555-111-111,555-222-222,555-333- 333,LDAP,"testuser@test.domain.com",,Y
 # update existing user - non-empty values will be updated, password can't be modified in this version
 U,user11,FirstName,LastName,newemail@test.com,,,,,,, U,testuser,Test,User,test.user@test.com,,555-222-333,,NATIVE,,password,Y
 # delete user
 D,testuser2,,,,,,,,,,

Example csv profile file:

 #Username,Type,Name,Role
 testusr,group,Break Fix,read
 testusr,group,QA Copies,read
 testusr,group,Sources,read
 testusr,databases,ASE pubs3 DB,owner
 testusr,databases,AdventureWorksLT2008R2,provisioner
 testusr,databases,Agile Masking,data
 testusr,databases,Employee Oracle DB,data


 Lock user

  dx_ctl_users -d Landshark5 -action lock -username testuser
  User testuser locked(disabled)

 Unlock user

  dx_ctl_users -d Landshark5 -action unlock -username testuser
  User testuser unlocked(enabled).

Change user password

  dx_ctl_users -d Landshark5 -action password -username testuser
  Password:
  User testuser updated. Password for user testuser updated.

Setting timeout for all users

  dx_ctl_users -d Landshark5 -action timeout -username all -timeout 30
  User delphix_admin updated.
  User dev updated.
  User user updated.
  User testuser updated.
  User js updated.

Force delete example - JS user own container

  dx_ctl_users -d Landshark5 -file /tmp/js.csv
  Cannot delete user "js" because that user is the owner of a Jet Stream data container.
  Problem with delete.
  User js exist. Skipping

  dx_ctl_users -d Landshark5 -file /tmp/js.csv -force
  Waiting for all actions to complete. Parent action is ACTION-20156
  Owner js removed
  User js deleted.
  User js created.

Setting a SSH key for user

 dx_ctl_users -d 53 -action sshkey -username admin -sshkeyfile /tmp/id_rsa.pub
 SSH key for admin set.

=cut
