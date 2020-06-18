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
# Program Name : Users.pm
# Description  : Delphix Engine User object
# It's include the following classes:
# - Users - class which map a Delphix Engine user API object
# Author       : Marcin Przepiorowski
# Created      : 24 Apr 2015 (v2.0.0)
#
#

package Users;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
use User_obj;


# constructor
# parameters
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $databases = shift;
    my $debug = shift;
    logger($debug, "Entering Users::constructor",1);

    my %users;
    my $self = {
        _users => \%users,
        _dlpxObject => $dlpxObject,
        _databases => $databases,
        _debug => $debug
    };

    bless($self,$classname);

    my $authorizations = new Authorization_obj($dlpxObject,$debug);

    $self->{_authorizations} = $authorizations;

    $self->{_currentuser} = '';

    $self->getUserList($debug);
    return $self;
}


# Procedure getUserByName
# parameters:
# - name
# Return user reference for particular user name

sub getUserByName {
    my $self = shift;
    my $name = shift;
    my $usertype = shift;
    logger($self->{_debug}, "Entering Users::getUserByName",1);
    my $ret;

    if (!defined($usertype)) {
      my $loginuser = $self->getCurrentUser();
      $usertype = $loginuser->{userType};
    }

    my @userpertype = grep { $self->{_users}->{$_}->{_user}->{userType} eq $usertype  } sort ( keys %{$self->{_users} } );

    for my $useritem ( @userpertype )  {
        my $user = $self->{_users}->{$useritem};
        if ( $user->getName() eq $name) {
          $ret = $user;
          next;
        }
    }

    return $ret;
}

# Procedure getAllUsersByName
# parameters:
# - name
# Return array with users obj

sub getAllUsersByName {
    my $self = shift;
    my $name = shift;

    logger($self->{_debug}, "Entering Users::getAllUsersByName",1);
    my $ret;

    # limit a list of users ref to ones matching a name parameter
    my @userrefarray = grep { $self->{_users}->{$_}->getName() eq $name } sort ( keys %{$self->{_users}} );

    return \@userrefarray;
}

# Procedure getUser
# parameters:
# - reference
# Return user hash for specific user reference

sub getUser {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Users::getUser",1);

    my $users = $self->{_users};
    return $users->{$reference};

}

# Procedure getUsers
# parameters:
# Return list of users

sub getUsers {
    my $self = shift;
    logger($self->{_debug}, "Entering Users::getUsers",1);
    return sort (keys %{$self->{_users}});
}

# Procedure getEditableUsers
# parameters:
# Return list of users from same domain as login user

sub getEditableUsers {
    my $self = shift;
    logger($self->{_debug}, "Entering Users::getEditableUsers",1);
    my $loginuser = $self->getCurrentUser();
    my $usertype = $loginuser->{userType};
    return grep { $self->{_users}->{$_}->{_user}->{userType} eq $usertype  } sort ( keys %{$self->{_users} } );;
}


# Procedure getJSUsers
# parameters:
# Return list of JS users plus delphix admin one as they can have JS objects

sub getJSUsers {
    my $self = shift;
    logger($self->{_debug}, "Entering Users::getJSUsers",1);
    my @retarray;
    for my $userref (sort (keys %{$self->{_users}})) {
      if (($self->{_users}->{$userref}->isJS()) || ($self->{_users}->{$userref}->isAdmin())) {
        push(@retarray, $userref);
      }
    }
    return \@retarray;
}


# Procedure getUsersByTarget
# parameters:
# - target ref
# Return list of users for target

sub getUsersByTarget {
    my $self = shift;
    my $target_ref = shift;
    logger($self->{_debug}, "Entering Users::getUsersByTarget",1);

    return $self->{_authorizations}->getUsersByTarget($target_ref);
}




# Procedure getDatabasesByUser
# parameters:
# - user ref
# Return list of objects per user

sub getDatabasesByUser {
    my $self = shift;
    my $username = shift;
    my $usertype = shift;

    logger($self->{_debug}, "Entering Users::getDatabasesByUser",1);
    my $user = $self->getUserByName($username, $usertype);
    if (defined($user)) {
      my $userref = $user->getReference();
      return $self->{_authorizations}->getDatabasesByUser($userref);
    } else {
      return undef;
    }
}


# Procedure getCurrentUser
# parameters:
# Return current logged user

sub getCurrentUser {
    my $self = shift;
    my $ret;

    logger($self->{_debug}, "Entering Users::getCurrentUser",1);

    if ($self->{_currentuser} eq '') {

      my $operation = "resources/json/delphix/user/current";
      my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

      if (defined($result->{status}) && ($result->{status} eq 'OK')) {
          $ret = $result->{result};
          $self->{_currentuser} = $ret;
      } else {
          print "No data returned for $operation. Try to increase timeout \n";
      }

    } else {
      $ret = $self->{_currentuser};
    }

    return $ret;

}

# Procedure getUserList
# parameters: none
# Load a list of user objects from Delphix Engine

sub getUserList
{
    my $self = shift;
    logger($self->{_debug}, "Entering Users::getUserList",1);

    delete $self->{_users};

    my $databases;
    if (defined($self->{_databases})) {
      $databases = $self->{_databases};
    } else {
      $databases = new Databases($self->{_dlpxObject},$self->{_debug});
      $self->{_databases} = $databases;
    }

    my $operation = "resources/json/delphix/user";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};


        for my $useritem (@res) {
            my $user = new User_obj($self->{_dlpxObject}, $self, $self->{_debug});
            $user->{_databases} = $databases;
            $user->{_user} = $useritem;
            $self->{_users}->{$useritem->{reference}} = $user;
        }
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}


sub addUser
{
  my $self = shift;
  my $username = shift;
  my $usertype = shift;
  my $firstname = shift;
  my $lastname = shift;
  my $email = shift;
  my $workphone = shift;
  my $homephone = shift;
  my $mobilephone  = shift;
  my $authtype = shift;
  my $principal = shift;
  my $password = shift;
  my $is_admin = shift;
  my $is_JS = shift;
  my $timeout = shift;
  my $apiuser = shift;

  my $ret = 0;
  my $user = $self->getUserByName($username, $usertype);

  if ( ! defined($user) ) {

    if (((uc $is_admin eq 'Y') || (uc $is_admin eq 'S')) && (uc $is_JS eq 'Y')) {
      print "User $username can't be Delphix Admin/sysadmin and Jet Stream user only at same time. Skipping \n";
      return 1;
    }

    if ( (! defined($email) ) || ( $email  eq '') ) {
        print "Email address is required fpr user $username\n";
        return 1;
    }

    my $newuser = new User_obj($self->{_dlpxObject}, $self, $self->{_debug});
    $newuser->setNames($firstname, $lastname);

    $newuser->setContact($email, $workphone, $homephone, $mobilephone);
    if (lc $authtype eq 'native') {
      $newuser->setAuthentication('NATIVE',$password);
    }
    if (lc $authtype eq 'ldap') {
      $newuser->setAuthentication('LDAP',$principal);
    }

    if (defined($timeout)) {
      $newuser->setTimeout($timeout);
    }

    if (defined($apiuser) ) {
      $newuser->setApiUser($apiuser);
    }


    if (uc $is_admin eq 'S') {
      $newuser->setSysadmin()
    }

    if ($newuser->createUser($username, $usertype)) {
      $ret = $ret + 1;
    } else {
      if ($newuser->setAdmin(uc ($is_admin))) {
        print "Problem with setting admin role for user $username\n";
        $ret = $ret + 1;
      };
      $newuser->setJS(uc ($is_JS));
      if ($usertype eq 'SYSTEM') {
        print "User $username with sysadmin role created\n";
      } else {
        print "User $username created. \n";
      }
    }

  } else {
      if ($user->getUserType() eq 'SYSTEM') {
        print "User $username with sysadmin role exist. Skipping \n";
      } else {
        print "User $username exist. Skipping \n";
      }

  }

  return $ret;

}


sub updateUser
{
  my $self = shift;
  my $username = shift;
  my $usertype = shift;
  my $firstname = shift;
  my $lastname = shift;
  my $email = shift;
  my $workphone = shift;
  my $homephone = shift;
  my $mobilephone  = shift;
  my $authtype = shift;
  my $principal = shift;
  my $password = shift;
  my $is_admin = shift;
  my $is_JS = shift;
  my $timeout = shift;
  my $apiuser = shift;

  my $ret = 0;

  my $user = $self->getUserByName($username, $usertype);

  if (defined($user) ) {

    if ((uc $is_admin eq 'Y') && (uc $is_JS eq 'Y')) {
      print "User $username can't be Delphix Admin and Jet Stream user only at same time. Skipping \n";
      $ret = $ret + 1;
      next;
    }

    if (defined($timeout)) {
      $user->setTimeout($timeout);
    }

    if (defined($apiuser) ) {
      $user->setApiUser($apiuser);
    }

    $user->setNames($firstname, $lastname);
    $user->setContact($email, $workphone, $homephone, $mobilephone);

    my $isadminYN = $user->isAdmin();


    if ( ($is_admin ne '') && ($isadminYN ne (uc $is_admin)) ) {
      print "Set Delphix Admin to $is_admin .";
      $user->setAdmin(uc ($is_admin));
    }

    my $isJSYN = $user->isJS() ? 'Y' : 'N';

    if ( ($is_JS ne '') && ($isJSYN ne (uc $is_JS))) {
      print "Set Jet Stream user to $is_JS .";
      $user->setJS(uc ($is_JS));
    }

    if ($user->updateUser() ) {
      $ret = $ret + 1;
    } else {
      print "User $username updated. ";
    }
    if (($ret eq 0) && ($password ne '')) {
      if ($user->updatePassword($password)) {
        $ret = $ret + 1;
      } else {
        print "Password for user $username updated. ";
      }
    }
    print "\n";
  } else {
    print "User $username doens't exist. Can't update\n";
    $ret = $ret + 1;
  }

  return $ret;
}

sub deleteUser
{
  my $self = shift;
  my $username = shift;
  my $usertype = shift;

  my $ret = 0;
  my $user = $self->getUserByName($username, $usertype);

  if (defined($user) ) {
    if ($user->deleteUser() ) {
      print "Problem with delete. \n";
      $ret = $ret + 1;
    } else {
      if ($user->getUserType() eq 'SYSTEM') {
        print "User $username with sysadmin role deleted\n";
      } else {
        print "User $username deleted. \n";
      }
      $self->getUserList();
    }

  }
  else {
    print "User $username doesn't exist in domain $usertype. Can't delete\n";
    $ret = $ret + 1;
  }

  return $ret;
}

sub lockUser
{
  my $self = shift;
  my $username = shift;
  my $usertype = shift;

  my $ret = 0;
  my $user = $self->getUserByName($username, $usertype);

  my $loginuser = $self->getCurrentUser();

  if ( $user->getReference() eq $loginuser->{reference} ) {
    # to avoid locking last user with access - GUI have same check
    print "You can't lock user you are using to log in. Skipping user $username \n";
    return 1;
  };

  if (defined($user) ) {
    if ($user->disableUser() ) {
      print "Problem with lock(disable). \n";
      $ret = $ret + 1;
    } else {
      print "User $username locked(disabled). \n";
    }

  }
  else {
    print "User $username doesn't exist. Can't locked(disabled)\n";
    $ret = $ret + 1;
  }

  return $ret;
}

sub unlockUser
{
  my $self = shift;
  my $username = shift;
  my $usertype = shift;

  my $ret = 0;
  my $user = $self->getUserByName($username, $usertype);

  if (defined($user) ) {
    if ($user->enableUser() ) {
      print "Problem with unlock(enable). \n";
      $ret = $ret + 1;
    } else {
      print "User $username unlocked(enabled). \n";
    }

  }
  else {
    print "User $username doesn't exist. Can't unlock(enable)\n";
    $ret = $ret + 1;
  }

  return $ret;
}

sub setSSHkey
{
  my $self = shift;
  my $username = shift;
  my $usertype = shift;
  my $sshfile = shift;

  my $ret = 0;
  my $user = $self->getUserByName($username, $usertype);

  if (defined($user) ) {
    my $FD;
    open($FD,$sshfile) or die("Can't open file $sshfile $!" );
    my @sshkeylines = <$FD>;
    close $FD;
    $user->setSSHkey(\@sshkeylines);
    if ($user->updateUser() ) {
      print "Problem with ssh key setting. \n";
      $ret = $ret + 1;
    } else {
      print "SSH key(s) for $username set. \n";
    }

  }
  else {
    print "User $username doesn't exist. Can't set SSH key\n";
    $ret = $ret + 1;
  }

  return $ret;
}

1;
