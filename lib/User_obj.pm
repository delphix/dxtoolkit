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
# Program Name : User_obj.pm
# Description  : Delphix Engine User object
# It's include the following classes:
# - User_obj - class which map a Delphix Engine user API object
# Author       : Marcin Przepiorowski
# Created      : 24 Apr 2015 (v2.0.0)
#
#


package User_obj;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
use Authorization_obj;
use Group_obj;
use Databases;
use warnings;
use strict;

# constructor
# parameters
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $user_list = shift;
    my $debug = shift;
    logger($debug, "Entering User_obj::constructor",1);

    my $user;
    my %new;

    my $self = {
        _user => \$user,
        _dlpxObject => $dlpxObject,
        _user_list => $user_list,
        _new => \%new,
        _debug => $debug
    };

    bless($self,$classname);

    $self->{_authorizations} = $self->{_user_list}->{_authorizations};

    $self->{_new}->{type} = "User";

    return $self;
}



# Procedure setNames
# parameters:
# - first_name
# - last_name


sub setNames {
    my $self = shift;
    my $first_name = shift;
    my $last_name = shift;

    logger($self->{_debug}, "Entering User_obj::setNames",1);
    if ( (defined($first_name) ) && ( $first_name ne '') ) {
        $self->{_new}->{firstName} =$first_name unless ( $first_name eq '' ) ;
    }
    if ( (defined($last_name) ) && ( $last_name ne '') ) {
        $self->{_new}->{lastName} = $last_name unless  ( $last_name  eq '' ) ;
    }
}


# Procedure getAuthType
# Return:
# - Native / LDAP

sub getAuthType {
    my $self = shift;

    logger($self->{_debug}, "Entering User_obj::getAuthType",1);
    return $self->{_user}->{authenticationType};
}

# Procedure getUserType
# Return:
# - DOMAIN / 'SYSTEM'

sub getUserType {
    my $self = shift;

    logger($self->{_debug}, "Entering User_obj::getUserType",1);
    return $self->{_user}->{userType};
}


# Procedure getNames
# Return:
# - first_name
# - last_name

sub getNames {
    my $self = shift;

    logger($self->{_debug}, "Entering User_obj::getNames",1);
    my $first_name = $self->{_user}->{firstName} ? $self->{_user}->{firstName} : '';
    my $last_name =  $self->{_user}->{lastName} ? $self->{_user}->{lastName} : '';
    return $first_name, $last_name;
}


# Procedure getNames
# Return:
# - email_address
# - work_phone
# - home_phone
# - cell_phone

sub getContact {
    my $self = shift;
    logger($self->{_debug}, "Entering User_obj::getContact",1);
    my $email_address = $self->{_user}->{emailAddress} ? $self->{_user}->{emailAddress} : '';
    my $work_phone = $self->{_user}->{workPhoneNumber} ? $self->{_user}->{workPhoneNumber} : '';
    my $home_phone = $self->{_user}->{homePhoneNumber} ? $self->{_user}->{homePhoneNumber} : '';
    my $cell_phone = $self->{_user}->{mobilePhoneNumber} ? $self->{_user}->{mobilePhoneNumber} : '';
    return $email_address, $work_phone, $home_phone, $cell_phone;
}


# Procedure setContact
# parameters:
# - email_address
# - work_phone
# - home_phone
# - cell_phone
#return 0 if OK

sub setContact {
    my $self = shift;
    my $email_address = shift;
    my $work_phone = shift;
    my $home_phone = shift;
    my $cell_phone = shift;

    logger($self->{_debug}, "Entering User_obj::setContact",1);

    if ( (defined($email_address) ) && ( $email_address ne '') ) {
        $self->{_new}->{emailAddress} = $email_address;
    }

    if ( (defined($work_phone) ) && ( $work_phone ne '') ) {
        $self->{_new}->{workPhoneNumber} = $work_phone;
    }

    if ( (defined($cell_phone) ) && ( $cell_phone ne '') ) {
        $self->{_new}->{mobilePhoneNumber} = $cell_phone;
    }

    if ( (defined($home_phone) ) && ( $home_phone ne '') ) {
        $self->{_new}->{homePhoneNumber} = $home_phone;
    }

    return 0;

}

# Procedure setAuthentication
# parameters:
# Return type , details (ex LDAP )

sub setAuthentication {
    my $self = shift;
    my $type = shift;
    my $details = shift;

    logger($self->{_debug}, "Entering User_obj::setAuthentication",1);

    my $user = $self->{_user};

    if ($type eq 'NATIVE') {
        $self->{_new}->{authenticationType} = 'NATIVE';
        $self->{_new}->{credential}->{type} = 'PasswordCredential';
        $self->{_new}->{credential}->{password} = $details;
    }
    elsif ($type eq 'LDAP') {
        $self->{_new}->{authenticationType} = 'LDAP';
        $self->{_new}->{principal} = $details;
    }

    return ($type, $details);
}


# Procedure updateUser
# parameters:
# Return 0 if user has been updated

sub updateUser {
    my $self = shift;
    my $reference = $self->{_user}->{reference};

    logger($self->{_debug}, "Entering User_obj::updateUser",1);

    my $operation = "resources/json/delphix/user/" . $reference;
    logger($self->{_debug}, $operation, 2);

    my $json_data = to_json($self->{_new});

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $self->{_user_list}->getUserList();
        return 0;
    } else {
        return 1;
    }
}

# Procedure updatePassword
# parameters:
# Return 0 if user has been updated

sub updatePassword {
    my $self = shift;
    my $newpass = shift;
    my $reference = $self->{_user}->{reference};

    logger($self->{_debug}, "Entering User_obj::updatePassword",1);

    my $operation = "resources/json/delphix/user/" . $reference . "/updateCredential";
    logger($self->{_debug}, $operation, 2);

    if ($self->{_user}->{authenticationType} ne 'NATIVE') {
        print "Password change is allowed for non-LDAP users only \n";
        return 1;
    }


    my %password = (
        "type" => "CredentialUpdateParameters",
        "newCredential" => {
            "type" => "PasswordCredential",
            "password" => $newpass
        }
    );

    my $json_data = to_json(\%password);

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $self->{_user_list}->getUserList();
        return 0;
    } else {
        return 1;
    }
}


# Procedure deleteUser
# parameters:
# Return 0 if user has been deleted

sub deleteUser {
    my $self = shift;
    my $reference = $self->{_user}->{reference};

    logger($self->{_debug}, "Entering User_obj::deleteUser",1);

    my $operation = "resources/json/delphix/user/" . $reference . "/delete";
    logger($self->{_debug}, $operation, 2);

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, "{}");

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $self->{_user_list}->getUserList();
        return 0;
    } else {
        return 1;
    }
}


# Procedure disableUser
# parameters:
# Return 0 if user has been disabled

sub disableUser {
    my $self = shift;
    my $reference = $self->{_user}->{reference};

    logger($self->{_debug}, "Entering User_obj::disableUser",1);

    my $operation = "resources/json/delphix/user/" . $reference . "/disable";
    logger($self->{_debug}, $operation, 2);

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, "{}");

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        return 0;
    } else {
        return 1;
    }
}

# Procedure enableUser
# parameters:
# Return 0 if user has been disabled

sub enableUser {
    my $self = shift;
    my $reference = $self->{_user}->{reference};

    logger($self->{_debug}, "Entering User_obj::enableUser",1);

    my $operation = "resources/json/delphix/user/" . $reference . "/enable";
    logger($self->{_debug}, $operation, 2);

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, "{}");

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        return 0;
    } else {
        return 1;
    }
}

# Procedure createUser
# parameters:
# - Username
# - usertype - DOMAIN/SYSTEM
# Return 0 if user has been updated

sub createUser {
    my $self = shift;
    my $username = shift;
    my $usertype = shift;

    logger($self->{_debug}, "Entering User_obj::createUser",1);


    if ( defined ($self->{_user_list}->getUserByName($username, $usertype)) ) {
        return 1;
    }

    my $operation = "resources/json/delphix/user";
    logger($self->{_debug}, $operation, 2);

    $self->{_new}->{name} = $username;
    my $json_data = to_json($self->{_new});

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $self->{_new}->{reference} = $result->{result};
        $self->{_user} = $self->{_new};

        $self->{_user_list}->getUserList();
        #$self->getAuthorizationList($self->{_debug});
        return 0;
    } else {
        logger($self->{_debug}, $result->{error}->{action}, 2);
        print "User $username not created. Error: ";
        print $result->{error}->{action} . "\n";
        return 1;
    }

}

# Procedure isAdmin
# parameters:
# Return S if User is Delphix Engine Sysadmin
# Y for Delphix Admin
# and N for normal user for specific user reference

sub isAdmin {
    my $self = shift;
    my $reference = $self->{_user}->{reference};

    logger($self->{_debug}, "Entering User_obj::isAdmin",1);

    my $authorizations = $self->{_authorizations};

    if ($self->{_user}->{userType} eq "SYSTEM") {
      return 'S';
    } else {
      if (defined($authorizations->isEngineAdmin($reference))) {
        return 'Y';
      } else {
        return 'N';
      }
    }

}

# Procedure setAdmin
# parameters:
# - flag - yes/no
# Return 0 if action is completed without error


sub setAdmin {
    my $self = shift;
    my $flag = shift;

    my $reference = $self->{_user}->{reference};


    logger($self->{_debug}, "Entering User_obj::setAdmin",1);

    my $authorizations = $self->{_authorizations};
    my $current_state = $authorizations->isEngineAdmin($reference);

    if ( uc $flag eq 'Y' ) {
        if ( defined($current_state) ) {
            logger($self->{_debug}, "User is already Delphix Engine admin. ",0);
            return 0;
        } else {
            if ( $authorizations->setAuthorisation($reference,'OWNER','DOMAIN') ) {
                return 1;
            } else {
                return 0;
            }

        }
    }

    if ( uc $flag eq 'N' ) {
        if ( defined($current_state) ) {
            if ( $authorizations->deleteAuthorisation($current_state) ) {
                return 1;
            } else {
                return 0;
            }
        } else {
            logger($self->{_debug}, "User is not Delphix Engine admin. ",0);
            return 0;
        }
    }


}


# Procedure isJS
# parameters:
# Return 1 if User is Jet Stream User for specific user reference

sub isJS {
    my $self = shift;
    my $reference = $self->{_user}->{reference};

    logger($self->{_debug}, "Entering User_obj::isJS",1);

    my $authorizations = $self->{_authorizations};
    return $authorizations->isJS($reference);

}

# Procedure setJS
# parameters:
# - flag - yes/no
# Return 0 if action is completed without error


sub setJS {
    my $self = shift;
    my $flag = shift;

    my $reference = $self->{_user}->{reference};


    logger($self->{_debug}, "Entering User_obj::setJS",1);


    my $authorizations = $self->{_authorizations};
    my $current_state = $authorizations->isJS($reference);

    if ( uc $flag eq 'Y' ) {
        if ( defined($current_state) ) {
            logger($self->{_debug}, "User is already Jet Stream User. ",0);
            return 0;
        } else {
            if ( $authorizations->setAuthorisation($reference,'Jet Stream User',$reference) ) {
                return 1;
            } else {
                return 0;
            }

        }
    }

    if ( uc $flag eq 'N' ) {
        if ( defined($current_state) ) {
            if ( $authorizations->deleteAuthorisation($current_state) ) {
                return 1;
            } else {
                return 0;
            }
        } else {
            logger($self->{_debug}, "User is not Jet Stream User. ",0);
            return 0;
        }
    }


}


# Procedure setProfile
# parameters:
# Return 0 if OK 1 otherwise

sub setProfile {
    my $self = shift;
    my $target_type = shift;
    my $target_name = shift;
    my $role_name = shift;

    logger($self->{_debug}, "Entering User_obj::setProfile",1);

    my $reference = $self->{_user}->{reference};

    my $databases;
    my $groups;
    my %profile;

    if (defined($self->{_databases}) ) {
        $databases = $self->{_databases};
    } else {
        $databases = new Databases($self->{_dlpxObject},$self->{_debug});
        $self->{_databases} = $databases;
    }

    if (defined($self->{_groups}) ) {
        $groups = $self->{_groups};
    } else {
        $groups = new Group_obj($self->{_dlpxObject},$self->{_debug});
        $self->{_groups} = $groups;
    }

    my $authorizations = $self->{_authorizations};
    my $target_ref;

    my $existing_profile = $self->getProfile();

    my @exiting_auths;

    if ($target_type eq 'group') {
        if (defined($groups->getGroupByName($target_name))) {
          $target_ref = $groups->getGroupByName($target_name)->{reference};
        } else {
          print "Group $target_name not found. ";
          return 1;
        }

        for my $item (sort ( @{$existing_profile->{'groups'} } ) ) {
          if ($item->{"name"} eq $target_name) {
            push(@exiting_auths, $item->{'authref'});
          }
        }

    } elsif ($target_type eq 'databases' ) {
        my $db_obj = $databases->getDBByName($target_name);
        if (scalar(@{$db_obj}) > 0 ) {
            $target_ref = $db_obj->[0]->getReference();
        } else {
          print "Database $target_name not found. ";
          return 1;
        }

        for my $item (sort ( @{$existing_profile->{'databases'} } ) ) {
          if ($item->{"name"} eq $target_name) {
            push(@exiting_auths, $item->{'authref'});
          }
        }

    } else {
      print "Target type $target_type not found. ";
      return 1;
    }


    if (lc $role_name eq 'none') {
      if (scalar(@exiting_auths) < 1) {
        print "User doesn't have any role assigned to $target_type named $target_name. ";
        return 1;
      }
    }

    my $ret = 0;

    for my $auth_ref (@exiting_auths) {
      $ret = $ret + $authorizations->deleteAuthorisation($auth_ref);
    }

    if (lc $role_name ne 'none') {
      $ret = $ret + $authorizations->setAuthorisation($self->{_user}->{reference}, $role_name, $target_ref);
    }

    return $ret;
}

# Procedure getProfile
# parameters:
# Return hash of user profile ( type - db name - role ) for particular user;

sub getProfile {
    my $self = shift;
    my $ref = shift;

    logger($self->{_debug}, "Entering User_obj::getProfile",1);

    my $reference = $self->{_user}->{reference};

    my $databases;
    my $groups;
    my %profile;

    if (defined($self->{_databases}) ) {
        $databases = $self->{_databases};
    } else {
        $databases = new Databases($self->{_dlpxObject},$self->{_debug});
        $self->{_databases} = $databases;
    }

    if (defined($self->{_groups}) ) {
        $groups = $self->{_groups};
    } else {
        $groups = new Group_obj($self->{_dlpxObject},$self->{_debug});
        $self->{_groups} = $groups;
    }

    my $authorizations = $self->{_authorizations};


    my $obj_list = $authorizations->getDatabasesByUser($reference);

    my @grouparray;
    my @databasearray;

    for my $obj ( @{$obj_list} ) {

        my $obj_ref = $obj->{'obj_ref'};

        if (defined($groups->getName($obj_ref)) && ($groups->getName($obj_ref) ne 'N/A')  ) {
          my %authobj;
          $authobj{'name'} = $groups->getName($obj_ref);
          $authobj{'authref'} = $obj->{'authref'};
          $authobj{'rolename'} = $obj->{'name'};
          push(@grouparray, \%authobj);
        }
        if (defined($databases->getName($obj_ref))) {
          my %authobj;
          $authobj{'name'} = $databases->getName($obj_ref);
          $authobj{'authref'} = $obj->{'authref'};
          $authobj{'rolename'} = $obj->{'name'};
          push(@databasearray, \%authobj);
        }
    }

    @grouparray = sort { $a->{'name'} cmp $b->{'name'} } @grouparray;
    @databasearray = sort { $a->{'name'} cmp $b->{'name'} } @databasearray;


    $profile{'groups'} = \@grouparray;
    $profile{'databases'} = \@databasearray;
    return \%profile;
}

# Procedure getAuthentication
# parameters:
# Return type , principal, password

sub getAuthentication {
    my $self = shift;

    logger($self->{_debug}, "Entering User_obj::getAuthentication",1);

    my $type;
    my $password = '';
    my $principal = '';
    my $user = $self->{_user};

    if ($user->{authenticationType} eq 'NATIVE') {
        $type = 'NATIVE';
        $password = 'password';
    }
    elsif ($user->{authenticationType} eq 'LDAP') {
        $type = 'LDAP';
        $principal = '"' . $user->{principal} . '"';
    }

    return ($type, $principal, $password);
}

# Procedure getName
# parameters:
# Return user name for specific user object

sub getName {
    my $self = shift;

    logger($self->{_debug}, "Entering User_obj::getName",1);

    return $self->{_user}->{name};
}

# Procedure getReference
# parameters:
# Return user reference for specific user object

sub getReference {
    my $self = shift;

    logger($self->{_debug}, "Entering User_obj::getReference",1);

    return $self->{_user}->{reference};
}


# Procedure getStatus
# parameters:
# Return user status

sub getStatus {
    my $self = shift;

    logger($self->{_debug}, "Entering User_obj::getStatus",1);
    my $ret;

    if (defined($self->{_user}->{enabled})) {
      if ($self->{_user}->{enabled}) {
        $ret = "enabled";
      } else {
        $ret = "disabled";
      }
    } else {
      $ret = 'N/A';
    }

    return $ret;
}

# Procedure setSysadmin
# parameters:


sub setSysadmin {
    my $self = shift;

    logger($self->{_debug}, "Entering User_obj::setSysadmin",1);
    my $ret;

    $self->{_new}->{userType}="SYSTEM";

}


# Procedure getLastLogin
# parameters:
# Return user last sucessful login

sub getLastLogin {
    my $self = shift;

    logger($self->{_debug}, "Entering User_obj::getLastLogin",1);
    my $ret;

    my $operation = "resources/json/delphix/action?fromDate=2000-01-01T00%3A00%3A00.000Z&pageSize=1&searchText=Log%20in%20as%20user&sortBy=reference&user=" . $self->{_user}->{reference};
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        if (scalar(@res)>1) {
          print "Too many entries. Last login returned more than 1 row\n";
          $ret = "Error";
        } else {
          if (defined($res[0]->{startTime})) {
            $ret = Toolkit_helpers::convert_from_utc($res[0]->{startTime},$self->{_dlpxObject}->getTimezone(),1);
          } else {
            $ret = "N/A";
          }
        }


    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }

    return $ret;
}

1;
