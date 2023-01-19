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
# Program Name : dx_ctl_js_container.pl
# Description  : Get Delphix Engine JS container
# Author       : Marcin Przepiorowski
# Created      : 02 Mar 2016 (v2.2.5)
#

use strict;
use warnings;
use JSON;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev); #avoids conflicts with ex host and help
use File::Basename;
use Pod::Usage;
use FindBin;
use Data::Dumper;

my $abspath = $FindBin::Bin;

use lib '../lib';
use Engine;
use Formater;
use Toolkit_helpers;
use JS_template_obj;
use JS_container_obj;
use JS_branch_obj;
use JS_datasource_obj;
use Users;
use Databases;
use Group_obj;
use Jobs_obj;


my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'container_name=s' => \(my $container_name),
  'template_name=s' => \(my $template_name),
  'branch_name=s' => \(my $branch_name),
  'bookmark_branchname=s' => \(my $full_branchname),
  'container_def=s@' => \(my $container_def),
  'container_owner=s@' => \(my $container_owners),
  'timestamp=s' => \(my $timestamp),
  'action=s' => \(my $action),
  'fromtemplate' => \(my $fromtemplate),
  'dropvdb=s' => \(my $dropvdb),
  'dontrefresh' => \(my $dontrefresh),
  'all' => (\my $all),
  'version' => \(my $print_version),
  'dever=s' => \(my $dever),
  'debug:i' => \(my $debug),
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




if (!defined($action) || ( ! ( (lc $action eq 'reset' ) || (lc $action eq 'refresh' ) || (lc $action eq 'restore' )
    || (lc $action eq 'create' ) || (lc $action eq 'delete' ) || (lc $action eq 'enable' ) || (lc $action eq 'disable' )
    || (lc $action eq 'addowner' ) || (lc $action eq 'deleteowner' ) ) ) ) {
  print "Action parameter not specified or has a wrong value\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (!defined($container_name)) {
  print "Container name is required \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    $ret = $ret + 1;
    next;
  };


  my $jstemplates = new JS_template_obj ($engine_obj, $debug );

  my $template_ref;

  if (defined($template_name)) {
    $template_ref = $jstemplates->getJSTemplateByName($template_name);
    if (! defined($template_ref)) {
      print "Can't find template $template_name \n";
      $ret = $ret + 1;
      next;
    }
  }

  my $jobno;
  my $jscontainers = new JS_container_obj ( $engine_obj, $template_ref, $debug);


  # actions for existing container
  if ((lc $action eq 'reset' ) || (lc $action eq 'refresh' ) || (lc $action eq 'restore' )
      || (lc $action eq 'delete' ) || (lc $action eq 'enable' ) || (lc $action eq 'disable' )
      || (lc $action eq 'addowner' ) || (lc $action eq 'deleteowner' ) ) {
    my $jscontainer_ref = $jscontainers->getJSContainerByName($container_name);
    if (! defined($jscontainer_ref)) {
      print "Can't find container name $container_name\n";
      $ret = $ret + 1;
      next;
    }

    if (lc $action eq 'reset') {
      $jobno = $jscontainers->resetContainer($jscontainer_ref);
    } elsif (lc $action eq 'refresh') {
      $jobno = $jscontainers->refreshContainer($jscontainer_ref);
    } elsif (lc $action eq 'restore') {
      if (!defined($timestamp)) {
        print "Timestamp is required\n";
        exit(1);
      }
      my $branch_ref;
      if (defined($fromtemplate)) {
        if (!defined($template_ref)) {
          print "Option fromtemplate require a template name to be specified\n";
          exit(1);
        }
        my $branch_obj = new JS_branch_obj ( $engine_obj, $template_ref, $debug);
        $branch_ref = $branch_obj->getJSBranchByName('master');
        $jobno = $jscontainers->restoreContainer($jscontainer_ref, $branch_ref, $timestamp, $template_ref, $full_branchname);
      } else {
        if (defined($branch_name)) {
          my $branch_obj = new JS_branch_obj ( $engine_obj, $jscontainer_ref, $debug);
          $branch_ref = $branch_obj->getJSBranchByName($branch_name);
        } else {
          $branch_ref = $jscontainers->getJSActiveBranch($jscontainer_ref);
        }
        $jobno = $jscontainers->restoreContainer($jscontainer_ref, $branch_ref, $timestamp, $jscontainer_ref, $full_branchname);
      }
    } elsif (lc $action eq 'delete') {
      if (!defined($dropvdb) || ( ! ( ( lc $dropvdb eq 'yes' ) || (lc $dropvdb eq 'no' ) ) ) ) {
        print "dropvdb parameter has to be set to yes or no \n";
        pod2usage(-verbose => 1,  -input=>\*DATA);
        exit (1);
      }
      $jobno = $jscontainers->deleteContainer($jscontainer_ref, $dropvdb);
    } elsif (lc $action eq 'enable') {
      $jobno = $jscontainers->enableContainer($jscontainer_ref);
    } elsif (lc $action eq 'disable') {
      $jobno = $jscontainers->disableContainer($jscontainer_ref);
    } elsif (lc $action eq 'addowner') {
      if (!defined($container_owners)) {
        print "Container_owner need to be defined\n";
        $ret = $ret + 1;
        next;
      }
      my $users = new Users ( $engine_obj, $debug );
      my $userobj;
      for my $cowner (@{$container_owners}) {
          my $userobj = $users->getUserByName($cowner);
          if (defined($userobj)) {
            if (($userobj->isJS()) || ($userobj->isAdmin())) {
              $jobno = $jscontainers->addOwner($jscontainer_ref, $userobj->getReference());
              $ret = $ret + Toolkit_helpers::waitForAction($engine_obj, $jobno, "Owner $cowner added", "There were problems with adding owner");
              undef $jobno;
            }
          } else {
            print "Delphix User $cowner not found\n";
            $ret = $ret + 1;
          }
      }

    } elsif (lc $action eq 'deleteowner') {
      if (!defined($container_owners)) {
        print "Container_owner need to be defined\n";
        $ret = $ret + 1;
        next;
      }
      my $users = new Users ( $engine_obj, $debug );
      my $userobj;
      for my $cowner (@{$container_owners}) {
          my $userobj = $users->getUserByName($cowner);
          if (defined($userobj)) {
            if (($userobj->isJS()) || ($userobj->isAdmin())) {
              $jobno = $jscontainers->removeOwner($jscontainer_ref, $userobj->getReference());
              $ret = $ret + Toolkit_helpers::waitForAction($engine_obj, $jobno, "Owner $cowner removed", "There were problems with removing owner");
              undef $jobno;
            }
          } else {
            print "Delphix User $cowner not found\n";
            $ret = $ret + 1;
          }
      }

    }
  }

  # create a new container only
  if (lc $action eq 'create') {
    if (!defined($container_def)) {
      print "Container definition parameter -container_def is required to create container \n";
      pod2usage(-verbose => 1,  -input=>\*DATA);
      exit (1);
    }

    if (!defined($template_name)) {
      print "Template name has to be defined to create container \n";
      pod2usage(-verbose => 1,  -input=>\*DATA);
      exit (1);
    }

    my @contowner_array;
    if (defined($container_owners)) {
      my $users = new Users ( $engine_obj, $debug );
      my $userobj;
      for my $cowner (@{$container_owners}) {
          my $userobj = $users->getUserByName($cowner);
          if (defined($userobj)) {
            if (($userobj->isJS()) || ($userobj->isAdmin())) {
              push(@contowner_array, $userobj->getReference());
            }
          }
      }
      if (scalar(@contowner_array) < scalar(@{$container_owners})) {
        print "Not all users defined as owners found. Skipping creation on engine $engine\n";
        $ret = $ret + 1;
        next;
      }
    }


    # find a source definition from template matching a VDB provided
    my $jsdatasources = new JS_datasource_obj ( $engine_obj, $template_ref, undef, $debug );
    my $databases = new Databases($engine_obj, $debug);
    my $groups = new Group_obj($engine_obj, $debug);

    my @cont_array;


    my %dupVDBprotection;

    for my $coitem ( @{$container_def} ) {

      my @single_cont = split(',', $coitem);
      if (scalar(@single_cont) ne 2) {
        print "container_def required a 2 comma separated values - group name, database name\n";
        pod2usage(-verbose => 1,  -input=>\*DATA);
        exit (1);
      }

      if (defined($dupVDBprotection{$single_cont[0].$single_cont[1]})) {
        print "VDB $single_cont[0]/$single_cont[1] is already used in other container definition. Exiting\n";
        exit(1);
      } else {
        $dupVDBprotection{$single_cont[0].$single_cont[1]} = 1;
      }

      for my $ds ( @{$jsdatasources->getJSDataSourceList()} ) {
        my $dsource = $jsdatasources->getJSDBContainer($ds);
        my $vdb_ref = Toolkit_helpers::get_dblist_from_filter(undef, $single_cont[0], undef, $single_cont[1], $databases, $groups, undef, undef, undef, undef, undef, undef, undef, $debug);

        if ((!defined($vdb_ref)) || (scalar(@{$vdb_ref}) < 1)) {
          print "VDB $single_cont[0]/$single_cont[1] not found \n";
          exit(1)
        }

        if (scalar(@{$vdb_ref}) > 1) {
          print "VDB $single_cont[0]/$single_cont[1] is not unique \n";
          exit(1)
        }

        my $pc = ($databases->getDB($vdb_ref->[0]))->getParentContainer();

        if ($dsource eq $pc) {
          my %con = (
            "source" => $jsdatasources->getName($ds),
            "vdb_ref" => $vdb_ref->[0]
          );
          push(@cont_array, \%con);
        }

      }

    }

    if (scalar(@cont_array) ne scalar(@{$container_def})) {
      print "Not all VDBs mapped to sources. Exiting\n";
      exit(1);
    }

    if (scalar(@{$jsdatasources->getJSDataSourceList()}) ne scalar(@cont_array)) {
      print "Template definition contain more sources than VDBs provided.Exiting\n";
      exit(1);
    }

    $jobno = $jscontainers->createContainer($container_name, $template_ref, \@cont_array, \@contowner_array, $dontrefresh);

  }

  if ( ! ( (lc $action eq 'addowner' ) || (lc $action eq 'deleteowner' ) ) ) {

    if (defined ($jobno) ) {
      print "Starting job $jobno for container $container_name.\n";
      my $job = new Jobs_obj($engine_obj, $jobno, 'true', $debug);

      my $jobstat = $job->waitForJob();
      if ($jobstat ne 'COMPLETED') {
        $ret = $ret + 1;
      }
    } else {
      print "Job for container is not created. \n";
      $ret = $ret + 1;
    }

  }

}


exit $ret;



__DATA__

=head1 SYNOPSIS

 dx_ctl_js_container    [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                        -action reset|refresh|restore|create|addowner|deleteowner|enable|disable
                        -container_name container_name
                        [-container_def GroupName,VDBName]
                        [-container_owner username]
                        [-template_name template_name]
                        [-timestamp timestamp]
                        [-bookmark_branchname bookmark_branch_name]
                        [-branch_name branch_name]
                        [-dropvdb yes|no]
                        [-dontrefresh]
                        [ -help|? ] [ -debug ]

=head1 DESCRIPTION

Run a action on the JetStream container. If branch is not specified

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 3

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

=item B<-action reset|refresh|recover|create|delete|enable|disable>
Run a action on the container

=over 3

=item B<-create> - create JS container

=item B<-delete> - create JS container

=item B<-reset> - reset JS container to latest point

=item B<-refresh> - refresh JS container from latest point of template

=item B<-restore> - restore a JS container to point in time or bookmark name.
Bookmark name can be from template or container timeline.
If ones want to restore container from template's point in time - template_name
and fromtemplate flag is required.

=item B<-addowner> - Add owner to container

=item B<-deleteowner> - Remove owner from container

=item B<-enable> - Enable container

=item B<-disable> - Disable container

=back

=item B<-container_name container_name>
Name of container to run action on

=item B<-template_name template_name>
Name of container's templates

=item B<-branch_name branch_name>
Container branch name for action - default branch name is "default"

=back

=head1 OPTIONS

=over 3

=item B<-container_def GroupName,VDBName>
Specify a VDB which will be used for  container.
This parameter can be repeated if more than one VDB is required.

=item B<-container_owner de_username>
Specify a container owner
This parameter can be repeated if more than one owner is required.

=item B<-timestamp "YYYY-MM-DD HH24:MI:SS" or bookmark name >
Use timestamp or bookmark name to restore container

=item B<-bookmark_branchname bookmark_branch_name>
If bookmark name is used as timestamp for restore action,
and bookmark name is not unique, this option allows to specify a branch name
which will unequally identify bookmark.

Full name format for template bookmarks is:
templatename/master

Full name format for container bookmarks is:
templatename/containername/branchname


=item B<-dropvdb yes|no>
Drop VDB when deleteing container

=item B<-fromtemplate>
Use template timeline for container restore

=item B<<-dontrefresh>
Don't refresh VDB while creating container (req. Delphix Enigne >= 5.1.6)

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging


=back

=head1 Examples

Reset of the container

 dx_ctl_js_container -d Landshark5 -container_name cont -action reset
 Starting job JOB-5043 for container cont.
 0 - 3 - 4 - 23 - 26 - 29 - 30 - 34 - 47 - 52 - 54 - 57 - 58 - 59 - 60 - 61 - 76 - 90 - 100
 Job JOB-5043 finished with state: COMPLETED


Refresh of the container from latest point in time in source

 dx_ctl_js_container -d Landshark5 -container_name cont -action refresh
 Starting job JOB-5050 for container cont.
 0 - 3 - 4 - 12 - 26 - 29 - 30 - 34 - 45 - 47 - 52 - 54 - 57 - 58 - 59 - 60 - 61 - 70 - 77 - 83 - 100
 Job JOB-5050 finished with state: COMPLETED

Restore of the cointainer to a bookmark "fixeddate"

 dx_ctl_js_container -d Landshark5 -container_name cont1 -action restore -timestamp fixeddate
 Starting job JOB-7637 for container cont1.
 0 - 3 - 4 - 23 - 26 - 29 - 30 - 34 - 45 - 47 - 52 - 54 - 57 - 58 - 59 - 60 - 61 - 68 - 77 - 82 - 100
 Job JOB-7637 finished with state: COMPLETED

Restore if the container from template timeline

 dx_ctl_js_container -d Landshark51 -action restore -template_name template2 -container_name cont2 -timestamp "2017-04-15 12:00:00" -fromtemplate
 Starting job JOB-2356 for container cont2.
 0 - 2 - 3 - 11 - 24 - 26 - 28 - 52 - 53 - 66 - 67 - 68 - 72 - 77 - 100
 Job JOB-2356 finished with state: COMPLETED

Create a new container based on template with 2 sources

 dx_ctl_js_container -d Landshark51 -action create -container_def "Analytics,testdx" -container_def "Analytics,autotest" -container_name cont2 -template_name template2 -container_owner js
 Starting job JOB-2411 for container cont2.
 0 - 2 - 3 - 13 - 24 - 26 - 27 - 48 - 52 - 53 - 66 - 67 - 68 - 75 - 81 - 100
 Job JOB-2411 finished with state: COMPLETED

Delete a container cont2 without dropping a VDB

 dx_ctl_js_container -d Landshark51 -action delete -container_name cont2 -dropvdb no
 Starting job JOB-2434 for container cont2.
 0 - 100
 Job JOB-2434 finished with state: COMPLETED

Adding owner to container

 dx_ctl_js_container -d Landshark5 -action addowner -container_name test1 -container_owner js
 Waiting for all actions to complete. Parent action is ACTION-16427
 Owner js added

=cut
