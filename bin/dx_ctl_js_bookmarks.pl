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
# Copyright (c) 2016,2017 by Delphix. All rights reserved.
#
# Program Name : dx_ctl_js_bookmarks.pl
# Description  : Get Delphix Engine timeflow bookmarks
# Author       : Marcin Przepiorowski
# Created      : 02 Mar 2016 (v2.2.3)
#

use strict;
use warnings;
use JSON;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev); #avoids conflicts with ex host and help
use File::Basename;
use Pod::Usage;
use FindBin;
use Data::Dumper;
use warnings;
use strict;
use version;

my $abspath = $FindBin::Bin;

use lib '../lib';
use Engine;
use Formater;
use Toolkit_helpers;
use JS_template_obj;
use JS_datasource_obj;
use JS_bookmark_obj;
use JS_branch_obj;
use JS_container_obj;
use JS_operation_obj;
use Databases;


my $version = $Toolkit_helpers::version;

my $diff = 60;

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'action=s'  => \(my $action),
  'template_name=s' => \(my $template_name),
  'container_name=s' => \(my $container_name),
  'bookmark_name=s' => \(my $bookmark_name),
  'usefullname' => \(my $usefullname),
  'branch_name=s' => \(my $branch_name),
  'bookmark_branchname=s' => \(my $full_branchname),
  'bookmark_time=s' => \(my $bookmark_time),
  'container_only' => \(my $container_only),
  'snapshots=s' => \(my $snapshots),
  'source=s' => \(my $source),
  'expireat=s' => \(my $expireat),
  'diff=i' => \($diff),
  'all' => (\my $all),
  'version' => \(my $print_version),
  'dever=s' => \(my $dever),
  'nohead' => \(my $nohead),
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

if ( (! defined($action) ) || ( ! ( ( $action eq 'create') || ( $action eq 'remove')
      || ( $action eq 'share')  || ( $action eq 'unshare') ) ) ) {
  print "Option -action not defined or has invalid parameter \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (lc $action eq 'create') {

  if (!defined($template_name) || (!(defined($bookmark_name)))) {
    print "Options template_name and bookmark_name or bookmark_prefix are required \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }

  if (! (defined($bookmark_time) || defined($snapshots) ) ) {
    print "Options bookmark_time or snapshots are required \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }

  if (defined($bookmark_time) && defined($snapshots)) {
    print "Options bookmark_time and snapshots are mutually exclusive \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }

  if (!defined($template_name) && defined($container_name)) {
    print "Options container_name required a template_name parametrer \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }


  if (defined($snapshots) && ( ! ( ( lc $snapshots eq 'all' ) || ( lc $snapshots eq 'both' ) || ( lc $snapshots eq 'first' ) || ( lc $snapshots eq 'last' ) ) ) ) {
    print "Option snapshot allow the following values all, both, first, last \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }

  if (defined($snapshots) && ((( lc $snapshots eq 'all' ) || ( lc $snapshots eq 'both' )) && (defined($usefullname)))) {
    print "Snapshot option all or both can't run with usefullname flag \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }

  if (defined($snapshots) && (!defined($source))) {
    print "Option snapshot require a source to be defined \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }


  if (defined($bookmark_time) && ( ! ( $bookmark_time eq 'latest' || $bookmark_time eq 'first' || $bookmark_time =~ /\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d/ ) )    ) {
    print "Wrong format of bookmark_time parameter - $bookmark_time \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }
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


  if (lc $action eq 'create') {
    # this is for creating a new bookmark

    my $datalayout;
    my $datalayout_ref;
    my $bookmarks;

    if (defined($template_name)) {
      $datalayout = new JS_template_obj ( $engine_obj, $debug );
      $datalayout_ref = $datalayout->getJSTemplateByName($template_name);

      if (defined($container_name)) {
        $datalayout = new JS_container_obj ( $engine_obj, $datalayout_ref, $debug );
        $datalayout_ref = $datalayout->getJSContainerByName($container_name);
      }
    }

    if (!defined($datalayout_ref)) {
      print "Can't find template with a name $template_name on engine $engine \n";
      $ret = $ret + 1;
      next;
    }

    $bookmarks = new JS_bookmark_obj ( $engine_obj, undef, undef, $debug );
    my $branchs = new JS_branch_obj ( $engine_obj, $datalayout_ref, $debug );

    my $active_branch;

    if (defined($branch_name)) {
      $active_branch = $branchs->getJSBranchByName($branch_name);
      if (!defined($active_branch)) {
        print "Can't find branch with a name $branch_name in template $template_name on engine $engine \n";
        $ret = $ret + 1;
        next;
      }
    } else {
      $active_branch =  $datalayout->getJSActiveBranch($datalayout_ref);
    }


    my $datasources = new JS_datasource_obj ( $engine_obj, $datalayout_ref, undef, undef );

    if ( defined($snapshots) ) {
      # create bookmarks on snapshots
      my $bookmark_times_hash = generate_snapshot_mapping($source, $datasources, $snapshots, $datalayout_ref, $active_branch);


      for my $bookname_item (sort (keys %{$bookmark_times_hash})) {
        if ( $datasources->checkTimeDelta($datalayout_ref, $bookmark_times_hash->{$bookname_item}, $diff ) ) {
          print "Delta between bookmark time and real time of source is bigger than $diff sec.\n"
        }
        create($bookmarks, $engine_obj, $debug, $bookname_item, $active_branch, $datalayout_ref, $bookmark_times_hash->{$bookname_item}, 1, $expireat);
      }

    } else {

      my $zulu;

      if (defined($template_name) && (lc $bookmark_time eq 'first') ) {
        my $firstop = $datalayout->getJSFirstOperation($datalayout_ref);


        if (defined($firstop)) {
          my $operations = new JS_operation_obj ( $engine_obj , $firstop, $debug);
          $operations->loadJSOperationList();

          $bookmark_time = $operations->getEndTime($firstop);

          $zulu = 1;

        } else {
          print "Can't find a first operation for template or container \n";
          $ret = $ret + 1;
          next;
        }
      }


      if ( $datasources->checkTimeDelta($datalayout_ref, $bookmark_time, $diff ) ) {
        print "Delta between bookmark time and real time of source is bigger than $diff sec.\n"
      }

      create($bookmarks, $engine_obj, $debug, $bookmark_name, $active_branch, $datalayout_ref, $bookmark_time, $zulu, $expireat);
    }
  } else {

    # this is for other bookmark actions
    my $bookmarks;
    my $template_ref;
    my $container_ref;


    if (defined($template_name)) {
      my $templates = new JS_template_obj ( $engine_obj, $debug );
      $template_ref = $templates->getJSTemplateByName($template_name);
      if (!defined($template_ref)) {
        print "Template $template_name not found\n";
        $ret = $ret + 1;
        next;
      }
    }

    if (defined($container_name)) {
      my $container = new JS_container_obj ( $engine_obj, $template_ref, $debug );
      $container_ref = $container->getJSContainerByName($container_name);
      if (!defined($container_ref)) {
        $ret = $ret + 1;
        next;
      }
    }

    if (defined($container_ref)) {
      $bookmarks = new JS_bookmark_obj ( $engine_obj, undef, $container_ref, $debug );
    } elsif (defined($template_ref)) {
      $bookmarks = new JS_bookmark_obj ( $engine_obj, $template_ref, undef, $debug );
    } else {
      $bookmarks = new JS_bookmark_obj ( $engine_obj, undef, undef, $debug );
    }


    my @bookmark_array;

    if (defined($bookmark_name)) {
      my $book_ref = $bookmarks->getJSBookmarkByName($bookmark_name, $full_branchname);
      if (defined($book_ref)) {
        push(@bookmark_array, $book_ref);
      } else {
        $ret = $ret + 1;
        next;
      }
    } else {
      @bookmark_array = @{$bookmarks->getJSBookmarkList($container_only)};
    }


    for my $bookmarkitem (@bookmark_array) {
      #print Dumper $bookmarkitem;

      $bookmark_name = $bookmarks->getName($bookmarkitem);

      if (lc $action eq 'delete') {
        my $jobno = $bookmarks->deleteBookmark($bookmarkitem);
        $ret = $ret + Toolkit_helpers::waitForJob($engine_obj, $jobno, "Bookmark $bookmark_name deleted", "Problem with deleting bookmark $bookmark_name")

      } elsif (lc $action eq 'share') {

        if ($bookmarks->shareBookmark($bookmarkitem)) {
          print "Issues with sharing bookmark $bookmark_name\n";
          $ret = $ret + 1;
          next;
        } else {
          print "Bookmark $bookmark_name shared\n";
        }

      } elsif (lc $action eq 'unshare') {

        if ($bookmarks->unshareBookmark($bookmarkitem)) {
          print "Issues with unsharing bookmark $bookmark_name\n";
          $ret = $ret + 1;
          next;
        } else {
          print "Bookmark $bookmark_name unshared\n";
        }

      }

    }
  }
}

exit $ret;

sub create {
  my $bookmarks = shift;
  my $engine_obj = shift;
  my $debug = shift;
  my $bookmark_name = shift;
  my $active_branch = shift;
  my $datalayout_ref = shift;
  my $bookmark_time = shift;
  my $zulu = shift;
  my $expireat = shift;

  if (defined($expireat)) {
    my $tz = $engine_obj->getTimezone();
    $expireat = Toolkit_helpers::convert_to_utc($expireat, $tz, undef, 1);
  }

  my $jobno = $bookmarks->createBookmark($bookmark_name, $active_branch, $datalayout_ref, $bookmark_time, $zulu, $expireat);

  if (defined ($jobno) ) {
    print "Starting job $jobno for bookmark $bookmark_name.\n";
    my $job = new Jobs_obj($engine_obj, $jobno, 'true', $debug);

    my $jobstat = $job->waitForJob();
    if ($jobstat ne 'COMPLETED') {
      $ret = $ret + 1;
    }
  } else {
    print "Job for bookmark is not created. \n";
    $ret = $ret + 1;
  }

}


sub generate_snapshot_mapping {
  my $source = shift;
  my $datasources = shift;
  my $snapshots = shift;
  my $datalayout_ref = shift;
  my $active_branch = shift;


  my %bookmark_times_hash;

  my $ds_ref = $datasources->getJSDataSourceByName($source);
  if (!defined($ds_ref)) {
    print "Source $source in template $template_name not found. \n";
    $ret = $ret + 1;
    return \%bookmark_times_hash;
  }


  my $cont = $datasources->getJSDBContainer($ds_ref);
  my $snapshot = new Snapshot_obj ( $engine_obj, $cont, 1, undef );

  if ((lc $snapshots eq 'first') || (lc $snapshots eq 'both')) {
    # find a first snapshot which can be used for bookmark ( has been taken after template was created )
    for my $snapitem ( @{ $snapshot->getSnapshots() }) {
      my $time = $snapshot->getStartPoint($snapitem);
      my $goodtime;
      if (version->parse($engine_obj->getApi()) < version->parse(1.8.0)) {
        $goodtime = $datasources->checkTime($datalayout_ref, $time);
      } else {
        $goodtime = $datasources->checkTime($active_branch, $time);
      }
      if ( defined($goodtime) && (scalar(@{$goodtime}) > 0 )) {
        my $timename = $time;
        $timename =~ s/T/ /;
        $timename =~ s/\....Z//;
        if (defined($usefullname)) {
          $timename = $bookmark_name;
        } else {
          $timename = $bookmark_name . "-" . $timename
        }
        $bookmark_times_hash{$timename} = $time;
        last;
      }
    }
  }

  if ((lc $snapshots eq 'last') || (lc $snapshots eq 'both')) {
    my $last_time = (@{ $snapshot->getSnapshots() })[-1];

    my $time = $snapshot->getStartPoint($last_time);
    my $goodtime;
    if (version->parse($engine_obj->getApi()) < version->parse(1.8.0)) {
      $goodtime = $datasources->checkTime($datalayout_ref, $time);
    } else {
      $goodtime = $datasources->checkTime($active_branch, $time);
    }

    if ( defined($goodtime) && (scalar(@{$goodtime}) > 0 )) {
      my $timename = $time;
      $timename =~ s/T/ /;
      $timename =~ s/\....Z//;
      if (defined($usefullname)) {
        $timename = $bookmark_name;
      } else {
        $timename = $bookmark_name . "-" . $timename
      }
      $bookmark_times_hash{$timename} = $time;
    }
  }

  if (lc $snapshots eq 'all') {
    for my $snapitem ( @{ $snapshot->getSnapshots() }) {
      my $time = $snapshot->getStartPoint($snapitem);
      my $goodtime;
      if (version->parse($engine_obj->getApi()) < version->parse(1.8.0)) {
        $goodtime = $datasources->checkTime($datalayout_ref, $time);
      } else {
        $goodtime = $datasources->checkTime($active_branch, $time);
      }
      if ( defined($goodtime) && (scalar(@{$goodtime}) > 0 )) {
        my $timename = $time;
        $timename =~ s/T/ /;
        $timename =~ s/\....Z//;
        $bookmark_times_hash{$bookmark_name . '-' . $timename} = $time;
      }
    }
  }

  return \%bookmark_times_hash;

}

__DATA__

=head1 SYNOPSIS

 dx_ctl_js_bookmarks    [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                         -action create | remove | share | unshare
                         -template_name template_name
                         -container_name container_name
                         -bookmark_name bookmark_name
                        [-bookmark_time "YYYY-MM-DD HH24:MI:SS" | first | latest ]
                        [-bookmark_branchname bookmark_branch_name]
                        [-snapshots first | last | both | all]
                        [-source source_name]
                        [-container_name container_name]
                        [-expireat timestamp ]
                        [-usefullname]
                        [ --help|? ] [ -debug ]

=head1 DESCRIPTION

Create or remove the Jet Stream bookmarks on Delphix Engine.

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

=item B<-action action_name>
Action name. Allowed values are :

create - to create bookmark

remove - to delete bookmark ( be aware that without bookmark_name - all bookmarks from template or container will be deleted)

share - to mark bookmark as shared

unshare - to mark bookmark as unshared

=item B<-template_name template_name>
Set templare for bookmark using template name

=item B<-container_name container_name>
Set container for bookmark using container name

=item B<-bookmark_name bookmark_name>
Set bookmark name if bookmark is created using bookmark_time.
When bookmarks are created using snapshot option,
names will be generated using bookmark name as a prefix
and snapshot time.

This behaviour can be modified using usefullname flag

=item B<-usefullname>
If bookmarks are created using a snapshot last or snapshot first
option, this flag will force a bookmark name to be set without
adding a time of the snapshot


=item B<-bookmark_time time>
Set bookmark time. Allowed values:

- "YYYY-MM-DD HH:MI:SS" - timestamp (24h)

- first - use a branch creation time for bookmark (for template or container)

- latest - use latest possible time from container or template (now)

=item B<-bookmark_branchname bookmark_branch_name>
If bookmark name is used and bookmark name is not unique, this option allows to specify a branch name
which will unequally identify bookmark.

Full name format for template bookmarks is:
templatename/master

Full name format for container bookmarks is:
templatename/containername/branchname

=item B<-source source_name>
Set source name used for snapshot based bookmark creation

=item B<-snapshots snapshot>
Use snapshot from source to create bookmarks. Allowed values:

=over 3

=item B<-all> - create bookmarks for all snapshot of source created after template was created.
Bookmark names will be generated using this pattern: bookname_name-YYYY-MM-DDTHH:MI:SS.SSSZ

=item B<-first> - create bookmark for a first snapshot of source after template was created
Bookmark name will be generated using this pattern: bookname_name-YYYY-MM-DDTHH:MI:SS.SSSZ
If the -usefullname parameter is used, bookmark name will be created without adding a snapshot time

=item B<-last>  - create bookmark for a last snapshot of source after template was created
Bookmark name will be generated using this pattern: bookname_name-YYYY-MM-DDTHH:MI:SS.SSSZ
If the -usefullname parameter is used, bookmark name will be created without adding a snapshot time

=item B<-both>  - create bookmark for a first and last snapshot of source after template was created
Bookmark names will be generated using this pattern: bookname_name-YYYY-MM-DDTHH:MI:SS.SSSZ

=back

Bookmark will be created with a name following this pattern:


=item B<-expireat timestamp>
Set a bookmark expiration time using format "YYYY-MM-DD"
or "YYYY-MM-DD HH24:MI:SS"

=back

=head1 OPTIONS

=over 3

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging


=back

=head1 EXAMPLES

Create template bookmarks for all snapshots for template "template"" and source "oracle", bookmarks name starts with prefix "pre"
plus time of snapshot,

 dx_ctl_js_bookmarks -d Landshark5 -bookmark_name "pre" -template_name template -snapshots all -source oracle -action create
 Starting job JOB-7623 for bookmark pre-2016-10-12 12:02:31.
 5 - 100
 Job JOB-7623 finished with state: COMPLETED

Create template bookmark for a first snapshot of source "oracle" taken after template was created

 dx_ctl_js_bookmarks -d Landshark5 -bookmark_name "firstsnap" -template_name template -snapshots first -source oracle -action create
 Starting job JOB-7625 for bookmark firstsnap-2016-10-12 12:02:31.
 5 - 100
 Job JOB-7625 finished with state: COMPLETED

Create template bookmark for particular time

 dx_ctl_js_bookmarks -d Landshark5 -bookmark_name "fixeddate" -template_name template -bookmark_time "2016-10-12 13:05:02" -branch_name master -action create
 Starting job JOB-7626 for bookmark fixeddate.
 5 - 100
 Job JOB-7626 finished with state: COMPLETED

Create container bookmart for latest point

 dx_ctl_js_bookmarks -d Landshark5 -bookmark_name "cont_now" -bookmark_time latest -container_name cont1 -action create -template_name template
 Starting job JOB-7627 for bookmark cont_now.
 5 - 43 - 100
 Job JOB-7627 finished with state: COMPLETED

Deleting bookmark for template

 dx_ctl_js_bookmarks -d Landshark5 -bookmark_name "firstsnap-2016-10-12 12:02:31" -action remove -template_name template
 Starting job JOB-7629 for bookmark firstsnap-2016-10-12 12:02:31.
 0 - 100
 Job JOB-7629 finished with state: COMPLETED

Shareing bookmark "cont_now"

 dx_ctl_js_bookmarks -bookmark_name cont_now -action share
 Bookmark cont_now shared

Unsharing bookmark "cont_now"

 dx_ctl_js_bookmarks -bookmark_name cont_now -action unshare
 Bookmark cont_now unshared


=cut
