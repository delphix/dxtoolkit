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
# Program Name : dx_get_js_bookmarks.pl
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
use Databases;
use Timeflow_obj;
use JS_operation_obj;


my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'template_name=s' => \(my $template_name),
  'container_name=s' => \(my $container_name),
  'bookmark_name=s' => \(my $bookmark_name),
  'container_only' => \(my $container_only),
  'realtime' => \(my $realtime),
  'all' => (\my $all),
  'format=s' => \(my $format),
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
  print "Options all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


if (defined($realtime) && (!defined($bookmark_name))) {
  print "Option realtime has to be used with single bookmark only \n";
  exit (1);
}



# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);
my $output = new Formater();


if (defined($realtime)) {
  $output->addHeader(
      {'Appliance',         20},
      {'Bookmark name',     30},
      {'Bookmark time',     30},
      {'Template name',     30},
      {'Container name',    30},
      {'Branch name',       20},
      {'Source name',       20},
      {'Source time',       30}
  );
} else {
  $output->addHeader(
      {'Appliance',         20},
      {'Bookmark name',     30},
      {'Bookmark time',     30},
      {'Template name',     30},
      {'Container name',    30},
      {'Branch name',       20}
  );
}


my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    $ret = $ret + 1;
    next;
  };

  my $tempref;

  my $container = new JS_container_obj ( $engine_obj, $tempref, $debug );
  my $container_ref = $container->getJSContainerByName("con1");


  print Dumper $container_ref;

  my $datasources = new JS_datasource_obj ( $engine_obj, $tempref, undef, $debug );

  my @dbarray;

  for my $dsref (@{$datasources->getJSDataSourceByContainer($container_ref)}) {
    my $ds = $datasources->getJSDataSource($dsref);
    push(@dbarray, $ds->{container});
  }

  my $databases = new Databases( $engine_obj, $debug);
  my $timeflows = new Timeflow_obj($engine_obj, undef, $debug);
  my $hier = $timeflows->generateHierarchy(undef, undef, $databases);



  my $operations = new JS_operation_obj ( $engine_obj , undef, $debug);
  $operations->loadJSOperationList();


  my $opsforcont = $operations->getJSOperationList($container_ref);

  my $realtime;
  my $opname;
  my $optime;


  # for my $op (@{$opsforcont}) {
  #   $optime = $operations->getEndTime($op);
  #   $opname = $operations->getName($op);
  #
  #   if (($opname eq "CREATE_BRANCH") ||  ($opname eq "RESTORE") ||
  #       ($opname eq "REFRESH") || ($opname eq "CREATE_BOOKMARK") ) {
  #
  #         if (version->parse($engine_obj->getApi()) < version->parse(1.8.0)) {
  #           $realtime = $datasources->checkTime($container_ref, $optime);
  #         } else {
  #           $realtime = $datasources->checkTime($operations->getBranch($op), $optime);
  #         }
  #
  #         print Dumper $optime;
  #         print Dumper $realtime;
  #
  #   }
  #
  #
  #
  # }


  print Dumper "-------------------------------";

  my $tfrangearray;
  my %timeflowranges;

  my $conttimeflows = $timeflows->getTimeflowsForContainer($dbarray[0]);

  for my $conttf (sort (@{$conttimeflows})) {
    my @optime = split("@", $timeflows->getName($conttf));

    if (scalar(@optime) > 1) {

      $tfrangearray = $timeflows->getTimeflowRange($conttf);
      my $timestart = $tfrangearray->[0]->{startPoint}->{timestamp};

      print Dumper $optime[1];

      my $firstop = $operations->findOpAfterDataTime($optime[1] . ".000Z");

      print Dumper $operations->getName($firstop);
      print Dumper $operations->getBranch($firstop);
      #print Dumper $conttf;
      #print Dumper $timestart . " - " .$tfrangearray->[0]->{endPoint}->{timestamp};

      $timeflowranges{$conttf}{range} = $tfrangearray;
      $timeflowranges{$conttf}{branch} = $operations->getBranch($firstop);

      my ($parenttf, $topchild) = $timeflows->findParentTimeflow( $conttf, $hier);
      #
      print Dumper "real Parent snapshot " . $timeflows->getParentSnapshot($topchild);


    } else {
       # this is for container created without refresh

       print Dumper $operations->getName($opsforcont->[0]);
       $tfrangearray = $timeflows->getTimeflowRange($conttf);
       $timeflowranges{$conttf}{range} = $tfrangearray;
       $timeflowranges{$conttf}{branch} = $operations->getBranch($opsforcont->[0]);
    }



  }



  my $dbobj = $databases->getDB($dbarray[0]);

  my $snapshots = new Snapshot_obj( $engine_obj, $dbobj->getReference(), undef, $debug);



  my $bookmarks = new JS_bookmark_obj ( $engine_obj, undef, $container_ref, $debug );

  for my $book (@{$bookmarks->getJSBookmarkList()}) {
    my $booktime = $bookmarks->getJSBookmarkTime($book, 1);
    my $bookbranch = $bookmarks->getJSBookmarkBranch($book);


    print Dumper $booktime;

    print Dumper $bookbranch;

    if (version->parse($engine_obj->getApi()) < version->parse(1.8.0)) {
      $realtime = $datasources->checkTime($container_ref, $booktime, 1);
    } else {
      $realtime = $datasources->checkTime($bookbranch, $booktime, 1);
    }

    print Dumper $realtime->[0]->{timestamp};

    #print Dumper \%timeflowranges;

    my @tfbranch = grep { ($timeflowranges{$_}{branch} eq $bookbranch) } sort (keys %timeflowranges);

    my $tf;

    for my $tfr (@tfbranch) {

      for my $r (@{$timeflowranges{$tfr}{range}}) {
        if (
             ( $realtime->[0]->{timestamp} ge $r->{startPoint}->{timestamp} )
             && ( $realtime->[0]->{timestamp} le $r->{endPoint}->{timestamp} )
           )
        {
          $tf = $r->{startPoint}->{timeflow};
          next;
        }
      }

    }

    # print Dumper \@tf;

    #exit;

    my ($parenttf, $topchild) = $timeflows->findParentTimeflow( $tf, $hier);
    #
    print Dumper "real Parent snapshot " . $timeflows->getParentSnapshot($topchild);

    my $snap = $snapshots->findSnapshotforTimestamp($realtime->[0]->{timestamp}, $tf);

    print Dumper $snap;

  }

  exit;

  print Dumper $dbarray[0];

  # this is for bookmark to branch mapping
  # each branch will have at least one timeflow

  print Dumper $timeflows->getTimeflowsForContainer($dbarray[0]);

  print Dumper $timeflows->getName("ORACLE_TIMEFLOW-1163");

  my $tfrangearray = $timeflows->getTimeflowRange("ORACLE_TIMEFLOW-1163");

  my $dbobj = $databases->getDB($dbarray[0]);

  my $ctf = "ORACLE_TIMEFLOW-1163";

  print Dumper $tfrangearray;

  my $timestart = $tfrangearray->[0]->{startPoint}->{timestamp};

  print Dumper "current tf " . $ctf;

  my ($parenttf, $topchild) = $timeflows->findParentTimeflow($ctf, $hier);

  print Dumper "hierarchy parent timeflow " . $parenttf;
  print Dumper "hierarchy topchild timeflow " . $topchild;

  my $snapshots = new Snapshot_obj( $engine_obj, $dbobj->getReference(), undef, $debug);

  my $snap = $snapshots->findSnapshotforTimestamp($timestart);
  print Dumper $snap->{snapshotref};
  print Dumper $snap->{timestamp};
  # print Dumper "Parent snapshot " . $timeflows->getParentSnapshot($snap->{timeflow});
  # print Dumper "time timeflow " . $snap->{timeflow};


  ($parenttf, $topchild) = $timeflows->findParentTimeflow($snap->{timeflow}, $hier);

  print Dumper "real Parent snapshot " . $timeflows->getParentSnapshot($topchild);

  # $snap = $snapshots->findSnapshotforTimestamp('2018-12-27 12:03:52');
  # print Dumper $snap->{snapshotref};
  # print Dumper $snap->{timestamp};
  # print Dumper "Parent snapshot " . $timeflows->getParentSnapshot($snap->{timeflow});
  # print Dumper "Parent timeflow " . $snap->{timeflow};

  exit;


  my $bookmarks;
  my $template_ref;


  if (defined($template_name)) {
    my $datalayout = new JS_template_obj ( $engine_obj, $debug );
    $template_ref = $datalayout->getJSTemplateByName($template_name);

    if (defined($container_name)) {
      my $container = new JS_container_obj ( $engine_obj, $template_ref, $debug );
      my $container_ref = $container->getJSContainerByName($container_name);
      $bookmarks = new JS_bookmark_obj ( $engine_obj, undef, $container_ref, $debug );
    } else {
      $bookmarks = new JS_bookmark_obj ( $engine_obj, $template_ref, undef, $debug );
    }
  }


  if (!defined($bookmarks)) {
    $bookmarks = new JS_bookmark_obj ( $engine_obj, undef, undef, $debug );
  }


  my $branchs = new JS_branch_obj ( $engine_obj, undef, $debug );
  my $datasources;

  if (defined($realtime)) {
    $datasources = new JS_datasource_obj ( $engine_obj, $template_ref, undef, $debug );
  }

  my @bookmark_array;

  if (defined($bookmark_name)) {
    my $book_ref = $bookmarks->getJSBookmarkByName($bookmark_name);
    if (defined($book_ref)) {
      push(@bookmark_array, $book_ref);
    } else {
      print "Can't find bookmark name $bookmark_name \n";
      $ret = $ret + 1;
    }
  } else {
    @bookmark_array = @{$bookmarks->getJSBookmarkList($container_only)};
  }


  for my $bookmarkitem (@bookmark_array) {
    my $bookmark_time = $bookmarks->getJSBookmarkTimeWithTimestamp($bookmarkitem);

    my $obj_ref;
    my $contref = $bookmarks->getJSBookmarkContainer($bookmarkitem);
    my $tempref = $bookmarks->getJSBookmarkTemplate($bookmarkitem);

    if (defined($contref) && ($contref ne 'N/A') ) {
      $obj_ref = $contref;
    } else {
      $obj_ref = $tempref;
    }

    if (defined($realtime)) {

      $output->addLine (
        $engine,
        $bookmarks->getName($bookmarkitem),
        $bookmark_time,
        $bookmarks->getJSBookmarkTemplateName($bookmarkitem),
        $bookmarks->getJSBookmarkContainerName($bookmarkitem),
        $branchs->getName($bookmarks->getJSBookmarkBranch($bookmarkitem)),
        '',
        ''
      );

      my $t = $bookmarks->getJSBookmarkTime($bookmarkitem, 1);
      my $realtime;

      if (version->parse($engine_obj->getApi()) < version->parse(1.8.0)) {
        $realtime = $datasources->checkTime($obj_ref, $t);
      } else {
        $realtime = $datasources->checkTime($bookmarks->getJSBookmarkBranch($bookmarkitem), $t);
      }

      for my $t ( @{$realtime} ) {
        $output->addLine (
          '',
          '',
          '',
          '',
          '',
          '',
          $t->{name},
          $t->{timestamp}
        );


      }

    } else {
      $output->addLine (
        $engine,
        $bookmarks->getName($bookmarkitem),
        $bookmark_time,
        $bookmarks->getJSBookmarkTemplateName($bookmarkitem),
        $bookmarks->getJSBookmarkContainerName($bookmarkitem),
        $branchs->getName($bookmarks->getJSBookmarkBranch($bookmarkitem))
      );
    }

  }



}


Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_get_js_bookmarks    [-engine|d <delphix identifier> | -all ]
                        [-template_name template_name]
                        [-container_name container_name]
                        [-bookmark_name bookmark_name]
                        [-realtime] [-container_only]
                        [-format csv|json ]
                        [-help|? ] [ -debug ]

=head1 DESCRIPTION

Get the list of Jet Stream bookmarks from Delphix Engine.

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

=item B<-template_name template_name>
Display bookmarks from template with template_name (no containers bookmark)

=item B<-container_name container_name>
Display bookmarks from container and template with template_name and container name

=item B<-container_only>
Display container only bookmarks and skip template bookmarks

=item B<-bookmark_name bookmark_name>
Display bookmarks with a bookmark_name

=item B<-realtime>
Display exact time of bookmark (works with bookmark name only)

=back

=head1 OPTIONS

=over 3

=item B<-format>
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=item B<-nohead>
Turn off header output

=back

=head1 EXAMPLES

List all bookmarks

 dx_get_js_bookmarks -d Landshark5

 Appliance            Bookmark name                  Bookmark time                  Template name                  Container name                 Branch name
 -------------------- ------------------------------ ------------------------------ ------------------------------ ------------------------------ --------------------
 Landshark5           Before insert                  2016-10-25 08:41:34 IST        Oracle dSource template        Dev container                  default
 Landshark5           BookmarkNOW                    2016-11-08 16:46:35 GMT        Oracle dSource template        Dev container                  default

List only containers JS bookmarks

 dx_get_js_bookmarks -d Landshark5 -container_only

 Appliance            Bookmark name                  Bookmark time                  Template name   Container name                 Branch name
 -------------------- ------------------------------ ------------------------------ --------------- ------------------------------ ---------------
 Landshark5           bookmark1                      2016-07-28 14:48:39 IST        test            cont                           default
 Landshark5           test book                      2016-07-28 15:54:15 IST        test            cont                           default
 Landshark5           last book                      2016-07-28 16:08:19 IST        test            cont                           default

Display a real database point for bookmark1

 dx_get_js_bookmarks -d Landshark5 -bookmark_name "BookmarkNOW" -realtime

 Appliance            Bookmark name                  Bookmark time                  Template name                  Container name                 Branch name          Source name          Source time
 -------------------- ------------------------------ ------------------------------ ------------------------------ ------------------------------ -------------------- -------------------- ------------------------------
 Landshark5           BookmarkNOW                    2016-11-08 16:46:35 GMT        Oracle dSource template        Dev container                  default
                                                                                                                                                                       Oracle dSource       2016-11-08 16:46:35 GMT


=cut
