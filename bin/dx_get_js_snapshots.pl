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
# Program Name : dx_get_js_snapshots.pl
# Description  : Get Self service timelines and snapshots
# Author       : Marcin Przepiorowski
# Created      : 20 Dec 2018 (v2.3)
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
use Toolkit_helpers qw (logger);


my $version = $Toolkit_helpers::version;
my $output_unit = 'G';

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'template_name=s' => \(my $template_name),
  'container_name=s' => \(my $container_name),
  'all' => (\my $all),
  'format=s' => \(my $format),
  'version' => \(my $print_version),
  'dever=s' => \(my $dever),
  'nohead' => \(my $nohead),
  'debug:i' => \(my $debug),
  'output_unit:s' => \($output_unit),
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

if ( !  ( ( uc $output_unit eq 'G') || ( uc $output_unit eq 'M') || ( uc $output_unit eq 'K') ) ) {
  print "Option -output_unit can be only G for GB, M for MB and K for KB \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);
my $output = new Formater();



$output->addHeader(
    {'Appliance',             20},
    {'Bookmark/timeline',     50},
    {'Template name',         15},
    {'Container name',        15},
    {'VDB name'   ,           15},
    {'Branch name',           15},
    {'Bookmark snapshot',     30},
    {Toolkit_helpers::get_unit('Bookmark snap size',$output_unit),    20},
    {'Parent snapshot',       30},
    {Toolkit_helpers::get_unit('Parent snap size',$output_unit),      20},
    {'Parent name',           30}
);


my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    $ret = $ret + 1;
    next;
  };

  my $timezone = $engine_obj->getTimezone();

  my $template_ref;
  my $templates = new JS_template_obj ( $engine_obj, $debug );

  if (defined($template_name)) {
    $template_ref = $templates->getJSTemplateByName($template_name);
    if (!defined($template_ref)) {
      print "Template not found. Skiping engine\n";
      $ret = $ret + 1;
      next;
    }
  }

  my $containers = new JS_container_obj ( $engine_obj, $template_ref, $debug );
  my $container_ref;
  my @contlist;

  my %snapshot_sizes;

  # build a list of container to show
  if (defined($container_name)) {
    $container_ref = $containers->getJSContainerByName($container_name);
    if (!defined($container_ref)) {
      print "Container not found. Skiping engine\n";
      $ret = $ret + 1;
      next;
    } else {
      push(@contlist, $container_ref);
    }
  } else {
    @contlist = @{$containers->getJSContainerList()};
  }

  # build hierarchy of timeflow for engine
  my $databases = new Databases( $engine_obj, $debug);
  my $timeflows = new Timeflow_obj($engine_obj, undef, $debug);
  my $hier = $timeflows->generateHierarchy(undef, undef, $databases);

  my $bookmarks;

  for my $workcon (@contlist) {
    logger($debug, "processing container " . $workcon, 2);


    my $datasources = new JS_datasource_obj ( $engine_obj, undef, undef, $debug );
    my $jsbranches = new JS_branch_obj ( $engine_obj, $workcon, $debug );

    # array for container databases
    my %dbarray;

    for my $dsref (@{$datasources->getJSDataSourceByContainer($workcon)}) {
      my $ds = $datasources->getJSDataSource($dsref);
      $dbarray{$ds->{container}} = $ds->{reference};
    }

    # load operations for container
    my $operations = new JS_operation_obj ( $engine_obj , undef, $debug);
    $operations->loadJSOperationList($workcon);
    my $opsforcont = $operations->getJSOperationList($workcon);

    my $tfrangearray;
    my %timeflowranges;
    my $parentname;

    for my $dbref (keys %dbarray) {

      # load all timeflows for particular database in container
      my $conttimeflows = $timeflows->getTimeflowsForSelfServiceContainer($dbref);
      my $tfhash = $operations->link_tf_with_ss_operation($conttimeflows, $timeflows);

      # load database snapshots for parent
      my $dbobj = $databases->getDB($dbref);
      my $snapshots = new Snapshot_obj( $engine_obj, $dbobj->getParentContainer(), undef, $debug);
      $snapshots->getSnapshotList($dbobj->getParentContainer());
      $snapshots->getSnapshotList(keys %dbarray);

      # for all timeflows generate timeflow range for bookmarks,
      # find a matching operation from container based on name of timeflow
      #
      for my $conttf (@{$conttimeflows}) {
        # check if there is a SS operation for timeflow
        my $operation_for_conttf = $tfhash->{$conttf};
        my $branchref;
        if (defined($operation_for_conttf)) {
          $branchref = $operations->getBranch($operation_for_conttf);
        } else {
          $branchref = 'N/A';
        }

        # add to hash array a timeflow with range and branch
        # this is used in bookmark section to find where bookmark sits
        $tfrangearray = $timeflows->getTimeflowRange($conttf);
        $timeflowranges{$conttf}{range} = $tfrangearray;
        $timeflowranges{$conttf}{branch} = $branchref;
        $timeflowranges{$conttf}{sourceref} = $dbarray{$dbref};

        my ($parenttf, $topchild) = $timeflows->findParentTimeflow( $conttf, $hier);

        my $snapref = $timeflows->getParentSnapshot($topchild);
        my $snapshotname = $snapshots->getSnapshotName($snapref);

        my $snapsize;

        if (!defined($snapshotname)) {
          $snapshotname = "deleted";
          $snapsize = 'N/A';
          $parentname = 'N/A';
        } else {
          if (!defined($snapshot_sizes{$snapref})) {
            $snapsize = $snapshots->getSnapshotSize($snapref);
            if (defined($snapsize)) {
              $snapsize = Toolkit_helpers::print_size($snapsize, 'B', $output_unit);
            } else {
              $snapsize = 'N/A';
            }
            $snapshot_sizes{$snapref} = $snapsize
          } else {
            $snapsize = $snapshot_sizes{$snapref};
          }
          my $parentdbobj = $databases->getDB($snapshots->getSnapshotContainer($snapref));
          if (defined($parentdbobj)) {
            $parentname = $parentdbobj->getName();
          } else {
            $parentname = 'N/A';
          }
        }

        if (defined($operation_for_conttf)) {
          my $optime = Toolkit_helpers::convert_from_utc($operations->getStartTime($operation_for_conttf), $timezone, 1);
          my $branchname = $jsbranches->getName($operations->getBranch($operation_for_conttf));

          $output->addLine(
            $engine,
            $operations->getName($operation_for_conttf) . " / " . $optime,
            $templates->getName($containers->getJSContainerTemplate($workcon)),
            $containers->getName($workcon),
            $dbobj->getName(),
            $branchname,
            'N/A',
            'N/A',
            $snapshotname,
            $snapsize,
            $parentname
          );
        } else {
          $output->addLine(
            $engine,
            'Timeflow not in Self Service',
            'N/A',
            'N/A',
            $dbobj->getName(),
            'N/A',
            'N/A',
            'N/A',
            $snapshotname,
            $snapsize,
            $parentname
          );
        }

      }

      # bookmarks list

      my $realtime;

      # load container database snapshots
      my $cont_snapshots = new Snapshot_obj( $engine_obj, $dbref, undef, $debug);
      $cont_snapshots->getSnapshotList($dbref);
      #
      $bookmarks = new JS_bookmark_obj ( $engine_obj, undef, $workcon, $debug );

      # loop through all bookmarks
      for my $book (@{$bookmarks->getJSBookmarkList()}) {

        # read bookmark time and branch
        my $booktime = $bookmarks->getJSBookmarkTime($book, 1);
        my $bookbranch = $bookmarks->getJSBookmarkBranch($book);

        if (!defined($jsbranches->getName($bookbranch))) {
          # this is a template bookmarks, we need to skip it
          # print Dumper "Skipping " . $bookmarks->getName($book);
          next;
        }

        # convert bookmark time into database time
        if (version->parse($engine_obj->getApi()) < version->parse(1.8.0)) {
          $realtime = $datasources->checkTime($container_ref, $booktime, 1);
        } else {
          $realtime = $datasources->checkTime($bookbranch, $booktime, 1);
        }

        # rtitem is a table which should have only 1 row
        my @rtitem = grep { $_->{dsref} eq $dbarray{$dbref} }  @{$realtime};

        if (scalar(@rtitem)>1) {
          print "There is a problem with datasource time\n";
          print Dumper \@rtitem;
          $ret = $ret + 1;
          next;
        }

        #filter timeflows for particular database (jetstream source)
        my @tfsource = grep { ($timeflowranges{$_}{sourceref} eq $dbarray{$dbref}) } sort (keys %timeflowranges);

        # filter timeflows to particular branch
        my @tfbranch = grep { ($timeflowranges{$_}{branch} eq $bookbranch) } @tfsource;

        my $tf;

        # looping though timeflows from bookmark branch
        # to find a timeflow where bookmark sits
        for my $tfr (@tfbranch) {

          # loop through time ranges inside one timeflow
          for my $r (@{$timeflowranges{$tfr}{range}}) {
            if (
                 ( $rtitem[0]->{timestamp} ge $r->{startPoint}->{timestamp} )
                 && ( $rtitem[0]->{timestamp} le $r->{endPoint}->{timestamp} )
               )
            {
              # if timeflow if found - stop loop
              $tf = $r->{startPoint}->{timeflow};
              last;
            }
          }

        }


        # find parent timeflow for timeflow where bookmark exist
        my ($parenttf, $topchild) = $timeflows->findParentTimeflow( $tf, $hier);


        # find a snapshot in container source timeflow used by bookmark
        # rtitem is on ZULU already so switch search in zulu time

        my $snap = $cont_snapshots->findSnapshotforTimestamp($rtitem[0]->{timestamp}, $tf, 1);

        # convert ref to names
        my $branchname = $jsbranches->getName($bookbranch);
        my $parentsnapshotref = $timeflows->getParentSnapshot($topchild);
        my $parentsnapshotname = $snapshots->getSnapshotName($parentsnapshotref);
        my $contsnapshotname = $cont_snapshots->getSnapshotName($snap->{snapshotref});
        my $parentsnapsize;
        if (!defined($parentsnapshotname)) {
          $parentsnapshotname = "deleted";
          $parentsnapsize = 'N/A';
          $parentname = 'N/A';
        } else {
          $parentname = $databases->getDB($snapshots->getSnapshotContainer($parentsnapshotref))->getName();
          if (!defined($snapshot_sizes{$parentsnapshotref})) {
            $parentsnapsize = $snapshots->getSnapshotSize($parentsnapshotref);
            if (defined($parentsnapsize)) {
              $parentsnapsize = Toolkit_helpers::print_size($parentsnapsize, 'B', $output_unit);
            } else {
              $parentsnapsize = 'N/A';
            }
            $snapshot_sizes{$snap->{snapshotref}} = $parentsnapsize;
          } else {
            $parentsnapsize = $snapshot_sizes{$parentsnapshotref};
          }
        }

        my $snapsize;
        if (!defined($snapshot_sizes{$snap->{snapshotref}})) {
          $snapsize = $cont_snapshots->getSnapshotSize($snap->{snapshotref});
          if (defined($snapsize)) {
            $snapsize = Toolkit_helpers::print_size($snapsize, 'B', $output_unit);
          } else {
            $snapsize = 'N/A';
          }
          $snapshot_sizes{$snap->{snapshotref}} = $snapsize;
        } else {
          $snapsize = $snapshot_sizes{$snap->{snapshotref}};
        }

        $output->addLine(
          $engine,
          $bookmarks->getName($book),
          $templates->getName($containers->getJSContainerTemplate($workcon)),
          $containers->getName($workcon),
          $dbobj->getName(),
          $branchname,
          $contsnapshotname,
          $snapsize,
          $parentsnapshotname,
          $parentsnapsize,
          $parentname
        );



      }
      # end of bookmark loop

    }
    # end of database loop for timelines

  }
  # end of conteiner loop

}
# end of engine loop


Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_get_js_snapshots    [-engine|d <delphix identifier> | -all ]
                        [-template_name template_name]
                        [-container_name container_name]
                        [-output_unit K|M|G|T]
                        [-format csv|json ]
                        [-help|? ] [ -debug ]

=head1 DESCRIPTION

Display a snapshot information for timelines and bookmarks in Self service
for particular container.

Output column description:

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
Limit display to containers using a template template_name

=item B<-container_name container_name>
Limit display to containers using container_name

=item B<-output_unit K|M|G|T>
Display usage using different unit. By default GB are used
Use K for KiloBytes, G for GigaBytes and M for MegaBytes, T for TeraBytes

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

List snapshots for all containers

  dx_get_js_snapshots -d Landshark5

  Appliance            Bookmark/timeline                                  Template name   Container name  VDB name        Branch name     Bookmark snapshot              Bookmark snap size   Parent snapshot                Parent snap size
  -------------------- -------------------------------------------------- --------------- --------------- --------------- --------------- ------------------------------ -------------------- ------------------------------ --------------------
  Landshark5           CREATE_BRANCH / 2019-01-07 12:59:56 GMT            tempdx          con1            con1            default         N/A                            N/A                  @2018-12-27T11:30:04.663Z             14.77
  Landshark5           CREATE_BRANCH / 2019-01-07 13:39:01 GMT            tempdx          con1            con1            version_2.3     N/A                            N/A                  @2018-12-27T11:30:04.663Z             14.77
  Landshark5           REFRESH / 2019-01-07 13:48:17 GMT                  tempdx          con1            con1            default         N/A                            N/A                  @2019-01-07T12:59:11.417Z              4.90
  Landshark5           firstbook                                          tempdx          con1            con1            default         @2019-01-07T13:38:50.118Z              0.67         @2018-12-27T11:30:04.663Z             14.77
  Landshark5           beforerefresh                                      tempdx          con1            con1            default         @2019-01-07T13:48:12.731Z              0.50         @2018-12-27T11:30:04.663Z             14.77
  Landshark5           CREATE_BRANCH / 2019-01-07 13:08:14 GMT            tempdx          con2            con2            default         N/A                            N/A                  @2019-01-07T12:59:11.417Z              4.90
  Landshark5           con2_bookmark                                      tempdx          con2            con2            default         @2019-01-07T14:10:20.718Z              0.47         @2019-01-07T12:59:11.417Z              4.90

List snapshots from container - con2

  dx_get_js_snapshots -d Landshark5 -container_name con2

  Appliance            Bookmark/timeline                                  Template name   Container name  VDB name        Branch name     Bookmark snapshot              Bookmark snap size   Parent snapshot                Parent snap size
  -------------------- -------------------------------------------------- --------------- --------------- --------------- --------------- ------------------------------ -------------------- ------------------------------ --------------------
  Landshark5           CREATE_BRANCH / 2019-01-07 13:08:14 GMT            tempdx          con2            con2            default         N/A                            N/A                  @2019-01-07T12:59:11.417Z              4.90
  Landshark5           RESTORE / 2019-01-07 14:10:31 GMT                  tempdx          con2            con2            default         N/A                            N/A                  @2019-01-07T12:59:11.417Z              4.90
  Landshark5           con2_bookmark                                      tempdx          con2            con2            default         @2019-01-07T14:10:20.718Z              0.47         @2019-01-07T12:59:11.417Z              4.90

List snapshots for a container with two databases

  dx_get_js_snapshots -d Landshark5 -container_name con_complex

  Appliance            Bookmark/timeline                                  Template name   Container name  VDB name        Branch name     Bookmark snapshot              Bookmark snap size   Parent snapshot                Parent snap size
  -------------------- -------------------------------------------------- --------------- --------------- --------------- --------------- ------------------------------ -------------------- ------------------------------ --------------------
  Landshark5           CREATE_BRANCH / 2019-01-07 14:58:51 GMT            t2sources       con_complex     con1            default         N/A                            N/A                  @2019-01-07T12:59:11.417Z              9.53
  Landshark5           RESTORE / 2019-01-07 15:03:53 GMT                  t2sources       con_complex     con1            default         N/A                            N/A                  @2019-01-07T12:59:11.417Z              9.53
  Landshark5           b1_complex                                         t2sources       con_complex     con1            default         @2019-01-07T15:03:46.260Z              0.40         @2019-01-07T12:59:11.417Z              9.53
  Landshark5           CREATE_BRANCH / 2019-01-07 14:58:51 GMT            t2sources       con_complex     Vpubs3AWL       default         N/A                            N/A                  @2019-01-02T13:50:00.000               0.03
  Landshark5           RESTORE / 2019-01-07 15:03:53 GMT                  t2sources       con_complex     Vpubs3AWL       default         N/A                            N/A                  @2019-01-02T13:50:00.000               0.03
  Landshark5           b1_complex                                         t2sources       con_complex     Vpubs3AWL       default         @2019-01-07T15:03:41.980               0.03         @2019-01-02T13:50:00.000               0.03

=cut
