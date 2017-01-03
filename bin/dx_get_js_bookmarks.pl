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
  'debug:i' => \(my $debug)
) or pod2usage(-verbose => 1,  -input=>\*DATA);

pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   

my $engine_obj = new Engine ($dever, $debug);
my $path = $FindBin::Bin;
my $config_file = $path . '/dxtools.conf';

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
      my $realtime = $datasources->checkTime($obj_ref, $t);

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



