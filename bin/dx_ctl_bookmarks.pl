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
# Program Name : dx_ctl_bookmarks.pl
# Description  : Get Delphix Engine timeflow bookmarks
# Author       : Marcin Przepiorowski
# Created: 02 Mar 2016 (v2.2.3)
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
use Bookmark_obj;
use Databases;

my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'name=s' => \(my $name),
  'dbname=s' => \(my $dbname),
  'group=s' => \(my $group),
  'action=s' => \(my $action),
  'timestamp=s' => \(my $timestamp),
  'all' => (\my $all),
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
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


if ( (!defined($action) )  || (! ( ( $action eq 'create') || ( $action eq 'delete')  ) ) )  {
  print "Option -action is not provided or has invalid parameter \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


if ( (lc $action eq 'create') && ( ! (defined($timestamp) && defined($dbname) && defined($name)  ) ) ) {
  print "Options -name, -dbname and -timestampe are required to create bookmark. \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if ( (lc $action eq 'delete') && (! defined($name)  ) ) {
  print "Options -name is required to drop bookmark. \n";
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
    next;
  };

  my $db = new Databases ( $engine_obj, $debug );
  my $groups = new Group_obj($engine_obj, $debug); 
  my $bookmarks = new Bookmark_obj ($engine_obj, $db, $debug );


  if (lc $action eq 'create') {
    #my $db = new Databases ( $engine_obj, $debug );

    #my $bookmarks = new Bookmark_obj ($engine_obj, $db );

    my $db_list = Toolkit_helpers::get_dblist_from_filter(undef, $group, undef, $dbname, $db, $groups, undef, undef);

    if (! defined($db_list)) {
      print "There is no DB selected to process on $engine . Please check filter definitions. \n";
      $ret = $ret + 1;
      next;
    }

    if ( ( scalar (@{$db_list} ) < 1 ) )  {
      print "Can't find database $dbname \n";
      $ret = $ret + 1;
      next;
    }

    if ( ( scalar (@{$db_list} ) > 1 ) )  {
      print "More than one database found. Please use dbname and group to select a database - $dbname \n";
      $ret = $ret + 1;
      next;
    }

    if ($bookmarks->createBookmark($db->getDB($db_list->[0]), $name, $timestamp)) {
      print "Can't create bookmark $name for database $dbname using timestamp $timestamp \n";
      $ret = $ret + 1;
      next;
    }

  } elsif (lc $action eq 'delete') {
    if ($bookmarks->deleteBookmark( $name ) ) {
      print "Can't drop bookmark $name \n";
      $ret = $ret + 1;
      next;
    }
  }


}




__DATA__

=head1 SYNOPSIS

 dx_ctl_bookmarks [ -engine|d <delphix identifier> | -all ] 
                     -action [ create | delete ] 
                     -name bookmarkname 
                     [-dbname database_name]
                     [-group group_name]
                     [-timestamp now|latest|'yyyy-mm-dd hh24:mi:ss']  
                     [-help|? ] 
                     [-debug ]


=head1 DESCRIPTION

Create or delete bookmark in current timeflow of database

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=item B<-action create | delete>
Specify an action

=item B<-name bookmark_name>
Bookmark name to create or delete

=item B<-dbname database_name>
Name of database to create bookmark for

=item B<-group group_name>
Name of group with database - only if dbname is not unique 

=item B<-timestamp timestamp>
Point in time to create bookmark for. Possible formats
- now - current time
- latest - time of latest snapshot
- "yyyy-mm-dd hh24:mi:ss" - point in time

=back

=head1 OPTIONS

=over 3

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=item B<-nohead>
Turn off header output

=back

=head1 EXAMPLES

Create bookmark named "TEST BOOKMARK" for database TESTDX on a last snapshot

 dx_ctl_bookmarks -d Landshark5 -action create -name "test bookmark" -dbname testdx -timestamp latest 
 Bookmark test bookmark for time 2016-04-20 12:58:41 has been created

Create bookmark named "BOOKMARK NOW" for database TESTDX on a current time. 
Timezone of engine is used to create a point in time.

 dx_ctl_bookmarks -d Landshark5 -action create -name "bookmark now" -dbname testdx -timestamp now 
 Bookmark bookmark now for time 2016-04-21T10:57:41.000Z has been created



=cut



