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
# Program Name : dx_get_bookmarks.pl
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
  'format=s' => \(my $format), 
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



# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $output = new Formater();


$output->addHeader(
    {'Appliance',      20},
    {'Bookmark name',  20},
    {'Timestamp',      40},
    {'Timeflow name',  40},
    {'Database name',  40}  
);


for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  my $db = new Databases ( $engine_obj, $debug );

  my $bookmarks = new Bookmark_obj ($engine_obj, $db, $debug );






  for my $bookitem (@{$bookmarks->getBookmarks()}) {

    my $bookmark_data = $bookmarks->getBookmarkTimestamp($bookitem);

    if (defined($dbname)) {

      # if like is defined we are going to resolve only ones maching like
      if ( ! ($bookmark_data->{object_name} =~ m/\Q$dbname/)  ) {
        next;
      } 

    }

    if (defined($name)) {

      # if like is defined we are going to resolve only ones maching like
      if ( ! ($bookitem =~ m/\Q$name/)  ) {
        next;
      } 

    }

    $output->addLine(
        $engine,
        $bookitem,
        $bookmark_data->{timestamp},
        $bookmarks->getBookmarkTimeflow($bookitem),
        $bookmark_data->{object_name}
    );



  }

}

Toolkit_helpers::print_output($output, $format, $nohead);



__DATA__

=head1 SYNOPSIS

 dx_get_bookmarks.pl [ -engine|d <delphix identifier> | -all ] [-name bookmark_name] [-dbname database_name] 
                  [ -format csv|json ]  [ --help|? ] [ -debug ]

=head1 DESCRIPTION

Get the list of bookmarks from Delphix Engine.

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=head2 Filters

Filter bookmarks using one of the following filters

=over 4

=item B<-name bookmark_name>
Bookmark name to create or delete

=item B<-dbname database_name>
Name of database to create bookmark for

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

List bookmarks for database "Employee Oracle 11G DB"

 dx_get_bookmarks -d Landshark43 -dbname "Employee Oracle 11G DB"
 Appliance            Bookmark name        Timestamp                             Timeflow name                            Database name
 -------------------- -------------------- ------------------------------------- ---------------------------------------- ------------------------
 Landshark43          after                2016-02-29 12:50:00 EST               default                                  Employee Oracle 11G DB
 Landshark43          before               2016-02-29 07:46:00 EST               default                                  Employee Oracle 11G DB
 Landshark43          middle               2016-02-11 07:54:00 EST               default                                  Employee Oracle 11G DB

List all bookmarks for a Delphix Engine

 dx_get_bookmarks -d Landshark5

 Appliance            Bookmark name        Timestamp                              Timeflow name                            Database name
 -------------------- -------------------- -------------------------------------- ---------------------------------------- -----------------------
 Landshark5           bookmark now         2016-04-21 11:57:41 IST                DB_PROVISION@2016-04-20T12:57:31         testdx
 Landshark5           nonjs                2016-04-20 12:58:41 IST                DB_PROVISION@2016-04-20T12:57:31         testdx
 Landshark5           test bookmark        2016-04-20 12:58:41 IST                DB_PROVISION@2016-04-20T12:57:31         testdx




=cut



