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
# Program Name : dx_ctl_template.pl
# Description  : Export DB templates
# Author       : Marcin Przepiorowski
# Created      : 14 April 2015 (v2.1.0)

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
use Template_obj;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'filename|n=s' => \(my $filename), 
  'indir=s' => \(my $indir),
  'import' => \(my $import),
  'update' => \(my $update),
  'debug:i' => \(my $debug), 
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'version' => \(my $print_version)
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

if (defined($filename) && defined($indir) ) {
  print "Option filename and indir are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if ( ( ! defined($filename)  ) && ( ! defined($indir) ) ) {
  print "Option filename or indir is required \n";
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

  # load objects for current engine
  my $templates = new Template_obj ( $engine_obj, $debug );

  my @template_list;

  if (defined($filename)) {
    if (defined($import)) {
      if ($templates->importTemplate($filename)) {
        print "Problem with load template from file $filename\n";
        $ret = $ret + 1;
        next;
      }
    } elsif (defined($update)) {  
      if ($templates->updateTemplate($filename)) {
        print "Problem with update template from file $filename\n";
        $ret = $ret + 1;
        next;
      }
    }
  } else {
    opendir (my $DIR, $indir) or die ("Can't open a directory $indir : $!");

    while (my $file = readdir($DIR)) {
        # take only .template files
        if ($file =~ m/\.template$/) {
          my $filename = $indir . "/" . $file;
          if (defined($import)) {
            if ($templates->importTemplate($filename)) {
              print "Problem with load template from file $filename\n";
              $ret = $ret + 1;
            }
          } elsif (defined($update)) {  
            if ($templates->updateTemplate($filename)) {
              print "Problem with update template from file $filename\n";
              $ret = $ret + 1;
            }
          }        
        }
    }

    closedir ($DIR);
  }

}

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_ctl_template.pl [ -engine|d <delphix identifier> | -all ] 
                    -import | -update  
                    [-filename filename | -indir dir]  
                    [-help|?] 
                    [-debug] 

=head1 DESCRIPTION

Import or update a VDB template from file name or directory.

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=head1 OPTIONS

=over 3

=item B<-import>                                                                                                                                            
Import template from file or directory

=item B<-update>                                                                                                                                            
Update template from file or directory

=item B<-filename>
Template filename

=item B<-indir>                                                                                                                                            
Location of imported templates files


=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Import VDB template from test.template file

 dx_ctl_template -d Landshark -import -filename ./test.template 
 Importing template from file ./test.template. Import completed

Update VDB template using file test.template

 dx_ctl_template -d Landshark -update -filename ./test.template 
 Updating template new from file ./test.template. Update completed
 
Import VDB templates from directory /tmp/test

 dx_ctl_template -d Landshark -update -indir /tmp/test/
 Updating template Dev Template from file /tmp/test//Dev Template.template. Update completed
 Updating template GBC Template from file /tmp/test//GBC Template.template. Update completed
 Updating template new from file /tmp/test//new.template. Update completed
 Updating template QA Template from file /tmp/test//QA Template.template. Update completed
 Updating template Training Template from file /tmp/test//Training Template.template. Update completed



=cut



