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
# Program Name : dx_ctl_policy.pl
# Description  : Import policy 
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
use Policy_obj;
use Toolkit_helpers;
use Databases;
use Group_obj;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'filename|n=s' => \(my $filename), 
  'indir=s' => \(my $indir),
  'import' => \(my $import),
  'update' => \(my $update),
  'mapping=s' => \(my $mapping),
  'debug:i' => \(my $debug), 
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'version' => \(my $print_version),
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

if (defined($filename) && defined($indir) ) {
  print "Option filename and indir are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if ( ( ! defined($filename)  ) && ( ! defined($indir) ) && ( ! defined($mapping) ) ) {
  print "Option filename, indir or mapping is required \n";
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

  # load objects for current engine
  my $policy = new Policy_obj( $engine_obj, $debug);

  if (defined($mapping)) {
    my $db = new Databases ( $engine_obj, $debug );
    my $groups = new Group_obj ( $engine_obj, $debug );
    if ($policy->applyMapping($mapping, $groups, $db)) {
      print "Error in applying mapping\n";
      exit 1;
    }
  } else {

    if (defined($filename)) {
      if (defined($import)) {
        if ($policy->importPolicy($filename)) {
          print "Problem with load policy from file $filename\n";
          exit 1;
        }
      } elsif (defined($update)) {  
        if ($policy->updatePolicy($filename)) {
          print "Problem with update policy from file $filename\n";
          exit 1;
        }
      }
    } else {
      opendir (my $DIR, $indir) or die ("Can't open a directory $indir : $!");

      while (my $file = readdir($DIR)) {
          # take only .template files
          if ($file =~ m/\.policy$/) {
            my $filename = $indir . "/" . $file;
            if (defined($import)) {
              if ($policy->importPolicy($filename)) {
                print "Problem with load policy from file $filename\n";
                $ret = $ret + 1;
              }
            } elsif (defined($update)) {  
              if ($policy->updatePolicy($filename)) {
                print "Problem with update policy from file $filename\n";
                $ret = $ret + 1;
              }
            }        
          }
      }

      closedir ($DIR);
    }
  }


}


exit $ret;

__DATA__

=head1 SYNOPSIS

 dx_ctl_policy   [ -engine|d <delphix identifier> | -all ] -import | -update | -mapping mapping_file [ -filename filename | -indir dir]  [ -help|? ] [ -debug ] 

=head1 DESCRIPTION

Import or update a Delphix Engine policy from file name or directory.

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=item B<-import>                                                                                                                                            
Import policy from file or directory

=item B<-update>                                                                                                                                            
Update policy from file or directory

=item B<-mapping mapping_file>                                                                                                                                            
Apply policy to databases / groups using mapping file mapping_file

=back

=head1 OPTIONS

=over 3


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

Import polices from /tmp/policy directory into engine.

 dx_ctl_policy -d Landshark43 -import -indir /tmp/policy
 Policy Default Retention from file /tmp/policy/Default Retention.policy already exist. Problem with load policy from file /tmp/policy/Default Retention.policy
 Policy Default Snapshot from file /tmp/policy/Default Snapshot.policy already exist. Problem with load policy from file /tmp/policy/Default Snapshot.policy
 Policy Default SnapSync from file /tmp/policy/Default SnapSync.policy already exist. Problem with load policy from file /tmp/policy/Default SnapSync.policy
 Policy jsontest from file /tmp/policy/jsontest.policy already exist.
 Problem with load policy from file /tmp/policy/jsontest.policy
 Importing policy from file /tmp/policy/marcintest.policy. Import completed
 Importing policy from file /tmp/policy/test.policy. Import completed
 Policy www from file /tmp/policy/www.policy already exist.
 Problem with load policy from file /tmp/policy/www.policy


Update existing polices using files from directory /tmp/policy

 dx_ctl_policy -d Landshark43 -update -indir /tmp/policy
 Updating policy Default Retention from file /tmp/policy/Default Retention.policy. Update completed Updating policy Default Snapshot from file /tmp/policy/Default Snapshot.policy. Update completed Updating policy Default SnapSync from file /tmp/policy/Default SnapSync.policy. Update completed Updating policy jsontest from file /tmp/policy/jsontest.policy. Update completed
 Updating policy marcintest from file /tmp/policy/marcintest.policy. Update completed
 Updating policy test from file /tmp/policy/test.policy. Update completed
 Updating policy www from file /tmp/policy/www.policy. Update completed

Apply polices to Delphix Engine objects using a mapping file

 dx_ctl_policy -d Landshark43 -mapping /tmp/policy/mapping.Landshark Database Masking in group Analytics doesn't exist. Skipping Database Masking in group Analytics doesn't exist. Skipping Database racdb in group Sources doesn't exist. Skipping
 Database racdb in group Sources doesn't exist. Skipping
 Applying policy Default Retention to database Employee Oracle 11G DB 
 Apply completed 
 Applying policy Default SnapSync to database Employee Oracle 11G DB 
 Apply completed

=cut



