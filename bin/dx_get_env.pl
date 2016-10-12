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
# Copyright (c) 2014,2016 by Delphix. All rights reserved.
#
# Program Name : dx_get_env.pl
# Description  : Get database and host information
# Author       : Edward de los Santos
# Created      : 30 Jan 2014 (v1.0.0)
#
#
# Modified: 14 Mar 2015 (v2.0.0) Marcin Przepiorowski
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
use Environment_obj;
use Toolkit_helpers;
use Repository_obj;
use Host_obj;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'name|n=s' => \(my $envname),
  'reference|r=s' => \(my $reference),
  'config' => \(my $config),
  'backup=s' => \(my $backup),
  'replist' => \(my $replist),
  'nohead' => \(my $nohead),
  'format=s' => \(my $format),
  'debug:i' => \(my $debug),
  'save=s' => \(my $save),
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

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $output = new Formater();

if (defined($backup)) {
  if (! -d $backup) {
    print "Path $backup is not a directory \n";
    exit (1);  
  }
  if (! -w $backup) {
    print "Path $backup is not writtable \n";
    exit (1);  
  }
}


if (defined($replist)) {
  $output->addHeader(
    {'Appliance', 20},
    {'Environment Name',  30},
    {'Repository list',   30}
  );
} elsif (defined($config)) {
  $output->addHeader(
    {'Appliance',         20},
    {'Environment Name',  30},
    {'Type',              25},
    {'Host name',         30},
    {'User Name',         30},
    {'Auth Type',         30},
    {'Toolkit dir',       60},
    {'Proxy',             30}
  );
} elsif (defined($backup)) {
  $output->addHeader(
    {'Command',        200}
  )  
}  
else {
  $output->addHeader(
    {'Appliance', 20},
    {'Reference', 30},
    {'Environment Name',  30},
    {'Type',      25},
    {'Status',     8}
  );
}


my %save_state;

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  # load objects for current engine
  my $environments = new Environment_obj( $engine_obj, $debug);
  my $repository_obj = new Repository_obj($engine_obj, $debug);
  my $hosts;

  if (defined($config) || defined($backup)) {
    $hosts = new Host_obj ( $engine_obj, $debug );
  }

  # filter implementation

  my @env_list;

  if (defined($reference)) {
    push(@env_list, $reference);
  }
  elsif (defined($envname)) {
    if (defined($environments->getEnvironmentByName($envname))) {
      push(@env_list, $environments->getEnvironmentByName($envname)->{reference});
    } else {
      $ret = $ret + 1;
    }
  } else {
    @env_list = $environments->getAllEnvironments();
  };

  # for filtered databases on current engine - display status
  for my $envitem ( @env_list ) {

    if (defined($replist)) {
      $output->addLine(
        $engine,
        $environments->getName($envitem),
        ''
      );
      my $reparray = $repository_obj->getRepositoryByEnv($envitem);
      for my $repitem (@{$reparray}) {
        $output->addLine(
          '',
          '',
          $repository_obj->getName($repitem)
        );
      }

    } elsif (defined($config) || defined($backup)) {

      my $envtype = $environments->getType($envitem);
      my $host_ref = $environments->getHost($envitem);
      if ($envtype eq 'rac') {
        my $clusenvnode = $environments->getClusterNode($envitem);
        $host_ref = $environments->getHost($clusenvnode);
      } 
      my $hostname = $hosts->getHostAddr($host_ref);
      my $user = $environments->getPrimaryUserName($envitem);
      my $toolkit = $hosts->getToolkitpath($host_ref);
      if (!defined($toolkit)) {
        $toolkit = 'N/A';
      }
      my $proxy_ref = $environments->getProxy($envitem);
      my $proxy;
      if ($proxy_ref eq 'N/A') {
        $proxy = 'N/A';
      } else {
        $proxy = $hosts->getHostAddr($proxy_ref);
      }

      my $envname = $environments->getName($envitem);
      my $userauth = $environments->getPrimaryUserAuth($envitem);

      if (defined($backup)) {
        my $suffix = '';
        if ( $^O eq 'MSWin32' ) { 
          $suffix = '.exe';
        }
        
        my $restore_args = "dx_create_env$suffix -d $engine -envname $envname -envtype $envtype -host $hostname -username \"$user\" -authtype $userauth -password ChangeMe ";
        if ($toolkit eq 'N/A') {
          $restore_args = $restore_args . "-proxy $proxy";
        } else {
          $restore_args = $restore_args . "-toolkitdir \"$toolkit\"";
        }
        
        if ($envtype eq 'rac') {
          my $clusloc = $environments->getClusterloc($envitem);
          my $clustname = $environments->getClusterName($envitem);
          $restore_args = $restore_args . " -clusterloc $clusloc -clustername $clustname ";
        }
        
        my $asedbuser =  $environments->getASEUser($envitem);
        if ($asedbuser ne 'N/A') {
          $restore_args = $restore_args . " -asedbuser $asedbuser -asedbpass ChangeMeDB ";
        }
        
        $output->addLine(
          $restore_args
        );
      } else {
        $output->addLine(
          $engine,
          $envname,
          $envtype,
          $hostname,
          $user,
          $userauth,
          $toolkit,
          $proxy
        );
      }
    } else {
      $output->addLine(
        $engine,
        $envitem,
        $environments->getName($envitem),
        $environments->getType($envitem),
        $environments->getStatus($envitem),
      );
    }

    $save_state{$envitem} = $environments->getStatus($envitem);

  }

  if ( defined($save) ) {
    # save file format - userspecified.enginename
    my $save_file = $save . "." . $engine;
    open (my $save_stream, ">", $save_file) or die ("Can't open file $save_file for writting : $!" );
    print $save_stream to_json(\%save_state, {pretty => 1});
    close $save_stream;
  }

}

if (defined($backup)) {
  my $FD;
  my $filename = File::Spec->catfile($backup,'backup_env.txt');
  
  if ( open($FD,'>', $filename) ) {
    $output->savecsv(1,$FD);
    print "Backup exported into $filename \n";
  } else {
    print "Can't create a backup file $filename \n";
    $ret = $ret + 1;
  }
  close ($FD);
} else {
  Toolkit_helpers::print_output($output, $format, $nohead);
}

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_get_env.pl [ -engine|d <delphix identifier> | -all ] [ -name env_name | -reference reference ] [-backup] [ -replist ] [  -format csv|json ]  [ -help|? ] [ -debug ]

=head1 DESCRIPTION

Get the information about host environment.

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=head2 Filters

=over 4

=item B<-name>
Environment Name

=item B<-reference>
Environment reference


=back

=head1 OPTIONS

=over 3

=item B<-backup>
Display dxtoolkit commands to recreate environments ( support for SI Oracle / MS SQL )


=item B<-replist>
Display repository list (Orcle Home / MS SQL instance / etc ) for environment

=item B<-format>
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back




=cut
