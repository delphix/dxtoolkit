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
# Program Name : dx_get_template.pl
# Description  : Export DB templates
# Author       : Marcin Przepiorowski
# Created      : 14 April 2015 (v2.1.0)
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
use Template_obj;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'name|n=s' => \(my $name),
  'outdir=s' => \(my $outdir),
  'export' => \(my $export),
  'parameters' => \(my $parameters),
  'compare=s' => \(my $compare),
  'debug:i' => \(my $debug),
  'all' => (\my $all),
  'dever=s' => \(my $dever),
  'version' => \(my $print_version),
  'nohead' => \(my $nohead),
  'format=s' => \(my $format),
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

if (defined($export) && ( ! defined($outdir) ) ) {
  print "Option export require option outdir to be specified \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (defined($parameters) && defined($compare)) {
  print "Option parameters and compare are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (defined($compare) && (!defined($name))) {
  print "Option compare require a template name to be specify \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $output = new Formater();


if (defined($parameters)) {
  $output->addHeader(
    {'Appliance',     20},
    {'Template name', 30},
    {'Parameter name',30},
    {'value',         30}
  );
} elsif (defined($compare)) {
  $output->addHeader(
    {'Appliance',     20},
    {'Parameter',     30},
    {'value in template', 30},
    {'value in init ',30}
  );
} else {
  $output->addHeader(
    {'Appliance',     20},
    {'Template name', 30}
  );
}

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  # load objects for current engine
  my $templates = new Template_obj ( $engine_obj, $debug );

  my @template_list;

  if (defined($name)) {
    my $template = $templates->getTemplateByName($name);
    if (!defined($template)) {
      print "Can't find template - $name\n";
      exit 1;
    }
    push (@template_list, $template);
  } else {
    @template_list = $templates->getTemplateList();
  }



  # for filtered databases on current engine - display status
  for my $tempitem ( @template_list ) {

    if (defined($parameters)) {
      my $paramhash = $templates->getTemplateParameters($tempitem);
      for my $par (sort (keys %{$paramhash})) {
        $output->addLine(
          $engine,
          $templates->getName($tempitem),
          $par,
          $paramhash->{$par}
        );
      }
    } elsif (defined($compare)) {

      my $spfile;
      open($spfile,'<',$compare) or die ('Can\'t open a file: $compare');
      chomp(my @initarray = <$spfile>);
      close $spfile;

      my %init;

      for my $line (@initarray) {
        $line =~ s/^[^.]*\.(.*)/$1/;
        my ($par, $value) = split('=',$line);

        if (defined($value)) {
          if ($par =~ /^#/ ) {
            next;
          } else {
            $init{$par} = $value;
          }
        } else {
          next;
        }
      }

      my ($notininit, $notintemplate, $differnent ) = $templates->compare($tempitem, \%init);

      for my $par (sort ( keys %{$differnent})) {
        $ret = $ret + 1;
        $output->addLine(
          $engine,
          $par,
          $differnent->{$par}->{template},
          $differnent->{$par}->{init}
        )
      }

      for my $par (sort ( keys %{$notininit})) {
        $ret = $ret + 1;
        $output->addLine(
          $engine,
          $par,
          $notininit->{$par},
          'NA'
        )
      }

      for my $par (sort ( keys %{$notintemplate})) {
        $ret = $ret + 1;
        $output->addLine(
          $engine,
          $par,
          'NA',
          $notintemplate->{$par}
        )
      }


    } else {
      $output->addLine(
        $engine,
        $templates->getName($tempitem)
      );
    }

    if (defined($export)) {
      $templates->exportTemplate($tempitem,$outdir);
    }

  }


}

if (!defined($export)) {
  Toolkit_helpers::print_output($output, $format, $nohead);
}

exit $ret;

__DATA__

=head1 SYNOPSIS

 dx_get_template    [ -engine|d <delphix identifier> | -all ]
                    [ -configfile file ]
                    [ -name template_name ]
                    [ -parameters | -compare file]
                    [ -export -outdir dir]
                    [ -format csv|json ]
                    [ -help|? ]
                    [ -debug ]


=head1 DESCRIPTION

List or export database template  from engine. If no template name is specified all templates will be processed.

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

=head2 Filters

=over 4

=item B<-name>
Template name

=back

=head1 OPTIONS

=over 3

=item B<-parameters>
Display parameters from template

=item B<-compare file>
Compare template with init<SID>.ora file ignoring restritcted parameters
Exit code will be 0 if no difference found.

=item B<-export>
Export template into JSON file in outdir directory

=item B<-outdir>
Location of exported templates files

=item B<-format>
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Display list of VDB templates

 dx_get_template -d Landshark43

 Appliance            Template name
 -------------------- ------------------------------
 Landshark43          Training Template
 Landshark43          QA Template
 Landshark43          Dev Template
 Landshark43          new

Export all VDB templates into /tmp/test directory

 dx_get_template -d Landshark -export -outdir /tmp/test/
 Exporting template into file /tmp/test//Dev Template.template
 Exporting template into file /tmp/test//Training Template.template
 Exporting template into file /tmp/test//new.template
 Exporting template into file /tmp/test//GC Template.template
 Exporting template into file /tmp/test//QA Template.template

Compare templare with initSID.ora file

 dx_get_template -d Landshark51  -name test123 -compare initTEST.ora

 Appliance            Parameter                      value in template              value in init
 -------------------- ------------------------------ ------------------------------ ------------------------------
 Landshark51          compatible                     11.2.0.4.0                     12.2.0.1
 Landshark51          sga_target                     522M                           400M
 Landshark51          open_cursors                   300                            NA
 Landshark51          pga_aggregate_target           200M                           NA
 Landshark51          processes                      700                            NA
 Landshark51          remote_login_passwordfile      'EXCLUSIVE'                    NA


=cut
