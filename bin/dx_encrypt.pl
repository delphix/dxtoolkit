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
# Program Name : dx_encrypt.pl
# Description  : Encrypt passwords from dxtools.conf
# Author       : Marcin Przepiorowski
# Created      : 22 May 2015 (v2.0.0)
#


use strict;
use warnings;
use JSON;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev); #avoids conflicts with ex host and help
use File::Basename;
use Pod::Usage;
use FindBin;
use Data::Dumper;
use POSIX qw/strftime/;
use File::Copy;

my $abspath = $FindBin::Bin;

use lib '../lib';
use Engine;

my $version = $Toolkit_helpers::version;
my $host_exists;


GetOptions(
'help|?' => \( my$help),
'debug:n' => \(my $debug),
'plainconfig|p=s' => \(my $plainconfig),
'shared' => \(my $shared),
'encryptedconfig|e=s' => \(my $encryptedconfig),
'version|v' => \(my $print_version)
) or pod2usage(-verbose => 1, -output=>\*STDERR);


pod2usage(-verbose => 2, -output=>\*STDERR) && exit if $help;
die  "$version\n" if $print_version;


if (! ( defined ($plainconfig) && defined($encryptedconfig) ) ) {
	print "Parameters plainconfig and encryptedconfig are required.\n";
	pod2usage(-verbose => 1, -output=>\*STDERR);
	exit;
}

my ($dlpxObject,$rc) = new Engine ('1.4',$debug);
$dlpxObject->load_config($plainconfig, 1);
$dlpxObject->encrypt_config($encryptedconfig, $shared);



__END__


=head1 SYNOPSIS

 dx_encrypt -plainconfig|p non_encrypted_config -encryptedconfig|e encrypted_config [-help] [-version]

=head1 DESCRIPTION

Encrypt config file. Put a plain password with plain configuration file and add parameter encrypted set to true. Only entries with parameter encrypted set to "true" will be encrypted.
Encrypted_config_file is encrypted configuration file which should be distributed with dxtoolkit.


ex.
dx_encrypt -plainconfig dxtools.conf.plain -encryptedconfig dxtools.conf

=head1 ARGUMENTS

=over 3

=item B<-plainconfig|p file>
Non encrypted config file

=item B<-encryptedconfig|e file>
Encrypted config file

=item B<-shared>
Encryption is done without hostname - config file can be shared between different hosts

=back


=head1 OPTIONS

=over 2

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Encrypt password in dxtools.conf.plain file and generate encrypted file dxtools.conf.enc

 $ cat dxtools.conf.plain
   {
      "data" : [ {
                  "protocol" : "http",
									"hostname" : "Landshark2",
                  "default" : "true",
                  "port" : "80",
                  "username" : "delphix_admin",
                  "encrypted" : "true",
                  "password" : "password",
                  "ip_address" : "delphix02"
      } ]
   }

 dx_encrypt -plainconfig dxtools.conf.plain -encryptedconfig dxtools.conf.enc
 New config file dxtools.conf.enc created.

 $ cat dxtools.conf
   {
      "data" : [ {
                  "protocol" : "http",
                  "hostname" : "Landshark2",
                  "default" : "true",
                  "port" : "80",
                  "username" : "delphix_admin",
                  "encrypted" : "true",
                  "password" : "818bd243bee573105b258c36489f351b806ee890eeba928ddb4d704f6e797bb6c1ac057e84c851f2",
                  "ip_address" : "delphix02"
      }]
   }


 Encrypt password in dxtools.conf.plain file and generate encrypted file dxtools.conf.enc for shared config file

 dx_encrypt -plainconfig dxtools.conf.plain -encryptedconfig dxtools.conf -shared
 New config file dxtools.conf created.

=cut
