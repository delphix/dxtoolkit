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
'encryptedconfig|e=s' => \(my $encryptedconfig),
'version|v' => \(my $print_version)   
) or pod2usage(-verbose => 2, -output=>\*STDERR);


pod2usage(-verbose => 2, -output=>\*STDERR) && exit if $help;
die  "$version\n" if $print_version;  


if (! ( defined ($plainconfig) && defined($encryptedconfig) ) ) {
	print "Parameters plainconfig and encryptedconfig are required.\n";
	pod2usage(-verbose => 2, -output=>\*STDERR);
	exit;
}

my ($dlpxObject,$rc) = new Engine ('1.4',$debug);
$dlpxObject->load_config($plainconfig);
$dlpxObject->encrypt_config($encryptedconfig);



__END__


=head1 SYNOPSIS

 dx_encrypt.pl -plainconfig|p non_encrypted_config -encryptedconfig|e encrypted_config [-help] [-version]

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


=back


=head1 OPTIONS

=over 2

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=back



=cut
