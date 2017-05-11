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
# Program Name : Formater.pm
# Description  : Module to print data into different formats
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#


package Formater;

use warnings;
use strict;
use Data::Dumper;
use Date::Manip;
use JSON;
use Toolkit_helpers qw (logger);

# constructor
# parameters 
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $debug = shift;
    logger($debug, "Entering Formater::constructor",1);

    my @lines;
    my $self = {
        _lines => \@lines,
        _debug => $debug
    };
    
    bless($self,$classname);
    return $self;
}


# procedure sortbycolumn
# parameters 
# - column number
# Sort output by column

sub sortbynumcolumn {
	my $self = shift;
	my $columnno = shift;

    logger($self->{_debug}, "Entering Engine::sortbycolumn",1);	

    my $oldlines = $self->{_lines};
    my @sortedlines = sort { Toolkit_helpers::sortcol_by_number($a, $b, $columnno) } @{$oldlines};

    $self->{_lines} = \@sortedlines;

}

# procedure sortbytextcolumn
# parameters 
# - column number
# Sort output by column

sub sortbytextcolumn {
	my $self = shift;
	my $columnno1 = shift;
	my $columnno2 = shift;
	my $columnno3 = shift;

    logger($self->{_debug}, "Entering Engine::sortbytextcolumn",1);	

    my $oldlines = $self->{_lines};
    my @sortedlines;

    if ( defined($columnno1) && defined($columnno2) && defined($columnno3) ) {
    	@sortedlines = sort { ( $a->[$columnno1] . $a->[$columnno2] . $a->[$columnno3] ) cmp ( $b->[$columnno1] . $b->[$columnno2] . $b->[$columnno3] )  } @{$oldlines};
    } 
    elsif ( defined($columnno1) && defined($columnno2) ) {
    	@sortedlines = sort { ( $a->[$columnno1] . $a->[$columnno2] ) cmp ( $b->[$columnno1] . $b->[$columnno2] )  } @{$oldlines};
    } 
    else {
    	@sortedlines = sort { $a->[$columnno1] cmp $b->[$columnno1] } @{$oldlines};
    }


    $self->{_lines} = \@sortedlines;

}

# Procedure print
# parameters: 
# -nohead - skip header
# Print data into screen using formating defined in header


sub print {
	my $self = shift;
	my $nohead = shift;
	my $file = shift;
    logger($self->{_debug}, "Entering Engine::print",1);
    logger($self->{_debug}, "Format " .  $self->{_format},2);
    my $FD;

    if (defined ($file)) {
        $FD = $file;
    } else {
        $FD = \*STDOUT;
    };

    print $FD "\n";

    if ( ! defined($nohead) ) {
		printf $FD $self->{_format}, @{$self->{_header}};
		print $FD $self->{_sepline} . "\n";
	}
	for my $line ( @{$self->{_lines}} ) {
		printf $FD $self->{_format}, @{$line};
	}
}


# Procedure savejson
# parameters: 
# -nohead - skip header
# Print data into screen using formating defined in header


sub savejson {
	my $self = shift;
	my $file = shift;
    logger($self->{_debug}, "Entering Engine::savejson",1);
    logger($self->{_debug}, "Format " .  $self->{_format},2);
    my $FD;

    if (defined ($file)) {
        $FD = $file;
    } else {
        $FD = \*STDOUT;
    };

    my @results;
            
	for my $line ( @{$self->{_lines}} ) {
		my %json_line;
		for (my $i=0; $i < scalar(@{$line}); $i++) {
			$json_line{ $self->{_header}[$i] } = @{$line}[$i];
		}
		push (@results, \%json_line);
	}

	my $json = new JSON();

	my $json_data =  $json->pretty->encode( {results => \@results} );
	print $FD "$json_data\n";

}

# Procedure savecsv
# parameters: 
# -nohead - skip header
# Print data into screen using formating defined in header


sub savecsv {
	my $self = shift;
	my $nohead = shift;
	my $file = shift;
    logger($self->{_debug}, "Entering Engine::savecsv",1);
    logger($self->{_debug}, "Format " .  $self->{_format},2);
    my $FD;

    if (defined ($file)) {
        $FD = $file;
    } else {
        $FD = \*STDOUT;
    };
    


    if ( ! defined($nohead) ) {
		print $FD "#" . join(',',@{$self->{_header}});
		print $FD "\n";
	}
	for my $line ( @{$self->{_lines}} ) {
		print $FD join(',',@{$line});
		print $FD "\n";
	}
}


# Procedure sendtosyslog
# Send data into syslog
# - handler - syslog_wrap class


sub sendtosyslog {
	my $self = shift;
	my $handler = shift;
  logger($self->{_debug}, "Entering Engine::sendtosyslog",1);
  logger($self->{_debug}, "Format " .  $self->{_format},2);
  
  
  my $json = new JSON();
  #this is for sort if necessary
  #$json->canonical();
  my $timestamp;
  for my $line ( @{$self->{_lines}} ) {
		my %json_line;
		for (my $i=0; $i < scalar(@{$line}); $i++) {
			$json_line{ $self->{_header}[$i] } = @{$line}[$i];
      if ($self->{_header}[$i] eq 'StartTime') {
        print Dumper @{$line}[$i];
        $timestamp = UnixDate( ParseDate(@{$line}[$i]), "%s" );
      }
      if ($self->{_header}[$i] eq 'Appliance') {
        print Dumper @{$line}[$i];
        $handler->setDE(@{$line}[$i]);
      }
		}
    if (!defined($timestamp)) {
      $timestamp = time;
    }
		my $json_data =  $json->encode( \%json_line );
    $handler->send($json_data, $timestamp);
  }
  
}


# Procedure addLine
# parameters: 
# - array of columns
# Adding a line with columns defined in parameter to internal array

sub addLine {
	my $self = shift;
	my @columns = @_;
    logger($self->{_debug}, "Entering Engine::addLine",1);	

	push(@{$self->{_lines}}, \@columns);
}

# Procedure addHeader
# parameters: 
# - array of hashes { col_name : size }
# Adding a header and format defined in parameter to internal array

sub addHeader {
	my $self = shift;
	my @columns = @_;
    logger($self->{_debug}, "Entering Engine::addHeader",1);		
	my $format = '';
	my $sepline = '';
	my @header;

	for my $col (@columns) {
		push (@header, keys %{$col});
		for my $value ( values %{$col} ) {
			$format = $format . "%-" . $value . "." . $value  . "s ";
			$sepline = $sepline . "-" x $value . " ";
		}
	};

	$format = $format . "\n";

	$self->{_format} = $format;
	$self->{_header} = \@header;
	$self->{_sepline} = $sepline;

}

# End of module;
1;