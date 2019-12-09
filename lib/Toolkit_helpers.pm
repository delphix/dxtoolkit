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

package Toolkit_helpers;
require Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw(logger trim);  # symbols to export on request

use warnings;
use strict;
use Data::Dumper;
use Pod::Usage;
use URI::Escape;
use Date::Manip;
use File::Spec;

use lib '../lib';

our $version = '2.4.4-hf1';

sub logger {
	my $debug = shift;
	my $msg = shift;
	my $verbose = shift;

	if (defined($debug)) {
		if (defined($verbose)) {
			if ($debug >= $verbose) {
				printf "%${verbose}s%s\n", '-', $msg;
			}
		} else {
			printf "%s\n", $msg;
		}
	}

	if (defined($debug) && ( $debug eq 3) ) {
		open (my $fh, ">>", 'debug.log') or die ("Can't open new debug file log.log for write");
		if (defined($verbose)) {
			if ($debug >= $verbose) {
				printf $fh "%${verbose}s%s\n", '-', $msg;
			}
		} else {
			printf $fh "%s\n", $msg;
		}
		close $fh;
	}

}


sub write_to_dir {
	my $output = shift;
	my $format = shift;
	my $nohead = shift;
	my $name = shift;
	my $path = shift;
	my $unique = shift;

	if (! -d $path) {
		print "Path $path is not a directory \n";
		exit (1);
	}

	if (! -w $path) {
		print "Path $path is not writtable \n";
		exit (1);
	}

	my $filename;

	if (defined($unique)) {
		my $datestring = UnixDate('now','%Y%m%d-%H-%M-%S');
		$filename = "$name-$datestring";
	} else {
		$filename = $name;
	}

	if (defined($format)) {
		if (lc $format eq 'csv') {
			$filename = $filename . ".csv";
		} elsif (lc $format eq 'json') {
			$filename = $filename . ".json";
		}
	} else {
		$filename = $filename . ".txt";
	}

	my $fullname = File::Spec->catfile($path,$filename);
	my $FD;
	if ( open($FD,'>', $fullname) ) {
		print_output($output, $format, $nohead, $FD);
		print "Data exported into $fullname \n";
	} else {
		print "Can't create a output file $fullname \n";
		exit 1;
	}
	close ($FD);


}


sub print_output {
  	my $output = shift;
  	my $format = shift;
  	my $nohead = shift;
  	my $FD = shift;

	if (defined($format) && ( lc ($format) eq 'csv') )  {
		$output->savecsv($nohead,$FD);
	}
	elsif (defined($format) && ( lc ($format) eq 'json') )  {
		$output->savejson($FD);
	} else {
		$output->print($nohead,$FD);
	}
}


sub check_format_opions {
	my $format = shift;

	if ( (defined($format) ) && ! ( ($format eq 'csv' ) || ($format eq 'json' ) ) ) {
		print "Option -format has a wrong argument \n\n";
		pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
		exit (1);
	}
}


sub check_filer_options {
	my $required = shift;
	my $type = shift;
	my $group = shift;
	my $host = shift;
	my $dbname = shift;
	my $envname = shift;
	my $dsource = shift;
	my $instancename = shift;



	if (defined($required)) {
		# at least one filter is required
		if ( ! ( defined($type) || defined($group) || defined($host) || defined($dbname) || defined($envname) || defined($dsource) || defined($instancename)  ) )  {
			print "At least one filter option -host, -name, -type, -envname, -dsource or -group is required \n\n";
			pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
			exit (1);
		}
	}
}

sub filter_array {
	my $array = shift;
	my $filter_array = shift;
	my @ret;

	for my $db ( @{$filter_array} ) {
		if ( grep { $_ eq $db } @{$array} ) {
			push (@ret, $db);
		}
	}

	return \@ret;
}

sub waitForJob {
	my $engine_obj = shift;
	my $jobno = shift;
	my $success = shift;
	my $failure = shift;

	my $ret;

	if (defined($jobno)) {

    print "Starting job - $jobno\n";

    my $job = new Jobs_obj($engine_obj,$jobno, 'true');
    my $retjob = $job->waitForJob();
    if ($retjob eq 'COMPLETED') {
      print $success . ".\n";
      $ret = 0;
    } else {
      print "There was a problem with job - $jobno. Job status is $retjob. \nIf there is no error on the screen, try with -debug flag to find a root cause\n";
      $ret =  1;
    }

  } else {
    print "Job wasn't defined. If there is no error on the screen, try with -debug flag to find a root cause.\n";
    $ret =  1;
  }

	return $ret;

}

sub waitForAction {
	my $engine_obj = shift;
	my $jobno = shift;
	my $success = shift;
	my $failure = shift;

	my $ret;
	if (defined($jobno)) {

      # check action status
      # get last hour of actions
      my $st = Toolkit_helpers::timestamp("-5mins", $engine_obj);
      my $action = new Action_obj ($engine_obj, $st, undef, undef, undef, $engine_obj->{_debug});
      $action->loadActionListbyID({ $jobno => 1 });

      print "Waiting for all actions to complete. Parent action is " . $jobno . "\n";
      if ( $action->checkStateWithChild($jobno) eq 'COMPLETED' ) {
          print $success . "\n";
          $ret = 0;
      } else {
          print $failure . "\n";
          $ret = 1;
      }

  } else {
      print $failure . ". No job defined.\n";
      $ret = 1;
  }

	return $ret;
}


sub get_dblist_from_filter {
	my $type = shift;
	my $group = shift;
	my $host = shift;
	my $dbname = shift;
	my $databases = shift;
	my $groups = shift;
	my $envname = shift;
	my $dsource = shift;
	my $primary = shift;
	my $instance = shift;
	my $instancename = shift;
	my $creation_time = shift;
	my $debug = shift;

	my @db_list;

	logger($debug, "Entering Toolkit_helpers::get_dblist_from_filter",1);
	my $msg = sprintf("Toolkit_helpers::get_dblist_from_filter arguments type - %s, group - %s, host - %s, dbname - %s, dSource - %s" , defined($type) ? $type : 'N/A' ,
		               defined($group) ? $group : 'N/A' , defined($host) ? $host : 'N/A' , defined($dbname) ? $dbname : 'N/A', defined($dsource) ? $dsource : 'N/A');
	logger($debug, $msg ,1);

	# get all DB

	if (defined($primary) ) {
		@db_list = sort { Toolkit_helpers::sort_by_dbname($a,$b,$databases,$groups, $debug) } $databases->getPrimaryDB();
	} else {
    @db_list = sort { Toolkit_helpers::sort_by_dbname($a,$b,$databases,$groups, $debug) } $databases->getDBList();
  }

	my $ret = \@db_list;

	if ( defined($host) ) {
    	# get all DB from one host
    	my @hostfilter =  ( $databases->getDBForHost($host, $instance) );
		  logger($debug, "list of DB on host" ,1);
    	logger($debug, join(",", @hostfilter) ,1);
  		$ret = filter_array($ret, \@hostfilter);
  		logger($debug, "list of DB after host filter" ,1);
  		logger($debug, join(",", @{$ret}) ,1);
  	}


	if ( defined($dsource) ) {
    	# get all DB from one host
    	my @hostfilter =  ( $databases->getDBByParent($dsource) );
		  logger($debug, "list of DB on parent " ,1);
    	logger($debug, join(",", @hostfilter) ,1);
  		$ret = filter_array($ret, \@hostfilter);
  		logger($debug, "list of DB after parent filter" ,1);
  		logger($debug, join(",", @{$ret}) ,1);
  	}

	if ( defined($instancename) ) {
    	# get all DB from one host
    	my @hostfilter =  ( $databases->getDBForInstanceName($instancename) );
		  logger($debug, "list of DB on instancename " ,1);
    	logger($debug, join(",", @hostfilter) ,1);
  		$ret = filter_array($ret, \@hostfilter);
  		logger($debug, "list of DB after instancename filter" ,1);
  		logger($debug, join(",", @{$ret}) ,1);
  	}


	if ( defined($envname) ) {
    	# get all DB from one env
    	my @envfilter =  ( $databases->getDBForEnvironment($envname) );
		  logger($debug, "list of DB on env" ,1);
    	logger($debug, join(",", @envfilter) ,1);
  		$ret = filter_array($ret, \@envfilter);
  		logger($debug, "list of DB after env filter" ,1);
  		logger($debug, join(",", @{$ret}) ,1);
  	}

  	if ( defined($type) ) {
    	# get all DB of one type
    	my @typefilter =  ( $databases->getDBByType($type) );
		  logger($debug, "list of DB on env" ,1);
    	logger($debug, join(",", @typefilter) ,1);
    	$ret = filter_array($ret, \@typefilter);
  		logger($debug, "list of DB after type filter" ,1);
  		logger($debug, join(",", @{$ret}) ,1);
  	}

	if ( defined($group) ) {
	    # get all DB of one group
	    my $group_ref = defined($groups->getGroupByName($group)) ? $groups->getGroupByName($group)->{reference} : '';
	    my @groupfilter =  ( $databases->getDBForGroup($group_ref) );
		  logger($debug, "list of DB in group" ,1);
    	logger($debug, join(",", @groupfilter) ,1);
	    $ret = filter_array($ret, \@groupfilter);
  		logger($debug, "list of DB after group filter" ,1);
  		logger($debug, join(",", @{$ret}) ,1);
	 }

	 if ( defined($creation_time) ) {
 	    # get all DB of older than
 	    my @creationfilter =  ( $databases->getDBByCreationTime($creation_time) );
 		  logger($debug, "list of DB in creation_time" ,1);
     	logger($debug, join(",", @creationfilter) ,1);
 	    $ret = filter_array($ret, \@creationfilter);
   		logger($debug, "list of DB after creation filter" ,1);
   		logger($debug, join(",", @{$ret}) ,1);
 	 }

	 if ( defined($dbname) ) {
	 		my @namefilter;

			for my $sdbname ( split(',', $dbname) ) {

		 		for my $db ( @{$databases->getDBByName($sdbname)} ) {
		    		push (@namefilter, $db->getReference());
				}

			}
	    $ret = filter_array($ret, \@namefilter);
	 }

	 if ( scalar(@{$ret}) < 1 ) {
		  #exit (1);
		  $ret = undef;
	 } else {
		 my @newret = sort { Toolkit_helpers::sort_by_dbname($a,$b,$databases,$groups, $debug) } @{$ret};
	 	 $ret = \@newret;
	 }

	 logger($debug, "Finishing Toolkit_helpers::get_dblist_from_filter",1);
	 return $ret;
}


sub get_engine_list {
	my $all = shift;
	my $dx_host = shift;
	my $engine_obj = shift;
	my $sysadmin = shift;

	my @engine_list;

	if (defined($all) ) {
	  # processing all engines
		if (defined($sysadmin)) {
			@engine_list = $engine_obj->getAllSysadminEngines();
		} else {
	  	@engine_list = $engine_obj->getAllNonSysadminEngines();
		}
	} elsif (defined($dx_host)) {
	  # processing one engine
	  push(@engine_list, $dx_host);
	} else {
	  #load default engine(s)
	  @engine_list = $engine_obj->getDefaultEngines();
	}

	if ( scalar(@engine_list) < 1 ) {
	  print "There is no engine selected to process. \n";
	  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
	  exit (1);
	}

	return \@engine_list;
}


sub parallel_job {
	my $jobs = shift;
	my $job;
	my $i;


	my $ret = 0;
	my $jobdone = 0;

#while ( scalar (@{$jobs}) > 0 ) {
 while ( $jobdone < 1 ) {
		sleep 1;

		for ($i=0; $i <(scalar(@{$jobs})); $i++)  {
			$job = $jobs->[$i];
			$job->loadJob();
			my $status = $job->getJobState();

			if (($status eq 'COMPLETED') || ($status eq 'CANCELED') || ($status eq 'FAILED') ) {
				print "Job " . $job->getJobName() . " finished with status " . $status . "\n";
				if ($status ne 'COMPLETED') {
					$ret = $ret + 1;
				}
				splice (@{$jobs}, $i, 1);
				$jobdone = 1;
			}
		}
	}

	return $ret;

}




sub timestamp {
	my $timestamp = shift;
	my $engine = shift;
	my $nourl = shift;
	my $ret;

	my ($year,$mon,$day,$hh,$mi,$ss);

	if (defined($timestamp)) {
		if (( $timestamp =~ /(\d\d\d\d)-(\d\d)-(\d\d) (\d?\d):(\d?\d):(\d\d)/ ) || ( $timestamp =~ /(\d\d\d\d)-(\d\d)-(\d\d)/ ) ) {
			$timestamp = $timestamp;
		} elsif (my ($min) = ($timestamp =~ /-(\d.*)min/)) {
			$timestamp = $engine->getTime($min);
		} elsif (my ($days) = ($timestamp =~ /-(\d.*)day(s?)/)) {
			$timestamp = $engine->getTime($days*24*60);
		}else {
			return $ret;
		}
	} else {
		# - 7 days
		$timestamp = $engine->getTime(7*24*60);
	}

	my $tz = new Date::Manip::TZ;
	my $detz = $engine->getTimezone();
	my $dt = ParseDate($timestamp);
	if ($dt ne '') {
		my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_to_gmt($dt, $detz);
		my $tstz = sprintf("%04.4d-%02.2d-%02.2dT%02.2d:%02.2d:%02.2d.000Z",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);
		if (defined($nourl)) {
			$ret = $tstz;
		} else {
			$ret = uri_escape($tstz);
		}
	}

   return $ret;
}

sub timestamp_to_urits_de_timezone {
	my $timestamp = shift;
	my $engine = shift;

	my $tz = new Date::Manip::TZ;
	my $detz = $engine->getTimezone();

	$timestamp =~ s/\....Z//;
	my $dt = ParseDate($timestamp);

    my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_to_gmt($dt, $detz);
	return sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d %s",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5], $abbrev);

}


sub timestamp_to_timestamp_with_de_timezone {
	my $timestamp = shift;
	my $engine = shift;

	my $tz = new Date::Manip::TZ;
	my $detz = $engine->getTimezone();

	$timestamp =~ s/\....Z//;
	my $dt = ParseDate($timestamp);

    my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_to_gmt($dt, $detz);
	return sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d %s",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5], $abbrev);

}

sub opname_options {
	my $opname = shift;
	my $read = shift;
	my $write = shift;

	if ( defined($opname) && (! ( (lc $opname eq 'b') || (lc $opname eq 'r') || (lc $opname eq 'w') ) ) ) {
	  print "Wrong -opname value $opname \n";
	  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
	  exit (3);
	}

	if ( defined($read) && defined($write) ) {
	  print "Use -read or -write. For read and write operation leave default or set opname='b' \n";
	  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
	  exit (3);
	}

	if (( defined($read) || defined($write) ) && defined($opname) ) {
	  print "Option -opname and -read or -write are mutually exclusive \n";
	  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
	  exit (3);
	}

	if ( defined($read) ) {
	  $opname = 'r';
	}
	elsif ( defined($write) ) {
	  $opname = 'w';
	}
	elsif ( ! defined ($opname) ) {
	  $opname = 'b';
	}


	return $opname;
}


sub nagios_check {
	my $engine = shift;
	my $analytic_list = shift;
	my $name = shift;
	my $metric = shift;
	my $arguments = shift;
	my $resolution = shift;
	my $raw = shift;
	my $crit = shift;
	my $warn = shift;

  	my $analytic;

	if (defined($analytic_list->getAnalyticByName($name))) {
		$analytic = $analytic_list->getAnalyticByName($name);
	} else {
		print "Can't find $name analytic\n";
		exit(3);
	}

	$analytic->getData($arguments, $resolution);
	$analytic->processData(2);
	$analytic->doAggregation();

	my $avg = $analytic->get_avg($metric);


	if (defined($raw)) {
		my $FD = \*STDOUT;
		$analytic->print($FD);
	} else {
		if ($avg eq -1) {
		  print "Unknown: No data for " . $engine . " " . $analytic->metric_desc($metric) . "\n";
		  exit(3);
		} elsif ($avg >= $crit) {
		  print "CRITICAL: " . $engine . " " . $analytic->metric_desc($metric) . " $avg\n";
		  exit(2);
		} elsif ($avg >= $warn) {
		  print "WARNING: " . $engine . " " . $analytic->metric_desc($metric) . " $avg\n";
		  exit(1);
		} else {
		  print "OK: " . $engine . " " . $analytic->metric_desc($metric)  . " $avg\n";
		  exit(0);
		}
	}

}


# procedure sort_by_dbname

sub sort_by_dbname {
	my $a = shift;
	my $b = shift;
	my $databases = shift;
	my $groups = shift;
	my $debug = shift;

	logger($debug, "Entering Toolkit_helpers::sort_by_dbname",1);
	my $dbobj_a = $databases->getDB($a);
	my $dbobj_b = $databases->getDB($b);

	my $dbname_a = lc ($groups->getName($dbobj_a->getGroup()) . $dbobj_a->getName());
	my $dbname_b = lc ($groups->getName($dbobj_b->getGroup()) . $dbobj_b->getName());

	return $dbname_a cmp $dbname_b;

}

# procedure sortcol_by_number

sub sortcol_by_number {
	my $a = shift;
	my $b = shift;
	my $col = shift;
  my $order = shift;

  my ( $anum ) = $a->[$col] =~ /(\d+[.]?\d+)/;
  my ( $bnum ) = $b->[$col] =~ /(\d+[.]?\d+)/;

  if (!defined($order) || (lc $order eq 'asc')) {
    return ( $anum || 0 ) <=> ( $bnum || 0 );
  } else {
    return ( $bnum || 0 ) <=> ( $anum || 0 );
  }
}

# procedure sort_by_number

sub sort_by_number {
	my $a = shift;
	my $b = shift;

  my ( $anum ) = $a =~ /(\d+[.]?\d?)/;
  my ( $bnum ) = $b =~ /(\d+[.]?\d?)/;
  return ( $anum || 0 ) <=> ( $bnum || 0 );
}


# procedure parse cron

sub parse_cron {
	my $cronstring = shift;
	my $ret;

	my @days = qw (Sun Mon Tue Wed Thu Fri Sat );

	if ( $cronstring =~ /(\d\d?\/\d\d?|\d+(?:, ?\d+)*)\s(\*\/\d\d?|\d\d?\/\d\d?|\d+(?:, ?\d+)*)\s(\*\/\d\d?|\*|\d\d?\/\d\d?|\d+(?:, ?\d+)*)\s\?|\*\s\*\s(\d|\*|\?)/ ) {

		if ( my ($sec,$min,$hour,$day) = $cronstring =~ /(\d?\d) (\d?\d) (\d?\d) \? \* (\d)/ ) {
			$ret =  sprintf("%s %2.2d:%2.2d" , $days[$day-1], $hour,$min);
		} else {
			my @cronpart = split(' ',$cronstring);

			# comman separeted list for minutes
			if ($cronpart[1] =~ /^\d+(?:, ?\d+)*$/ ) {
					my $nopattern = 0;
					my $diff;
					my @minutes = sort { $a <=> $b } ( split(',',$cronpart[1]) ) ;

					if (scalar(@minutes)>1) {
						$diff = $minutes[1] - $minutes[0];
						for (my $min=1;$min < scalar(@minutes);$min++) {
							if (( $minutes[$min] - $minutes[$min-1] ) ne $diff ) {
								$nopattern = 1;
							}
						}

						if ($nopattern eq 1) {
							$ret = "on " . join(',', map { sprintf("%2.2d" , $_) } @minutes) . " min ";
						} else {
							$ret = "every $diff min ";
						}

					} else {
						$ret = "on " . join(',', map { sprintf("%2.2d" , $_) } @minutes) . " min ";
					}
			} elsif (my ($diff) = $cronpart[1] =~ /\d\d?\/(\d?\d)/ ) {
				$ret = "every $diff min ";
			} elsif (($diff) = $cronpart[1] =~ /\*\/(\d\d?)/ ) {
				$ret = "every $diff min ";
			}

			# comman separeted list for hours
			if ($cronpart[2] =~ /^\d+(?:, ?\d+)*$/ ) {
				my @hours = sort { $a <=> $b } ( split(',',$cronpart[2]) ) ;
				$ret = $ret . "on " . join(',', map { sprintf("%2.2d" , $_) } @hours) . " h ";
			} elsif (my ($diff) = $cronpart[2] =~ /\*\/(\d\d?)/ ) {
				$ret = $ret . "every $diff h ";
			} elsif ($cronpart[2] =~ /\*/ ) {
				$ret = $ret . "on every hour ";
			}

			# comman separeted list for days
			if ($cronpart[3] =~ /^\d+(?:, ?\d+)*$/ ) {
				my @daysarr = sort { $a <=> $b } ( split(',',$cronpart[3]) ) ;
				$ret = $ret . "on " . join(',', map { sprintf("%s" , $days[$_] ) } @daysarr) . " h ";
			} elsif ( my ($diff) = $cronpart[3] =~ /\*\/(\d\d?)/ ) {
				$ret = $ret . "every $diff day ";
			} elsif ($cronpart[3] =~ /\*|\?/ ) {
				$ret = $ret . "daily ";
			}

		}
	} else {
		$ret = $cronstring;
	}

	return $ret;
}


sub extractErrorFromHash {
	my $hash = shift;
	my $ret = '';

	if (ref($hash) eq 'HASH') {
		if (defined($hash->{details})) {
			$ret = $hash->{details};
		} else {
			for my $h (keys %{$hash}) {
				$ret = $h . "->" . $ret . extractErrorFromHash($hash->{$h});
			}
		}
	} else {
		$ret = $hash;
	}

	return $ret;

}


sub  trim {
	my $s = shift;
	$s =~ s/^\s+|\s+$//g;
	return $s
};

sub convert_using_offset {
	my $timestamp = shift;
	my $src = shift;
	my $dst = shift;
	my $printtz = shift;
	my $makeiso = shift;
  my $printoff = shift;

	my $tz = new Date::Manip::TZ;
	my $dt = new Date::Manip::Date;
	my ($err,$date,$offset,$isdst,$abbrev);

	chomp($timestamp);
	$timestamp =~ s/T/ /;
	$timestamp =~ s/\.000Z//;

	$err = $dt->parse($timestamp);
	my $dttemp = $dt->value();

	my $delta;
	my $ts;
	my $off;

	if ($src eq 'UTC') {
		my ($h, $m) = ($dst =~ /GMT([+|-]\d\d)\:(\d\d)/);
		$off = $dst;
		my $hn = $h+0;
		my $mn = $m+0;
		$delta = $dt->new_delta();
		$delta->parse('0:0:0:0:' . $hn . ':' . $mn .':0');
	}

	if ($dst eq 'UTC') {
		my ($h, $m) = ($src =~ /GMT([+|-]\d\d)\:(\d\d)/);
		$off = 'UTC';
		my $hn = ($h+0)*-1;
		my $mn = ($m+0)*-1;
		$delta = $dt->new_delta();
		$delta->parse('0:0:0:0:' . $hn . ':' . $mn .':0');
	}

	my $nd = $dt->calc($delta);

	if (defined($makeiso)) {
		$ts = $nd->printf("%Y-%m-%dT%T.000Z");
	} else {
		$ts = $nd->printf("%Y-%m-%d %T");
		if (defined($printtz)) {
      if (defined($printoff)) {
        $off =~ s/GMT//;
        $ts = $ts . ' ' . $off;
      } else {
			  $ts = $ts . ' ' . $off;
      }
		}
	}

	return $ts;

}

sub convert_timezone {
	my $timestamp = shift;
	my $src = shift;
	my $dst = shift;
	my $printtz = shift;
	my $makeiso = shift;
  my $printoff = shift;

	my $tz = new Date::Manip::TZ;
	my $dt = new Date::Manip::Date;
	my ($err,$date,$offset,$isdst,$abbrev);

	$dt->config("setdate","zone," . $src);

	chomp($timestamp);
	$timestamp =~ s/T/ /;
	$timestamp =~ s/\.000Z//;


	$err = $dt->parse($timestamp);

	if ($err) {
		return undef;
	}

	my $dttemp = $dt->value();
	($err,$date,$offset,$isdst,$abbrev) = $tz->convert($dttemp, $src, $dst);
	my $ts;

	if ($err) {
		return undef;
	}

	if (defined($makeiso)) {
		$ts = sprintf("%04.4d-%02.2d-%02.2dT%02.2d:%02.2d:%02.2d.000Z",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);
	} else {
		if (defined($printtz)) {
      if (defined($printoff)) {
         $ts = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d %d:%2.2d",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5], $offset->[0], $offset->[1]);
      } else {
			   $ts = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d %s",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5], $abbrev);
		  }
    } else {
			$ts = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);
		}
	}




	return $ts;


}

sub convert_from_utc {
	my $timestamp = shift;
	my $timezone = shift;
	my $printtz = shift;
  my $printoff = shift;
  if ( $timezone =~ /GMT([+|-]\d\d)\:(\d\d)/ ) {
		return convert_using_offset($timestamp, 'UTC', $timezone, $printtz, undef, $printoff);
	} else {
    return convert_timezone($timestamp, 'UTC', $timezone, $printtz, undef, $printoff);
	}
}

sub convert_to_utc {
	my $timestamp = shift;
	my $timezone = shift;
	my $printtz = shift;
	my $doiso = shift;
	if ( $timezone =~ /GMT([+|-]\d\d)\:(\d\d)/ ) {
		return convert_using_offset($timestamp, $timezone, 'UTC', $printtz, $doiso);
	} else {
    return convert_timezone($timestamp, $timezone, 'UTC', $printtz, $doiso);
	}
}




# end of package
1;
