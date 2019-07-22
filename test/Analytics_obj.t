#!/usr/bin/perl
# Program Name : Analytic_obj.t
# Description  : Analytics objects tests
# Author       : Marcin Przepiorowski
# Created: 10 Feb 2015 (v1.0.0)
#
# Updated: dd Mon YYYY (v1.0.x)
#   <comment>
#   By: <Author>
#
# Copyright (c) 2014 by Delphix. All rights reserved.
#


use Test::More tests => 36;
use Test::Output;
use JSON;
use Data::Dumper;

use lib '../lib';
use Analytic_obj;
use Analytic_io_obj;
use Analytic_cpu_obj;
use Analytic_tcp_obj;
use Analytic_network_obj;

my @collAxes = ( 'throughput' , 'latency' , 'count' );

my @args = ( undef, 'general', 'ref', 'type', \@collAxes, 1 );
my $obj = new_ok( 'Analytic_obj' =>  \@args , 'general');

my @arr = ( 128, 109, 74, 80, 80, 85, 92, 94, 97, 98, 98, 100, 101, 101, 104, 104, 106, 112, 115,137 );

ok($obj->calc_percentile(\@arr,0.25) eq 94, "Percentile 0.25");
ok($obj->calc_percentile(\@arr,0.50) eq 101, "Percentile 0.50");
ok($obj->calc_percentile(\@arr,0.75) eq 106, "Percentile 0.75");
ok($obj->calc_percentile(\@arr,1)    eq 137, "Percentile 1.00");
ok($obj->calc_avg(\@arr) eq 100.75, "Mean");

stdout_is ( sub { $obj->printDetails_banner(); }, "Name             StatisticType          CollectionInterval  CollectionAxes\n" , "general banner");

stdout_is ( sub { $obj->printDetails(); }, "general                                 1                   throughput,latency,count                                    \n" , "general details");

my %latency = ( '100000' => 10 , '5000000' => 10, '9000000' => 40 ); # 7275000 7.27


ok($obj->calculate_latency(\%latency) eq "7275000.00", "Latency");

my %latency = ( 
'1000'=>20,
'2000'=>1,
'4000'=>2,
'6000'=>3,
'8000'=>4,
'10000'=>5,
'20000'=>6,
'30000'=>7,
'50000'=>8,
'70000'=>9,
'100000'=>10,
'200000'=>1,
'600000'=>2,
'900000'=>3,
'1000000'=>4,
'2000000'=>5,
'100000000'=>6,
'200000000'=>7
); # 25981359.22

ok($obj->calculate_latency(\%latency) eq "25981359.22", "Latency");


my %hist;
my %new = ( '100000' => 10 , '5000000' => 10 );


$obj->add_histogram(\%hist, \%new);
is_deeply(\%hist, \%new, 'add empty hash');

my %new = ( '100000' => 30 , '3000000' => 10 );
my %result = (  '100000' => 40 , '5000000' => 10, '3000000' => 10);

$obj->add_histogram(\%hist, \%new);
is_deeply(\%hist, \%result, 'add non-empty hash');


# nfs by client 


my $FD = \*STDOUT;

my @args = ( undef, 'nfs-by-client', 'ref', 'type', 'throughput,latency,count'  );
my $obj = new_ok( 'Analytic_io_obj' =>  \@args , 'nfs-by-client');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83

my %row = ( 'throughput' => 10*1024*1024, 'count' => 20, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:00:00" , \%row, '10.10.10.10', 'read');
my %row = ( 'throughput' => 20*1024*1024, 'count' => 10, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:00:00" , \%row, '10.10.10.10', 'write');


$obj->processData(10);


my $output_line = <<'END_LINE';
#timestamp,client,read_throughput,write_throughput,total_throughput,read_latency,write_latency,ops_read,ops_write,total_ops
2015-03-06 01:00:00,10.10.10.10,10.00,20.00,30.00,2.83,2.83,20,10,30
END_LINE

stdout_is ( sub { $obj->print($FD); } , $output_line , "single line - nfs-by-client");




my %latency = ( '100000' => 10 , '5000000' => 10, '9000000' => 40 ); # 7275000 7.28
my %row = ( 'throughput' => 50*1024*1024, 'count' => 200, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 02:00:00" , \%row, '10.10.10.10', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10, '9000000' => 40 ); # 7275000 7.28
my %row = ( 'throughput' => 60*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 02:00:00" , \%row, '10.10.10.10', 'write');
$obj->processData(10);

my $output_line = <<'END_LINE';
#timestamp,client,read_throughput,write_throughput,total_throughput,read_latency,write_latency,ops_read,ops_write,total_ops
2015-03-06 01:00:00,10.10.10.10,10.00,20.00,30.00,2.83,2.83,20,10,30
2015-03-06 02:00:00,10.10.10.10,50.00,60.00,110.00,7.28,7.28,200,100,300
END_LINE

stdout_is ( sub { $obj->print($FD); } , $output_line, "multi line - nfs-by-client");



my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 60*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 03:00:00" , \%row, '10.10.10.10', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 70*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 04:00:00" , \%row, '10.10.10.10', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 80*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 05:00:00" , \%row, '10.10.10.10', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 90*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 06:00:00" , \%row, '10.10.10.10', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 100*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 07:00:00" , \%row, '10.10.10.10', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 110*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 08:00:00" , \%row, '10.10.10.10', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 120*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 09:00:00" , \%row, '10.10.10.10', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 130*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 10:00:00" , \%row, '10.10.10.10', 'read');
$obj->processData(10);

my $output_line = <<'END_LINE';
#timestamp,client,read_throughput,write_throughput,total_throughput,read_latency,write_latency,ops_read,ops_write,total_ops
2015-03-06 01:00:00,10.10.10.10,10.00,20.00,30.00,2.83,2.83,20,10,30
2015-03-06 02:00:00,10.10.10.10,50.00,60.00,110.00,7.28,7.28,200,100,300
2015-03-06 03:00:00,10.10.10.10,60.00,0.00,60.00,2.83,N/A,100,0,100
2015-03-06 04:00:00,10.10.10.10,70.00,0.00,70.00,2.83,N/A,100,0,100
2015-03-06 05:00:00,10.10.10.10,80.00,0.00,80.00,2.83,N/A,100,0,100
2015-03-06 06:00:00,10.10.10.10,90.00,0.00,90.00,2.83,N/A,100,0,100
2015-03-06 07:00:00,10.10.10.10,100.00,0.00,100.00,2.83,N/A,100,0,100
2015-03-06 08:00:00,10.10.10.10,110.00,0.00,110.00,2.83,N/A,100,0,100
2015-03-06 09:00:00,10.10.10.10,120.00,0.00,120.00,2.83,N/A,100,0,100
2015-03-06 10:00:00,10.10.10.10,130.00,0.00,130.00,2.83,N/A,100,0,100
END_LINE

stdout_is ( sub { $obj->print($FD); } , $output_line, "multi line 10 - nfs-by-client");

$obj->doAggregation();

my $output_line = <<'END_LINE';
#time,client,throughput_r_min,throughput_r_max,throughput_r_85pct,throughput_w_min,throughput_w_max,throughput_w_85pct,throughput_t_min,throughput_t_max,throughput_t_85pct,latency_r_min,latency_r_max,latency_r_85pct,latency_w_min,latency_w_max,latency_w_85pct,iops_r_min,iops_r_max,iops_r_85pct,iops_w_min,iops_w_max,iops_w_85pct,iops_min,iops_max,iops_85pct
2015-03-06,10.10.10.10,10.00,130.00,120.00,0.00,60.00,20.00,30.00,130.00,120.00,2.83,7.28,2.83,2.83,7.28,7.28,20.00,200.00,100.00,0.00,100.00,10.00,30.00,300.00,100.00
END_LINE

stdout_is ( sub { $obj->print_aggregation($FD); } , $output_line, "aggregation - nfs-by-client");


# cpu tests

my @args = ( undef, 'default.cpu', 'ref', 'type', 'throughput,latency,count', 1 );
my $obj = new_ok( 'Analytic_cpu_obj' =>  \@args , 'CPU');

my %row = ( 'idle' => 80, 'kernel' => 10,'user' => 10 ); 
$obj->add_row( "2015-03-06 10:00:00" , \%row, 'none');

$obj->processData(10);

my $output_line = <<'END_LINE';
#timestamp,util
2015-03-06 10:00:00,20.00
END_LINE

stdout_is ( sub { $obj->print($FD); } , $output_line , "single line - cpu");

my %row = ( 'idle' => 90, 'kernel' => 0.00001,'user' => 10 ); 
$obj->add_row( "2015-03-06 11:00:00" , \%row, 'none');
my %row = ( 'idle' => 70, 'kernel' => 10,'user' => 20 ); 
$obj->add_row( "2015-03-06 12:00:00" , \%row, 'none');
my %row = ( 'idle' => 60, 'kernel' => 25,'user' => 15 ); 
$obj->add_row( "2015-03-06 13:00:00" , \%row, 'none');
my %row = ( 'idle' => 50, 'kernel' => 10,'user' => 40 ); 
$obj->add_row( "2015-03-06 14:00:00" , \%row, 'none');
my %row = ( 'idle' => 40, 'kernel' => 10,'user' => 50 ); 
$obj->add_row( "2015-03-06 15:00:00" , \%row, 'none');
my %row = ( 'idle' => 30, 'kernel' => 20,'user' => 50 ); 
$obj->add_row( "2015-03-06 16:00:00" , \%row, 'none');
my %row = ( 'idle' => 20, 'kernel' => 30,'user' => 50 ); 
$obj->add_row( "2015-03-06 17:00:00" , \%row, 'none');
my %row = ( 'idle' => 10, 'kernel' => 45,'user' => 45 ); 
$obj->add_row( "2015-03-06 18:00:00" , \%row, 'none');
my %row = ( 'idle' => 0, 'kernel' => 100,'user' => 0 ); 
$obj->add_row( "2015-03-06 19:00:00" , \%row, 'none');

$obj->processData(10);

my $output_line = <<'END_LINE';
#timestamp,util
2015-03-06 10:00:00,20.00
2015-03-06 11:00:00,10.00
2015-03-06 12:00:00,30.00
2015-03-06 13:00:00,40.00
2015-03-06 14:00:00,50.00
2015-03-06 15:00:00,60.00
2015-03-06 16:00:00,70.00
2015-03-06 17:00:00,80.00
2015-03-06 18:00:00,90.00
2015-03-06 19:00:00,100.00
END_LINE

stdout_is ( sub { $obj->print($FD); }  , $output_line , "multi line - cpu");

$obj->doAggregation();

my $output_line = <<'END_LINE';
#time,utilization_min,utilization_max,utilization_85pct
2015-03-06,10.00,100.00,90.00
END_LINE

stdout_is ( sub { $obj->print_aggregation($FD); }, $output_line, "aggregation - cpu");

# nfs tests

my @args = ( undef, 'default.nfs', 'ref', 'type', 'throughput,latency,count', 1 );
my $obj = new_ok( 'Analytic_io_obj' =>  \@args , 'default.nfs');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83

my %row = ( 'throughput' => 10*1024*1024, 'count' => 20, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:00:00" , \%row, 'none', 'read');
my %row = ( 'throughput' => 20*1024*1024, 'count' => 10, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:00:00" , \%row, 'none', 'write');


$obj->processData(10);


my $output_line = <<'END_LINE';
#timestamp,read_throughput,write_throughput,total_throughput,read_latency,write_latency,ops_read,ops_write,total_ops
2015-03-06 01:00:00,10.00,20.00,30.00,2.83,2.83,20,10,30
END_LINE

stdout_is ( sub { $obj->print($FD); } , $output_line , "single line - default.nfs");


my %latency = ( '100000' => 10 , '5000000' => 10, '9000000' => 40 ); # 6850000 7.28
my %row = ( 'throughput' => 50*1024*1024, 'count' => 200, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 02:00:00" , \%row, 'none', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 60*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 02:00:00" , \%row, 'none', 'write');

$obj->processData(10);

my $output_line = <<'END_LINE';
#timestamp,read_throughput,write_throughput,total_throughput,read_latency,write_latency,ops_read,ops_write,total_ops
2015-03-06 01:00:00,10.00,20.00,30.00,2.83,2.83,20,10,30
2015-03-06 02:00:00,50.00,60.00,110.00,7.28,2.83,200,100,300
END_LINE

stdout_is ( sub { $obj->print($FD); } , $output_line, "multi line - default.nfs");


my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 60*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 03:00:00" , \%row, 'none', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 70*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 04:00:00" , \%row, 'none', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 80*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 05:00:00" , \%row, 'none', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 90*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 06:00:00" , \%row, 'none', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 99*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 07:00:00" , \%row, 'none', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 110*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 08:00:00" , \%row, 'none', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 120*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 09:00:00" , \%row, 'none', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 140*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 10:00:00" , \%row, 'none', 'read');

$obj->processData(10);

my $output_line = <<'END_LINE';
#timestamp,read_throughput,write_throughput,total_throughput,read_latency,write_latency,ops_read,ops_write,total_ops
2015-03-06 01:00:00,10.00,20.00,30.00,2.83,2.83,20,10,30
2015-03-06 02:00:00,50.00,60.00,110.00,7.28,2.83,200,100,300
2015-03-06 03:00:00,60.00,0.00,60.00,2.83,N/A,100,0,100
2015-03-06 04:00:00,70.00,0.00,70.00,2.83,N/A,100,0,100
2015-03-06 05:00:00,80.00,0.00,80.00,2.83,N/A,100,0,100
2015-03-06 06:00:00,90.00,0.00,90.00,2.83,N/A,100,0,100
2015-03-06 07:00:00,99.00,0.00,99.00,2.83,N/A,100,0,100
2015-03-06 08:00:00,110.00,0.00,110.00,2.83,N/A,100,0,100
2015-03-06 09:00:00,120.00,0.00,120.00,2.83,N/A,100,0,100
2015-03-06 10:00:00,140.00,0.00,140.00,2.83,N/A,100,0,100
END_LINE

stdout_is ( sub { $obj->print($FD); } , $output_line, "multi line 10 - default.nfs");

$obj->doAggregation();

my $output_line = <<'END_LINE';
#time,throughput_r_min,throughput_r_max,throughput_r_85pct,throughput_w_min,throughput_w_max,throughput_w_85pct,throughput_t_min,throughput_t_max,throughput_t_85pct,latency_r_min,latency_r_max,latency_r_85pct,latency_w_min,latency_w_max,latency_w_85pct,iops_r_min,iops_r_max,iops_r_85pct,iops_w_min,iops_w_max,iops_w_85pct,iops_min,iops_max,iops_85pct
2015-03-06,10.00,140.00,120.00,0.00,60.00,20.00,30.00,140.00,120.00,2.83,7.28,2.83,2.83,2.83,2.83,20.00,200.00,100.00,0.00,100.00,10.00,30.00,300.00,100.00
END_LINE

stdout_is ( sub { $obj->print_aggregation($FD); } , $output_line, "aggregation - default.nfs");


# network tests

my @args = ( undef, 'default.network', 'ref', 'type', 'inBytes,outBytes', 1 );
my $obj = new_ok( 'Analytic_network_obj' =>  \@args , 'Network');



my %row = ( 'inBytes' => 12345, 'outBytes' => 6789, 'inPackets' => 12345, 'outPackets' => 6789 ); 
$obj->add_row( "2015-03-06 10:00:00" , \%row, 'none', undef, undef, 'vmxnet3s0');

$obj->processData(10);

my $output_line = <<'END_LINE';
#timestamp,inBytes,outBytes,inPackets,outPackets,vmxnet3s0_inBytes,vmxnet3s0_outBytes,vmxnet3s0_inPackets,vmxnet3s0_outPackets
2015-03-06 10:00:00,12345,6789,12345,6789,12345,6789,12345,6789
END_LINE

stdout_is ( sub { $obj->print($FD); }, $output_line , "single line - network");

my %row = ( 'inBytes' => 1000, 'outBytes' => 2000 ); 
$obj->add_row( "2015-03-06 11:00:00" , \%row, 'none', undef, undef, 'vmxnet3s0');
my %row = ( 'inBytes' => 1000, 'outBytes' => 2000 ); 
$obj->add_row( "2015-03-06 12:00:00" , \%row, 'none', undef, undef, 'vmxnet3s0');
my %row = ( 'inBytes' => 1000, 'outBytes' => 2000 ); 
$obj->add_row( "2015-03-06 13:00:00" , \%row, 'none', undef, undef, 'vmxnet3s0');
my %row = ( 'inBytes' => 1000, 'outBytes' => 6789 ); 
$obj->add_row( "2015-03-06 14:00:00" , \%row, 'none', undef, undef, 'vmxnet3s0');
my %row = ( 'inBytes' => 1000, 'outBytes' => 2000 ); 
$obj->add_row( "2015-03-06 15:00:00" , \%row, 'none', undef, undef, 'vmxnet3s0');
my %row = ( 'inBytes' => 1000, 'outBytes' => 6789 ); 
$obj->add_row( "2015-03-06 16:00:00" , \%row, 'none', undef, undef, 'vmxnet3s0');
my %row = ( 'inBytes' => 12345, 'outBytes' => 6789 ); 
$obj->add_row( "2015-03-06 17:00:00" , \%row, 'none', undef, undef, 'vmxnet3s0');
my %row = ( 'inBytes' => 12345, 'outBytes' => 9999999999 ); 
$obj->add_row( "2015-03-06 18:00:00" , \%row, 'none', undef, undef, 'vmxnet3s0');
my %row = ( 'inBytes' => 12345, 'outBytes' => 0 ); 
$obj->add_row( "2015-03-06 19:00:00" , \%row, 'none', undef, undef, 'vmxnet3s0');

$obj->processData(10);

my $output_line = <<'END_LINE';
#timestamp,inBytes,outBytes,inPackets,outPackets,vmxnet3s0_inBytes,vmxnet3s0_outBytes,vmxnet3s0_inPackets,vmxnet3s0_outPackets
2015-03-06 10:00:00,12345,6789,12345,6789,12345,6789,12345,6789
2015-03-06 11:00:00,1000,2000,0,0,1000,2000,0,0
2015-03-06 12:00:00,1000,2000,0,0,1000,2000,0,0
2015-03-06 13:00:00,1000,2000,0,0,1000,2000,0,0
2015-03-06 14:00:00,1000,6789,0,0,1000,6789,0,0
2015-03-06 15:00:00,1000,2000,0,0,1000,2000,0,0
2015-03-06 16:00:00,1000,6789,0,0,1000,6789,0,0
2015-03-06 17:00:00,12345,6789,0,0,12345,6789,0,0
2015-03-06 18:00:00,12345,9999999999,0,0,12345,9999999999,0,0
2015-03-06 19:00:00,12345,0,0,0,12345,0,0,0
END_LINE

stdout_is ( sub { $obj->print($FD); }, $output_line , "multi line - network");

$obj->doAggregation();

my $output_line = <<'END_LINE';
#time,inBytes_min,inBytes_max,inBytes_85pct,outBytes_min,outBytes_max,outBytes_85pct
2015-03-06,1000.00,12345.00,12345.00,0.00,9999999999.00,6789.00
END_LINE

stdout_is ( sub { $obj->print_aggregation($FD); } , $output_line, "aggregation - network");


# nfs all 

my @axis = ( 'latency','throughput','count','op','client','cached' );
my @args = ( undef, 'nfs-all', 'nbc', 'type', \@axis , 1 );
my $obj = new_ok( 'Analytic_io_obj' =>  \@args , 'nfs all - one line');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83

my %row = ( 'throughput' => 10*1024*1024, 'count' => 20, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:00:00" , \%row, '10.10.10.10', 'read', 0);
my %row = ( 'throughput' => 20*1024*1024, 'count' => 10, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:00:00" , \%row, '10.10.10.10', 'write', 0);

my %row = ( 'throughput' => 20*1024*1024, 'count' => 40, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:00:00" , \%row, '10.10.10.10', 'read', 1);
my %row = ( 'throughput' => 10*1024*1024, 'count' => 20, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:00:00" , \%row, '10.10.10.10', 'write', 1);

$obj->processData(10);

my $output_line = <<'END_LINE';
#timestamp,client,read_throughput,write_throughput,total_throughput,read_latency,write_latency,read_cache_hit_ratio,ops_read,ops_write,total_ops
2015-03-06 01:00:00,10.10.10.10,30.00,30.00,60.00,2.83,2.83,66.67,60,30,90
END_LINE

stdout_is ( sub { $obj->print($FD); } , $output_line , "single line - nfs-all");

$obj->doAggregation();

my $output_line = <<'END_LINE';
#time,client,throughput_r_min,throughput_r_max,throughput_r_85pct,throughput_w_min,throughput_w_max,throughput_w_85pct,throughput_t_min,throughput_t_max,throughput_t_85pct,latency_r_min,latency_r_max,latency_r_85pct,latency_w_min,latency_w_max,latency_w_85pct,cache_hit_ratio_min,cache_hit_ratio_max,cache_hit_ratio_85pct
2015-03-06,10.10.10.10,30.00,30.00,30.00,30.00,30.00,30.00,60.00,60.00,60.00,2.83,2.83,2.83,2.83,2.83,2.83,66.67,66.67,66.67
END_LINE

stdout_is ( sub { $obj->print_aggregation($FD); } , $output_line, "aggregation - nfs-all");

# disk  

my @axis = ( 'latency','throughput','count','op' );
my @args = ( undef, 'default.disk', 'nbc', 'type', \@axis , 1 , 1 );
my $obj = new_ok( 'Analytic_io_obj' =>  \@args , 'disk - one line');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83

my %row = ( 'throughput' => 10*1024*1024, 'count' => 20, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:00:00" , \%row, 'none', 'read');
my %row = ( 'throughput' => 20*1024*1024, 'count' => 10, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:00:00" , \%row, 'none', 'write');

my %row = ( 'throughput' => 20*1024*1024, 'count' => 40, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:01:00" , \%row, 'none', 'read');
my %row = ( 'throughput' => 10*1024*1024, 'count' => 20, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:01:00" , \%row, 'none', 'write');


$obj->processData(10);

my $output_line = <<'END_LINE';
#timestamp,read_throughput,write_throughput,total_throughput,ops_read,ops_write,total_ops,read_latency,write_latency
2015-03-06 01:00:00,10.00,20.00,30.00,20,10,30,2.83,2.83
2015-03-06 01:01:00,20.00,10.00,30.00,40,20,60,2.83,2.83
END_LINE


stdout_is ( sub { $obj->print($FD); } , $output_line , "single line - disk");


# iscsi  

my @axis = ( 'latency','throughput','count','op' );
my @args = ( undef, 'default.iscsi', 'nbc', 'type', \@axis , 1 );
my $obj = new_ok( 'Analytic_io_obj' =>  \@args , 'iscsi - one line');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83

my %row = ( 'throughput' => 10*1024*1024, 'count' => 20, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:00:00" , \%row, 'none', 'read');
my %row = ( 'throughput' => 20*1024*1024, 'count' => 10, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:00:00" , \%row, 'none', 'write');

my %row = ( 'throughput' => 20*1024*1024, 'count' => 40, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:01:00" , \%row, 'none', 'read');
my %row = ( 'throughput' => 10*1024*1024, 'count' => 20, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:01:00" , \%row, 'none', 'write');

$obj->processData(10);

my $output_line = <<'END_LINE';
#timestamp,read_throughput,write_throughput,total_throughput,ops_read,ops_write,total_ops,read_latency,write_latency
2015-03-06 01:00:00,10.00,20.00,30.00,20,10,30,2.83,2.83
2015-03-06 01:01:00,20.00,10.00,30.00,40,20,60,2.83,2.83
END_LINE


stdout_is ( sub { $obj->print($FD); }  , $output_line , "single line - iscsi");


# 5 min CPU

my @args = ( undef, 'default.cpu', 'ref', 'type', 'throughput,latency,count', 1 );
my $obj = new_ok( 'Analytic_cpu_obj' =>  \@args , 'CPU');

my %row = ( 'idle' => 80, 'kernel' => 10,'user' => 10 ); 
$obj->add_row( "2015-03-06 23:56:00" , \%row, 'none');

$obj->processData(2);

my $output_line = <<'END_LINE';
#timestamp,util
2015-03-06 23:56:00,20.00
END_LINE

stdout_is ( sub { $obj->print($FD); } , $output_line , "5 min - cpu");

my %row = ( 'idle' => 90, 'kernel' => 0,'user' => 10 ); 
$obj->add_row( "2015-03-06 23:57:00" , \%row, 'none');
my %row = ( 'idle' => 90, 'kernel' => 5,'user' => 5 ); 
$obj->add_row( "2015-03-06 23:58:00" , \%row, 'none');
my %row = ( 'idle' => 90, 'kernel' => 5,'user' => 5 ); 
$obj->add_row( "2015-03-06 23:59:00" , \%row, 'none');
my %row = ( 'idle' => 90, 'kernel' => 5,'user' => 5 ); 
$obj->add_row( "2015-03-07 00:00:00" , \%row, 'none');


$obj->processData(2);

my $output_line = <<'END_LINE';
#timestamp,util
2015-03-06 23:56:00,20.00
2015-03-06 23:57:00,10.00
2015-03-06 23:58:00,10.00
2015-03-06 23:59:00,10.00
2015-03-07 00:00:00,10.00
END_LINE

stdout_is ( sub { $obj->print($FD); }  , $output_line , "5 min - cpu");

$obj->doAggregation();

ok( $obj->get_avg('utilization') eq "12.00", "5 min - cpu");


# 5 min nfs tests

my @args = ( undef, 'default.nfs', 'ref', 'type', 'throughput,latency,count', 1 );
my $obj = new_ok( 'Analytic_io_obj' =>  \@args , 'default.nfs');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83

my %row = ( 'throughput' => 10*1024*1024, 'count' => 20, 'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:00:00" , \%row, 'none', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 60*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:01:00" , \%row, 'none', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 70*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:02:00" , \%row, 'none', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 80*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:03:00" , \%row, 'none', 'read');

my %latency = ( '100000' => 10 , '5000000' => 10 ); # 2825000 2.83
my %row = ( 'throughput' => 90*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:04:00" , \%row, 'none', 'read');

my %latency = ( '100000' => 10 , '5000000' => 1 ); # 2825000 2.83
my %row = ( 'throughput' => 90*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:03:00" , \%row, 'none', 'write');

my %latency = ( '100000' => 10 , '5000000' => 1 ); # 2825000 2.83
my %row = ( 'throughput' => 90*1024*1024, 'count' => 100,'latency' => \%latency ); 
$obj->add_row( "2015-03-06 01:04:00" , \%row, 'none', 'write');

$obj->processData(2);
$obj->doAggregation();
ok( $obj->get_avg('throughput_r') eq "62.00", "5 min - nfs - throughput_r");
ok( $obj->get_avg('latency_r') eq "2.83", "5 min - nfs - latency_r");
ok( $obj->get_avg('latency_w') eq "0.64", "5 min - nfs - latency_w");
ok( $obj->get_avg('latency_w') eq "0.64", "5 min - nfs - latency_t");

print $obj->get_avg('latency_r')  . "\n";
print $obj->get_avg('latency_w')  . "\n";
print $obj->get_avg('latency_t')  . "\n";

