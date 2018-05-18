use lib '/Users/mprzepiorowski/Documents/oss_dxtoolkit/dxtoolkit/test/';
use server;
use Log::Log4perl qw(:easy);

Log::Log4perl->easy_init($ERROR);

my $server = server->new(8080);
$server->host('127.0.0.1');
$server->run();
