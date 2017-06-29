use server;

my $server = server->new(8080);
$server->host('127.0.0.1');
$server->run();
