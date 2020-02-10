package server;
use JSON;
use Data::Dumper;
use Test::More;
use HTTP::Server::Simple::CGI;
use Log::Log4perl qw(:easy);


use base qw(HTTP::Server::Simple::CGI);



sub print_banner {    
   my $self = shift;
} 

sub handle_request {    
  
  my $self = shift;
  my $cgi  = shift;
  my $path = $cgi->path_info();
  if ($path eq '/stop') {        
    exit;
  } 
  
  my $log = Log::Log4perl->get_logger();

  
  $path =~ s|/resources/json/delphix/||;
  $path =~ s|/resources/json/service/||;
  my @path_components = split('/', $path);
  my $endfile;
  
  $log->error($path);
  if ($path eq 'login') {
    if (defined($self->{_postreq})) {
      undef $self->{_postreq};
    }
  }
  
  if ($cgi->param() && ($cgi->request_method eq 'GET')) {        
    my @params = $cgi->param();
    $endfile = "_";
    
    for my $parm (@params) {            
      if ($endfile ne "_") {                
        $endfile = $endfile . "_";
      }            
      $endfile = $endfile . $parm . "=" . $cgi->param($parm);
    }        
    $endfile = $endfile . ".json";
  } else {        
    $endfile = ".json";
  }        
  
  my $handler = $path_components[-1];
  my $filename = $path . $endfile;
  $filename =~ s|\?|_|;
  $filename =~ s|\&|_|g;
  $filename =~ s|\:|_|g;
        
  if ($filename && $cgi->request_method eq 'GET') { 	
    $self->readfile($cgi, $filename);
  } elsif ($filename && $cgi->request_method eq 'POST') {       
    $self->checkpost($cgi, $filename);
  } else {        
    print $path_components[0] . ' aaa ' . $path . " bbbb " . $filename;
  }
}  

sub readfile {    
  my $self = shift;
  my $cgi  = shift;
  my $jsonname = shift;
  open(my $fd, $jsonname) or die("Can't open file - $jsonname");
  my @content = <$fd>;
  close($fd);
  for my $line (@content) {      
    print $line;
  }
}

sub checkpost {    
  my $self = shift;
  my $cgi  = shift;
  my $jsonname = shift;
  
  
  if ( defined($self->{_postreq}) && (defined($self->{_postreq}->{$jsonname}))) {
    $self->{_postreq}->{$jsonname} = $self->{_postreq}->{$jsonname} + 1;
  } else {
    if (!defined($self->{_postreq})) {
      my %postreq;
      $self->{_postreq} = \%postreq;  
    }
    $self->{_postreq}->{$jsonname} = 1;
  }
  
  
  my $log = Log::Log4perl->get_logger();
  $log->error("Plik " . $jsonname);
  $log->error("Numerek " . $self->{_postreq}->{$jsonname});
  
  open(my $fd, $jsonname . "." . $self->{_postreq}->{$jsonname}) or die("POST Can't open file - $jsonname");
  my @content = <$fd>;
  close($fd);
  
  my @req;
  if ( -f $jsonname . ".req." . $self->{_postreq}->{$jsonname} ) {        
    open(my $fd, $jsonname . ".req." . $self->{_postreq}->{$jsonname} ) or die("Can't open file - $jsonname");
    @req = <$fd>;
    close($fd);
    my $json = JSON->new();
    $data = $json->decode( "" . $cgi->param('POSTDATA'));
    my $jsonreq = JSON->new();
    $datareq = $json->decode(join('',@req));
    my $a = is_deeply($datareq, $data);
    if ($a) {              
      for my $line (@content) {              
        print $line;
      }        
    } else {            
      print '{ "type" : "OKResult","status" : "FAILED"}' . "\n";
    }    
  } else {        
    for my $line (@content) {          
      print $line;
    }           
  }
}

1;

