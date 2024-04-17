

package PluginVDB_obj;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
our @ISA = qw(VDB_obj);

sub new {
    my $class  = shift;
    my $dlpxObject = shift;
    my $debug = shift;
    logger($debug, "Entering PluginVDB_obj::constructor",1);
    # call VDB_obj constructor
    my $self       = $class->SUPER::new($dlpxObject, $debug);

    $self->{_dbtype} = 'plugin';
    $self->{_pluginbased} = 1;

    # my @configureClone;
    # my @postRefresh;
    # my @preRefresh;
    # my @configParams;
    # my @mntPoints;
    # my %configParams = ();

    my %operations = (
        "type" => "VirtualSourceOperations",
        "configureClone" => \@configureClone,
        "postRefresh" => \@postRefresh,
        "preRefresh" => \@preRefresh
    );
    my %prov = (
        "type" => "AppDataProvisionParameters",
        "container" => {
            "type" => 'AppDataContainer',
            "name" => '',
            "group" => ''
        },
        "sourceConfig" => {
                "type" => "AppDataDirectSourceConfig",
                "repository" => "",
                "name" => ""
        },
        "source" => {
                "type" => "AppDataVirtualSource",
                "operations" => \%operations
        },
        "timeflowPointParameters" => {
            "type" => "TimeflowPointSemantic",
            "container" => "",
            "location" => "LATEST_SNAPSHOT"
        },
    );
    $self->{"NEWDB"} = \%prov;


    return $self;
}


sub addSource 
{
    my $self = shift;
    $sourcename = shift;
    $dbuser = shift;
    $password = shift;
    $dsourcename = shift;
    $group = shift;
    $logsync = shift;
    $stageenv = shift;
    $stageinst = shift;
    $stage_os_user = shift;
    $backup_dir  = shift;
    $sourcehostname = shift;
    $sourceport = shift;
    $ingestiontype = shift;
    $dumpdir = shift;
    $restorejobs = shift;
    $dumpjobs = shift;
    $staging_port = shift;
    $singledbname = shift;
    $mountbase = shift;
    $plugin_parameters = shift;


    logger($self->{_debug}, "Entering PluginVDB_obj::addSource",1);

    my $config = $self->setConfig($sourcename, $stageinst, $stageenv);

    if (! defined($config)) {
        print "Source database $sourcename not found\n";
        return undef;
    }

    if ( $self->setGroup($group) ) {
        print "Group $group not found. dSource won't be created\n";
        return undef;
    }

    if ( $self->setEnvironment($stageenv) ) {
        print "Staging environment $stageenv not found. dSource won't be created\n";
        return undef;
    }


    my $stage_osuser_ref = $self->{_environment}->getEnvironmentUserByName($stageenv,$stage_os_user);

    if (!defined($stage_osuser_ref)) {
        print "Source OS user $stage_os_user not found\n";
        return undef;
    }



    my %dsource_params = (
      "type" => "LinkParameters",
      "group" => $self->{"NEWDB"}->{"container"}->{"group"},
      "name" => $dsourcename,
      "linkData" => {
          "type" => "AppDataStagedLinkData",
          "config" => $config->{reference},
          "environmentUser" => $stage_osuser_ref,
          "stagingEnvironment" => $self->{'_newenv'},
          "stagingEnvironmentUser" => $stage_osuser_ref,
          "parameters" => $plugin_parameters,
          "syncParameters" => {
              "type"=> "AppDataSyncParameters",
              "parameters" => {
                "resync" => JSON::true
              }
          }
      }
    );


    my $ds_hooks = $self->set_dsource_hooks();
    if (defined($ds_hooks)) {
      if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.8.0)) {
        $dsource_params{"source"}{"operations"} = $ds_hooks;
      } else {
        $dsource_params{"linkData"}{"operations"} = $ds_hooks;
      }
    }

    my $operation = 'resources/json/delphix/database/link';
    my $json_data = to_json(\%dsource_params, {pretty=>1});
    #my $json_data = encode_json(\%dsource_params, pretty=>1);

    logger($self->{_debug}, $json_data, 1);

    return $self->runJobOperation($operation,$json_data, 'ACTION');


}


# Procedure snapshot
# parameters:
# - resync - yes/no
# Run snapshot
# Return job number if job started or undef otherwise

sub snapshot
{
    my $self = shift;
    my $resync = shift;
    logger($self->{_debug}, "Entering PluginVDB_obj::snapshot",1);

    my %snapshot_type = (
            "type" => "AppDataSyncParameters"
    );

    my $resync_value;

    if (defined ($resync)) {
      $resync_value = JSON::true
    } else {
      $resync_value = JSON::false
    }

    if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.11.6)) {
      # until 6.0.6
      %snapshot_type = (
              "type" => "AppDataSyncParameters",
              "resync" => $resync_value
      );

    } else {
      # 6.0.6 and higher
      %snapshot_type = (
              "type" => "AppDataSyncParameters",
              "parameters" => {
                "resync" => $resync_value
              }
      );

    }


    return $self->VDB_obj::snapshot(\%snapshot_type) ;
}


sub createVDB {
    my $self = shift;
    my $group = shift;
    my $env = shift;
    my $inst = shift;
    my $mountpoint = shift;
    my $port = shift;
    my $parameters = shift;

    logger($self->{_debug}, "Entering PluginVDB_obj::createVDB",1);


    if ( $self->setGroup($group) ) {
        print "Group $group not found. VDB won't be created\n";
        return undef;
    }

    if ( $self->setEnvironment($env) ) {
        print "Environment $env not found. VDB won't be created\n";
        return undef;
    }

    if ( $self->setHome($inst) ) {
        print "Instance $inst in environment $env not found. VDB won't be created\n";
        return undef;
    }

    if (!defined($mountpoint)) {
        print "Mount point not defined. VDB won't be created\n";
        return undef;    
    }

    if (!defined($port)) {
        print "Port not defined. VDB won't be created\n";
        return undef;    
    }

    $self->{"NEWDB"}->{"source"}->{"parameters"}=$parameters;

    my $operation = 'resources/json/delphix/database/provision';
    my $json_data = $self->getJSON();

    return $self->runJobOperation($operation,$json_data);

}