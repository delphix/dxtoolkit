

package PostgresVDB_obj;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
our @ISA = qw(PluginVDB_obj);

sub new {
    my $class  = shift;
    my $dlpxObject = shift;
    my $debug = shift;
    logger($debug, "Entering PostgresVDB_obj::constructor",1);
    # call VDB_obj constructor
    my $self       = $class->SUPER::new($dlpxObject, $debug);

    $self->{_dbtype} = 'postgresql';
    $self->{_pluginbased} = 1;

    return $self;
}


# Procedure getStagingEnvironmentName
# parameters: none
# Return database staging environment

sub getStagingEnvironmentName
{
    my $self = shift;
    logger($self->{_debug}, "Entering PostgresVDB_obj::getStagingEnvironmentName",1);
    my $ret;
    if (defined($self->{environment}->{name})) {
      $ret = $self->{environment}->{name};
    } else {
      $ret = 'N/A';
    }
    return $ret;
}

# Procedure getStagingInst
# parameters: none
# Return database staging environment

sub getStagingInstName
{
    my $self = shift;
    logger($self->{_debug}, "Entering PostgresVDB_obj::getStagingInst",1);
    my $ret;

    my $repo = $self->{_repository}->getRepository($self->{"repository"});

    if (defined($repo)) {
      $ret = $repo->{name};
    } else {
      $ret = 'N/A';
    }
    return $ret;
}


# Procedure getMountPoint
# parameters:
# Get mountpoint of staging DB.

sub getMountPoint {
    my $self = shift;
    logger($self->{_debug}, "Entering PostgresVDB_obj::getMountPoint",1);
    return $self->{"source"}->{"parameters"}->{"mountLocation"};
}


# Procedure getCustomparams
# parameters:
# Get custom parameters

sub getCustomparams {
    my $self = shift;
    logger($self->{_debug}, "Entering PostgresVDB_obj::getCustomparams",1);
    my $ret = "";
    if (defined($self->{"source"}->{"parameters"}->{"configSettingsStg"})) {
        for my $param (@{$self->{"source"}->{"parameters"}->{"configSettingsStg"}}) {
            if ($param->{"commentProperty"}) {
                # remove parameter so output with #
                $ret = $ret . "-customparameters \"#" . $param->{"propertyName"} . "\" "; 
            } else {
                # add parameter
                $ret = $ret . "-customparameters \"" . $param->{"propertyName"} . "=" . $param->{"value"} . "\" "; 
            }
        }
    } 
    return $ret;
}



# Procedure getStagingUser
# parameters: none
# Return OS user

sub getStagingUser
{
    my $self = shift;
    logger($self->{_debug}, "Entering PostgresVDB_obj::getStagingUser",1);
    my $ret;
    my $user;

    my $staging_env = $self->{environment}->{reference};
    my $staging_user_ref;

    if (defined($staging_env)) {
      $staging_user_ref = $self->{source}->{stagingEnvironmentUser};
      $ret = $self->{_environment}->getEnvironmentUserNamebyRef($staging_env, $staging_user_ref);
    } else {
      $ret = 'N/A';
    }

    return $ret;
}


# Procedure getPostgresqlPort
# parameters: none
# Return staging port

sub getPostgresqlPort
{
    my $self = shift;
    logger($self->{_debug}, "Entering PostgresVDB_obj::getPostgresqlPort",1);
    my $ret;
    if (defined($self->{"source"}->{"parameters"}) && defined($self->{"source"}->{"parameters"}->{"postgresPort"})) {
        $ret = $self->{"source"}->{"parameters"}->{"postgresPort"};
    } else {
        $ret = 'N/A';
    }
    return $ret;
}


# Procedure getSingleParam
# parameters: parameter name
# Return parameter value for single ingestion

sub getSingleParam
{
    my $self = shift;
    my $param = shift;
    logger($self->{_debug}, "Entering PostgresVDB_obj::getSingleParam",1);
    my $ret;
    if (defined($self->{"source"}->{"parameters"}) && defined($self->{"source"}->{"parameters"}->{"singleDatabaseIngestionFlag"}) && $self->{"source"}->{"parameters"}->{"singleDatabaseIngestionFlag"}) {
        $ret = $self->{"source"}->{"parameters"}->{"singleDatabaseIngestion"}->[-1]->{$param};
    } else {
        $ret = 'N/A';
    }
    return $ret;
}


# Procedure getInitialParam
# parameters: parameter name
# Return parameter value for initialized ingestion

sub getInitialParam
{
    my $self = shift;
    my $param = shift;
    logger($self->{_debug}, "Entering PostgresVDB_obj::getInitialParam",1);
    my $ret;
    if (defined($self->{"source"}->{"parameters"}) && defined($self->{"source"}->{"parameters"}->{"delphixInitiatedBackup"}) && (scalar(@{$self->{"source"}->{"parameters"}->{"delphixInitiatedBackup"}}) > 0) ) {
        $ret = $self->{"source"}->{"parameters"}->{"delphixInitiatedBackup"}->[-1]->{$param};
    } else {
        $ret = 'N/A';
    }
    return $ret;
}

# Procedure getExternalParam
# parameters: parameter name
# Return parameter value for initialized ingestion

sub getExternalParam
{
    my $self = shift;
    my $param = shift;
    logger($self->{_debug}, "Entering PostgresVDB_obj::getExternalParam",1);
    my $ret;
    if (defined($self->{"source"}->{"parameters"}) && defined($self->{"source"}->{"parameters"}->{"externalBackup"}) && (scalar(@{$self->{"source"}->{"parameters"}->{"externalBackup"}}) > 0)) {
        $ret = $self->{"source"}->{"parameters"}->{"externalBackup"}->[-1]->{$param};
    } else {
        $ret = 'N/A';
    }
    return $ret;
}


# Procedure getIngestionType
# parameters: None
# return an ingestion type
# delphixInitiatedBackup, singleDatabaseIngestion, stagingPush, externalBackup

sub getIngestionType
{
    my $self = shift;
    logger($self->{_debug}, "Entering PostgresVDB_obj::getIngestionType",1);
    my $ret;

    if (defined($self->{"source"}->{"parameters"}) && defined($self->{"source"}->{"parameters"}->{"delphixInitiatedBackupFlag"}) && $self->{"source"}->{"parameters"}->{"delphixInitiatedBackupFlag"}) {
        $ret = "initiated";
    } 
    elsif (defined($self->{"source"}->{"parameters"}) && defined($self->{"source"}->{"parameters"}->{"singleDatabaseIngestionFlag"}) && $self->{"source"}->{"parameters"}->{"singleDatabaseIngestionFlag"}) {
        $ret = "single";
    }
    elsif (defined($self->{"source"}->{"parameters"}) && defined($self->{"source"}->{"parameters"}->{"stagingPushFlag"}) && $self->{"source"}->{"parameters"}->{"stagingPushFlag"}) {
        $ret = "stagingpush";
    }
    else {
        if (defined($self->{"source"}->{"parameters"}) && defined($self->{"source"}->{"parameters"}->{"externalBackup"}) && (scalar(@{$self->{"source"}->{"parameters"}->{"externalBackup"}})>0)) {
            $ret = "externalbackup";
        } else {
            $ret = 'N/A';
        }
    }

    return $ret;
}

# Procedure getConfig
# parameters: none
# Return database config

sub getConfig
{
    my $self = shift;
    my $templates = shift;
    my $backup = shift;

    logger($self->{_debug}, "Entering PostgresVDB_obj::getConfig",1);
    my $config = '';
    my $joinsep;

    if (defined($backup)) {
      $joinsep = ' ';
    } else {
      $joinsep = ',';
    }

    if ($self->getType() eq 'VDB') {
        my $vdb_port = $self->getPostgresqlPort();
        my $mountpoint = $self->getMountPoint();
         $config = join($joinsep,($config, "-mntpoint \"" . $mountpoint . "\""));
         $config = join($joinsep,($config, "-port \"" . $vdb_port . "\""));
    } else {
        # dSource
        my $staging_port = $self->getPostgresqlPort();
        my $ingestion_type = $self->getIngestionType();


        $config = join($joinsep,($config, "-mountbase \"" . $self->getMountPoint() . "\""));
        $config = join($joinsep,($config, "-stagingport " . $staging_port ));

        # postgeresql related
        $config = join($joinsep,($config, "-ingestiontype $ingestion_type "));

        if ($ingestion_type eq "single" ) {
            $config = join($joinsep,($config, "-dbuser \"" . $self->getSingleParam("databaseUserName") . "\""));
            $config = join($joinsep,($config, "-password xxxxxxxxx"));
            $config = join($joinsep,($config, "-sourcehostname \"" . $self->getSingleParam("sourceHost") . "\""));
            $config = join($joinsep,($config, "-sourceport " . $self->getSingleParam("sourcePort") ));
            $config = join($joinsep,($config, "-singledbname \"" . $self->getSingleParam("databaseName") . "\""));
            $config = join($joinsep,($config, "-dumpdir \"" . $self->getSingleParam("dumpDir") . "\""));
            $config = join($joinsep,($config, "-restorejobs " . $self->getSingleParam("restoreJobs") ));
            $config = join($joinsep,($config, "-dumpjobs " . $self->getSingleParam("dumpJobs") ));
        } 
        elsif ($ingestion_type eq "initiated" ) {
            $config = join($joinsep,($config, "-dbuser \"" . $self->getInitialParam("userName") . "\""));
            $config = join($joinsep,($config, "-password xxxxxxxxx"));
            $config = join($joinsep,($config, "-sourcehostname \"" . $self->getInitialParam("sourceHostAddress") . "\""));
            $config = join($joinsep,($config, "-sourceport " . $self->getInitialParam("postgresSourcePort") ));   
        }
        elsif ($ingestion_type eq "externalbackup" ) {
            $config = join($joinsep,($config, "-backup_dir \"" . $self->getExternalParam("backupPath") . "\""));
            $config = join($joinsep,($config, "-backup_dir_log \"" . $self->getExternalParam("walLogPath") . "\""));
            my $insync = $self->getExternalParam("keepStagingInSync");
            my $insync_text;
            if ($insync) {
                $insync_text = "yes";
                $config = join($joinsep,($config, "-dbuser \"" . $self->getInitialParam("userName") . "\""));
                $config = join($joinsep,($config, "-password xxxxxxxxx"));
                $config = join($joinsep,($config, "-sourcehostname \"" . $self->getInitialParam("sourceHostAddress") . "\""));
                $config = join($joinsep,($config, "-sourceport " . $self->getInitialParam("postgresSourcePort") ));   
            } else {
                $insync_text = "no";
            }
            $config = join($joinsep,($config, "-keepinsync " . $insync_text ));

        }
    }

    $config = join($joinsep, ($config, $self->getCustomparams()));

    if ( (my $rest) = $config =~ /^,(.*)/ ) {
      $config = $rest;
    }

    return $config;

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
    $customparameters = shift;
    $backup_path = shift;
    $backup_dir_log = shift;
    $keepinsync = shift;

    logger($self->{_debug}, "Entering PostgresVDB_obj::addSource",1);

    my @empty;

    my %parameters = (
        'configSettingsStg' => \@empty,
        'externalBackup' => \@empty,
        'delphixInitiatedBackupFlag' => undef,
        'singleDatabaseIngestion' => \@empty,
        'delphixInitiatedBackup' => \@empty,
        'stagingPushFlag' => undef,
        'singleDatabaseIngestionFlag' => undef,
        'postgresPort' => $staging_port,
        "mountLocation" => $mountbase
    );


    if (lc $ingestiontype eq 'single') {
        $parameters{"delphixInitiatedBackupFlag"} = JSON::false;
        $parameters{"stagingPushFlag"} = JSON::false;
        $parameters{"singleDatabaseIngestionFlag"} = JSON::true;
        my @arr;

        if ((!defined($dbuser)) || (!defined($password)) || (!defined($sourcehostname)) || (!defined($sourceport))
            || (!defined($singledbname)) || (!defined($restorejobs)) || (!defined($dumpjobs)) || (!defined($dumpdir)) ) {
            print "Parameters -dbuser, -password, -sourcehostname, -sourceport, -singledbname, -restorejobs, -dumpjobs, -dumpdir are mandatory with single ingestion mode. Exiting\n";
            return undef;
        }

        my %param_single = (
                "databaseName" => $singledbname,
                "databaseUserName" => $dbuser,
                "databaseUserPassword" => $password,
                "dumpDir" => $dumpdir,
                "dumpJobs" => $dumpjobs,
                "restoreJobs" => $restorejobs,
                "sourceHost" => $sourcehostname,
                "sourcePort" => $sourceport
        );

        push(@arr, \%param_single);

        $parameters{"singleDatabaseIngestion"} = \@arr;

    }
    elsif (lc $ingestiontype eq 'initiated') {
        $parameters{"delphixInitiatedBackupFlag"} = JSON::true;
        $parameters{"stagingPushFlag"} = JSON::false;
        $parameters{"singleDatabaseIngestionFlag"} = JSON::false;
        my @arr;

        if ((!defined($dbuser)) || (!defined($password)) || (!defined($sourcehostname)) || (!defined($sourceport)) ) {
            print "Parameters -dbuser, -password, -sourcehostname, -sourceport are mandatory with initiated ingestion mode. Exiting\n";
            return undef;
        }

        my %param_single = (
                "userName" => $dbuser,
                "userPass" => $password,
                "sourceHostAddress" => $sourcehostname,
                "postgresSourcePort" => $sourceport
        );

        push(@arr, \%param_single);

        $parameters{"delphixInitiatedBackup"} = \@arr;


    }
    elsif (lc $ingestiontype eq 'externalbackup') {

        my @arr;
        my @arr_sync;

        if ((!defined($backup_dir)) || (!defined($backup_dir_log)) || (!defined($keepinsync)) ) {
            print "Parameters -backup_dir, -backup_dir_log and -keepinsync are mandatory external backup ingestion mode. Exiting\n";
            return undef;
        }

        $parameters{"delphixInitiatedBackupFlag"} = JSON::false;
        $parameters{"stagingPushFlag"} = JSON::false;
        $parameters{"singleDatabaseIngestionFlag"} = JSON::false;

        if (!(defined($keepinsync) && (( lc $keepinsync eq 'no' ) || (lc $keepinsync eq 'yes')))) {
            print "Parameter keepinsync should be no or yes. Exiting.\n";
            return undef;
        }

        my $insync;

        if ( lc $keepinsync eq 'no' ) {
            $insync = JSON::false;
        } else {
            $insync = JSON::true;

            if ((!defined($dbuser)) || (!defined($password)) || (!defined($sourcehostname)) || (!defined($sourceport)) ) {
                print "Parameters -dbuser, -password, -sourcehostname, -sourceport are mandatory with keepinsync set to yes. Exiting\n";
                return undef;
            }

            my %param_sync = (
                    "userName" => $dbuser,
                    "userPass" => $password,
                    "sourceHostAddress" => $sourcehostname,
                    "postgresSourcePort" => $sourceport
            );

            push(@arr_sync, \%param_sync);
            $parameters{"delphixInitiatedBackup"} = \@arr_sync; 
        }

        my %param_single = (
                "backupPath" => $backup_dir,
                "walLogPath" => $backup_dir_log,
                "keepStagingInSync" => $insync,
        );

        push(@arr, \%param_single);
        $parameters{"externalBackup"} = \@arr;

    }

    if (defined($customparameters)) {
        $parameters{"configSettingsStg"} = $self->setCustomParams($customparameters);
    }

    return $self->PluginVDB_obj::addSource(
        $sourcename,
        $dbuser,
        $password,
        $dsourcename,
        $group,
        $logsync,
        $stageenv,
        $stageinst,
        $stage_os_user,
        $backup_dir ,
        $sourcehostname,
        $sourceport,
        $ingestiontype,
        $dumpdir,
        $restorejobs,
        $dumpjobs,
        $staging_port,
        $singledbname,
        $mountbase,
        \%parameters
    ) ;

}


# Procedure setSource
# parameters:
# - name - source name
# Set dsource reference by name for new db.
# Return 0 if success, 1 if not found

sub setSource {
    my $self = shift;
    #my $name = shift;
    my $sourceitem = shift;
    logger($self->{_debug}, "Entering PostgresVDB_obj::setSource",1);

    my $dlpxObject = $self->{_dlpxObject};
    my $debug = $self->{_debug};


    if (defined ($sourceitem)) {
        my $sourcetype = $sourceitem->{container}->{'type'};

        if (($sourcetype eq 'AppDataContainer') || ($sourcetype eq 'AppDataVirtualSource') || ($sourcetype eq 'AppDataLinkedSource') ) {
            $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"}  = $sourceitem->{container}->{reference};
            return 0;
        } else {
            return 1;
        }
    } else {
        return 1;
    }

}


# Procedure setName
# parameters:
# - contname - container name
# - dbname - database name
# Set name for new db.

sub setName {
    my $self = shift;
    my $contname = shift;
    my $dbname = shift;
    logger($self->{_debug}, "Entering PostgresVDB_obj::setName",1);

    $self->{"NEWDB"}->{"container"}->{"name"} = $contname;
    $self->{"NEWDB"}->{"sourceConfig"}->{"name"} = $contname;

}


# Procedure createVDB
# parameters:
# - group - new DB group
# - env - new DB environment
# - inst - new DB instance
# - mountpoint - moint point
# - port - VDB port
# Start job to create postgresql VBD
# all above parameters are required. Additional parameters should by set by setXXXX procedures before this one is called
# Return job number if provisioning has been started, otherwise return undef

sub createVDB {
    my $self = shift;
    my $group = shift;
    my $env = shift;
    my $inst = shift;
    my $mountpoint = shift;
    my $port = shift;
    my $customparameters = shift;

    logger($self->{_debug}, "Entering PostgresVDB_obj::createVDB",1);

    my %parameters;

    $parameters{"mountLocation"} = $mountpoint;
    $parameters{"postgresPort"} = $port;

    if (defined($customparameters)) {
        $parameters{"configSettingsStg"} = $self->setCustomParams($customparameters);
    }

    return $self->PluginVDB_obj::createVDB(
        $group,
        $env,
        $inst,
        $mountpoint,
        $port,
        \%parameters
    );

}


sub setCustomParams {
    my $self = shift;
    my $customparameters = shift;

    my @postgresql_params;
    if (defined($customparameters)) {
        my @pair;
        for my $param (@{$customparameters}) {
            if ($param =~ /=/ ) {
                @pair = split("=", $param);
                if (scalar(@pair)!=2) {
                    print "Error with vdbparameter definition. $param. Exiting\n";
                    return undef;
                }
                push(@postgresql_params, {
                    "propertyName" => $pair[0],
                    "value"=> $pair[1],
                    "commentProperty" => JSON::false
                });
            }
            elsif (($toremove) = $param =~ /^#(.*)/ ) {
                push(@postgresql_params, {
                    "propertyName" => $toremove,
                    "value"=> "",
                    "commentProperty" => JSON::true
                });
            }
            else {
                print "Error with vdbparameter definition. $param. Exiting\n";
                return undef;
            }
        }
    }
    return \@postgresql_params;
}

# Procedure getDatabaseName
# parameters: none
# Return database name

sub getDatabaseName
{
    my $self = shift;
    logger($self->{_debug}, "Entering PostgresVDB_obj::getDatabaseName",1);
    return $self->{source}->{name};
}

#
# End of package


1;