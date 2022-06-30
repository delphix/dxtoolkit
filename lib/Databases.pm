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
# Program Name : Databases.pm
# Description  : Delphix Engine Database objects
# It's include the following classes:
# - Databases - class which list of Databases (source / target) defined in Delphix Engine
# Author       : Marcin Przepiorowski
# Created: 13 Apr 2015 (v2.0.0)
#



package Databases;
use Data::Dumper;
use JSON;
use strict;
use warnings;


use Source_obj;
use SourceConfig_obj;
use Repository_obj;
use Environment_obj;
use Host_obj;
require VDB_obj;
use OracleVDB_obj;
use MSSQLVDB_obj;
use SybaseVDB_obj;
use AppDataVDB_obj;
use DB2VDB_obj;
use Toolkit_obj;
use Toolkit_helpers qw (logger);
use Encode qw(decode_utf8);

# constructor
# parameters
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $debug = shift;

    logger($debug, "Entering Databases::constructor",1);

    my %dbs; # hash of all databases defined in DE

    # auxiliary objects to resolve dependency like host or database type
    my $source = new Source_obj($dlpxObject, $debug);
    my $sourceconfigs = new SourceConfig_obj($dlpxObject, $debug);
    my $repositories = new Repository_obj($dlpxObject, $debug);
    my $environments = new Environment_obj($dlpxObject, $debug);
    my $hosts = new Host_obj($dlpxObject, $debug);
    my $namespace = new Namespace_obj ( $dlpxObject, $debug );


    my $self = {
        _dbs => \%dbs,                      # hash with all DB on Delphix Engine - each DB is a object
        _dlpxObject => $dlpxObject,         # connection object to Delphix Engine
        _source => $source,                 # list of Delphix Source objects ( this is not dsource !!!)
        _sourceconfigs => $sourceconfigs,   # list of configuration of database objects ( sources )
        _repositories => $repositories,     # list of repositories ( DB home / host )
        _environments => $environments,     # environments list (host / user )
        _hosts => $hosts,                    # list of hosts
        _debug => $debug,
        _namespace => $namespace
   };

    bless($self,$classname);

    # load all databases from Delphix Engine into hash
    $self->LoadDBList();
    return $self;
}


# Procedure LoadDBList
# parameters: none
# Load all databases from Delphix Engine into hash of VDB objects
# VDB class if defined in VDB_obj file
# each database has all information loaded into object

sub LoadDBList
{
    my $self = shift;
    logger($self->{_debug}, "Entering Databases::LoadDBList",1);


    my $toolkits = new Toolkit_obj($self->{_dlpxObject}, $self->{_debug});

    my $operation = "resources/json/delphix/database";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);


    # load existing has into $dbs
    my $dbs = $self->{_dbs};

    # load list of databases into array - res
    my @res = @{$result->{result}};

    # temporaty db object
    my $db;

    # for every db in array - create object of proper type
    for my $dbitem (@res) {

        if ($dbitem->{type} eq 'OracleDatabaseContainer' )
        {
            if (defined($dbitem->{contentType})) {
              if ($dbitem->{contentType} eq 'AUX_CDB') {
                next;
              } else {
                $db = OracleVDB_obj->new($self->{_dlpxObject}, $self->{_debug});
              }
            } else {
              print "Content type for $dbitem->{name} not found\n";
              next;
            }

        }
        elsif ($dbitem->{type} eq 'MSSqlDatabaseContainer' )
        {
            $db = MSSQLVDB_obj->new($self->{_dlpxObject}, $self->{_debug});
        }
        elsif ($dbitem->{type} eq 'PgSQLDatabaseContainer' )
        {
            $db = PostgresVDB_obj->new($self->{_dlpxObject}, $self->{_debug});
        }
        elsif ($dbitem->{type} eq 'ASEDBContainer' )
        {
            $db = SybaseVDB_obj->new($self->{_dlpxObject}, $self->{_debug});
        }
        elsif ($dbitem->{type} eq 'MySQLDatabaseContainer' )
        {
            $db = MySQLVDB_obj->new($self->{_dlpxObject}, $self->{_debug});
        }
        elsif ($dbitem->{type} eq 'AppDataContainer' )
        {
            if (defined($dbitem->{toolkit}) && defined($toolkits->getName($dbitem->{toolkit})) ) {
                if ($toolkits->getName($dbitem->{toolkit}) eq 'db2db') {
                  $db = DB2VDB_obj->new($self->{_dlpxObject}, $self->{_debug});
                } else {
                  $db = AppDataVDB_obj->new($self->{_dlpxObject}, $self->{_debug});
                }
            } else {
                $db = AppDataVDB_obj->new($self->{_dlpxObject}, $self->{_debug});
            }
        }
        else
        {
            $db = VDB_obj->new($self->{_dlpxObject}, $self->{_debug});
        }



        $db->{"container"} = $dbitem;




        $db->{"namespace"} = $self->{_namespace};

        # load DB source information
        logger($self->{_debug},"db name $dbitem->{name} ",2);
        logger($self->{_debug},"source ref $dbitem->{reference} ",2);
        $db->{"source"}  = $self->{_source}->getSource($dbitem->{reference});

        $db->{_source} = $self->{_source};

        # load sourceConfig object
        my $configname = $self->{_source}->getSourceConfig($dbitem->{reference}); # Source Config name is inside source object
        my $stagingname = $self->{_source}->getStaging($dbitem->{reference}); # Staging Source is other object

        if ( defined($configname) ) {
          logger($self->{_debug},"config name - $configname ",2);
        } else {
          logger($self->{_debug},"config name - not available ",2);
        }
        logger($self->{_debug},"Source config to assign ",2);
        my $sc = Dumper $self->{_sourceconfigs}->getSourceConfig($configname);
        logger($self->{_debug}, $sc , 2);
        $db->{"sourceConfig"}  = $self->{_sourceconfigs}->getSourceConfig($configname);

        #added to keep object
        $db->{_sourceconfig} = $self->{_sourceconfigs};
        $db->{_repository} = $self->{_repositories};

        # load repostory information ( home / env )
        my $repository = $self->{_sourceconfigs}->getRepository($configname); # Source Config name is inside source object

        if (defined($repository)) {
          #unlinked dSource
          logger($self->{_debug},"repository name - $repository ",2);
          $db->{"repository"}  = $self->{_repositories}->getRepository($repository);
        } else {
          logger($self->{_debug},"repository name - undefined ",2);
        }

        # load environment information
        my $environment = $self->{_repositories}->getEnvironment($repository); # Environment name is inside source config object
        logger($self->{_debug},"environment name - $environment ",2);
        $db->{"environment"}  = $self->{_environments}->getEnvironment($environment);

        $db->{_environment} = $self->{_environments};

        # load host information
        my $host = $self->{_environments}->getHost($environment); # host name is inside environment object
        logger($self->{_debug},"host name - $host ",2);
        if ( $host eq 'CLUSTER' ) {

            my %fake = ( "name" => "CLUSTER");
            $db->{"host"}  = \%fake;
        } else {
            $db->{"host"}  = $self->{_hosts}->getHost($host);
        }


        # for oracle load instances

        if ($db->getDBType eq 'oracle') {
            my %orainst;

            if ( $db->getInstances() ne 'UNKNOWN') {

                for my $inst (@{$db->getInstances()}) {

                    if ($inst->{type} eq 'OracleInstance') {
                        $orainst{$inst->{instanceNumber}}{host} = $self->{_hosts}->getHost($host)->{name};
                        $orainst{$inst->{instanceNumber}}{name} = $inst->{instanceName};
                    } elsif ($inst->{type} eq 'OracleRACInstance') {
                        my $insthost = $self->{_environments}->getHost($inst->{node});
                        my $instnode = $self->{_environments}->getName($inst->{node});
                        $orainst{$inst->{instanceNumber}}{host} = $self->{_hosts}->getHost($insthost)->{name};
                        $orainst{$inst->{instanceNumber}}{name} = $inst->{instanceName};
                        $orainst{$inst->{instanceNumber}}{nodename} = $instnode;
                    }
                }

                $db->{instances} = \%orainst;

            }

            # pdb check
            my $cdb = $db->getCDBContainerRef();

            if (defined($cdb) && defined($self->{_source}->getSourceByConfig($cdb))) {
              $db->{"cdb"} = $self->{_source}->getSourceByConfig($cdb)->{container};
            }

        }

        # load parent information for VDB
        if ($db->getType() eq 'VDB') {
            my $parent_ref = $db->{"container"}->{provisionContainer};

            if (defined($parent_ref)) {
                logger($self->{_debug},"parent ref  - $parent_ref ",2);
                $db->{"provisionContainer_name"}  = $self->{_source}->getName($parent_ref);
            } else {
              logger($self->{_debug},"parent ref  - not defined ",2);
            }
        }


        if ( $stagingname ) {
        # if DB has staging we need to add staging environment

            $db->{"staging_source"}  = $self->{_source}->getSource($stagingname);

            my $configname = $self->{_source}->getSourceConfig($stagingname); # Source Config name is inside source object
            logger($self->{_debug},"db name $dbitem->{name} ",2);
            logger($self->{_debug},"staging config name - $configname ",2);
            $db->{"staging_sourceConfig"}  = $self->{_sourceconfigs}->getSourceConfig($configname);

            my $repository = $self->{_sourceconfigs}->getRepository($configname); # Source Config name is inside source object
            logger($self->{_debug},"staging repository name - $repository ",2);
            $db->{"staging_repository"}  = $self->{_repositories}->getRepository($repository);

            my $environment = $self->{_repositories}->getEnvironment($repository); # Environment name is inside source config object
            logger($self->{_debug},"staging environment name - $environment ",2);
            $db->{"staging_environment"}  = $self->{_environments}->getEnvironment($environment);

            my $host = $self->{_environments}->getHost($environment); # host name is inside environment object
            logger($self->{_debug},"staging host name - $host ",2);
            $db->{"staging_host"}  = $self->{_hosts}->getHost($host);
        }


        # add database to hash of DB objects

        $db->{_databases} = $self;

        $dbs->{$dbitem->{reference}} = $db;

    }
}


# Procedure getDB
# parameters:
# - refrence - container name
# Return database object for a particular DB refrence
# object type is one of objects defined in VBD_obj ( like OracleVDB, MSSQLVDB)

sub getDB {
    my $self = shift;
    my $refrence = shift;

    logger($self->{_debug}, "Entering Databases::getDB",1);
    my $dbs = $self->{_dbs};

    return $dbs->{$refrence};
}


# Procedure getDBByName
# parameters: name of database
# Return database object(s) for a particular DB name
# object type is one of objects defined in VBD_obj ( like OracleVDB, MSSQLVDB)
# mssql can return more db

sub getDBByName {
    my $self = shift;
    my $name = shift;

    $name = decode_utf8($name);

    logger($self->{_debug}, "Entering Databases::getDBByName",1);
    my $dbs = $self->{_dbs};
    my @ret;

    for my $dbitem ( sort ( keys %{ $dbs } ) ) {
        my $db = $dbs->{$dbitem};
        if ( $db->getName() eq $name) {
            push(@ret,$db);
        }
    }

    return \@ret;

}


# Procedure getName
# parameters:
# - reference
# Return database name for particular reference

sub getName {
    my $self = shift;
    my $reference = shift;
    my $ret;

    logger($self->{_debug}, "Entering Databases::getName",1);
    my $dbs = $self->{_dbs};

    if (defined($dbs->{$reference}) ) {
        $ret = $dbs->{$reference}->getName();
    }
    return $ret;
}


# Procedure getDBList
# parameters: none
# Return list of database names loaded into hash

sub getDBList {
    my $self = shift;
    logger($self->{_debug}, "Entering Databases::getDBList",1);
    my $dbs = $self->{_dbs};

    my @sorteddb = sort { $self->getDB($a)->getName() cmp $self->getDB($b)->getName() } ( keys %{$dbs} );

    logger($self->{_debug}, join(",", @sorteddb) ,1);
    logger($self->{_debug}, "Finishing Databases::getDBList",1);
    return  @sorteddb;
}



# Procedure getDBForEnvironment
# parameters: env name
# Return list of database names which are provisioned on host name / IP

sub getDBForEnvironment
{
    my $self = shift;
    my $env = shift;
    my @dbs;

    logger($self->{_debug}, "Entering Databases::getDBForEnvironment",1);
    for my $dbname ( $self->getDBList() ) {
        my $dbobj = $self->getDB($dbname);
        if ( $dbobj->getEnvironmentName() eq $env) {
            push (@dbs, $dbname)
        }
    }

    return  sort { $self->getDB($a)->getName() cmp $self->getDB($b)->getName() }  ( @dbs );
}

# Procedure getDBForHost
# parameters: host name / IP
# Return list of database names which are provisioned on host name / IP

sub getDBForHost
{
    my $self = shift;
    my $host = shift;
    my $instance_number = shift;
    my @dbs;

    logger($self->{_debug}, "Entering Databases::getDBForHost",1);
    for my $dbname ( $self->getDBList() ) {
        my $dbobj = $self->getDB($dbname);

        if (($dbobj->getDBType() eq 'oracle') && (defined($dbobj->getCDBContainerRef()))) {
          # database has a CDB container so it's a PDB and there is no instance info
          # dbobj will be switch to container to show data

          my $contsource = $self->{_source}->getSourceByConfig($dbobj->getCDBContainerRef());
          $dbobj = $self->getDB($contsource->{container});
        }

        if ( $dbobj->getHost() eq 'CLUSTER') {

            if ($dbobj->getDBType() eq 'oracle') {
              my $instances = $dbobj->getInstances();
              if (defined($instances)) {
                  if (defined($instance_number)) {
                      if ($dbobj->getInstanceHost($instance_number) eq $host) {
                          push (@dbs, $dbname)
                      }
                  } else {
                      for my $inst ( @{$dbobj->getInstances()} ) {
                          if ($dbobj->getInstanceHost($inst->{instanceNumber}) eq $host) {
                              push (@dbs, $dbname)
                          }
                      }
                  }
              }
            } elsif ($dbobj->getDBType() eq 'mssql') {
              my @nodearray = $dbobj->getNodes();
              for my $node (@nodearray) {
                if ($self->{_hosts}->getHostAddr($node) eq $host) {
                  push(@dbs, $dbname);
                }
              }

            }
        } else {
            if ( $dbobj->getHost() eq $host ) {
                push (@dbs, $dbname)
            }
        }
    }

    return  sort { $self->getDB($a)->getName() cmp $self->getDB($b)->getName() } ( @dbs );
}

# Procedure getDBByType
# parameters: type - dsource / VDB
# Return list of database names which are vdb / dsource

sub getDBByType
{
    my $self = shift;
    my $type = shift;
    my @dbs;

    logger($self->{_debug}, "Entering Databases::getDBByType",1);
    for my $dbname ( $self->getDBList() ) {
        my $dbobj = $self->getDB($dbname);
        if ( lc ($dbobj->getType()) eq lc ($type) ) {
            push (@dbs, $dbname)
        }
    }

    return  sort { $self->getDB($a)->getName() cmp $self->getDB($b)->getName() } ( @dbs );
}

# Procedure getDBByParent
# parameters: parent - name
# Return list of database names which are child of dSource

sub getDBByParent
{
    my $self = shift;
    my $parent = shift;
    my @dbs;

    logger($self->{_debug}, "Entering Databases::getDBByParent",1);

    for my $dbname ( $self->getDBList() ) {
        my $dbobj = $self->getDB($dbname);

        if ($dbobj->getParentContainer() ne '') {
            my $parentname = $self->getDB($dbobj->getParentContainer())->getName();
            if ( lc ($parentname) eq lc ($parent) ) {
                push (@dbs, $dbname)
            }
        }
    }

    return  sort { $self->getDB($a)->getName() cmp $self->getDB($b)->getName() } ( @dbs );
}


# Procedure getDBForGroup
# parameters: type - dsource / VDB
# Return list of database names which are vdb / dsource

sub getDBForGroup
{
    my $self = shift;
    my $group = shift;
    my @dbs;

    logger($self->{_debug}, "Entering Databases::getDBForGroup",1);
    for my $dbname ( $self->getDBList() ) {
        my $dbobj = $self->getDB($dbname);
        if ( $dbobj->getGroup() eq $group ) {
            push (@dbs, $dbname)
        }
    }

    return  sort { $self->getDB($a)->getName() cmp $self->getDB($b)->getName() } ( @dbs );
}

# Procedure getDBForInstanceName
# parameters:
# instance name
# Return list of database names which has a instance name

sub getDBForInstanceName
{
    my $self = shift;
    my $instancename = shift;
    my @dbs;

    logger($self->{_debug}, "Entering Databases::getDBForInstanceName",1);
    for my $dbname ( $self->getDBList() ) {
        my $dbobj = $self->getDB($dbname);
        if (defined($dbobj->{instances})) {
          # Oracle
          for my $i (keys %{$dbobj->{instances}}) {
            if ($dbobj->{instances}->{$i}->{name} eq $instancename) {
              push (@dbs, $dbname);
            }
          }
        }

    }

    return  sort { $self->getDB($a)->getName() cmp $self->getDB($b)->getName() } ( @dbs );
}


# Procedure getDBByCreationTime
# parameters:
# creation time
# Return list of database older than creation time

sub getDBByCreationTime
{
    my $self = shift;
    my $creationtime = shift;
    my @dbs;

    logger($self->{_debug}, "Entering Databases::getDBByCreationTime",1);
    for my $dbname ( $self->getDBList() ) {
        my $dbobj = $self->getDB($dbname);
        my $crtime = $dbobj->getCreationTime();
        if (defined($crtime)) {
          if ( $crtime lt $creationtime ) {
              push (@dbs, $dbname);
          }
        } else {
          logger($self->{_debug}, $dbobj->getName() . " doesn't have a creation time. Adding to list");
          print $dbobj->getName() . " doesn't have a creation time. Adding to output as exception\n";
          push (@dbs, $dbname);
        }
    }

    return  sort { $self->getDB($a)->getName() cmp $self->getDB($b)->getName() } ( @dbs );
}

# Procedure getDBForRepository
# parameters: repo name
# Return list of database names which are provisioned using repo

sub getDBForRepository
{
    my $self = shift;
    my $repo = shift;
    my @dbs;

    logger($self->{_debug}, "Entering Databases::getDBForRepository",1);
    for my $dbname ( $self->getDBList() ) {
        my $dbobj = $self->getDB($dbname);
        if ( $dbobj->getHome() eq $repo) {
            push (@dbs, $dbname)
        }
    }

    return  sort { $self->getDB($a)->getName() cmp $self->getDB($b)->getName() }  ( @dbs );
}

# Procedure getPrimaryDB
# Return list of database ref which are primary

sub getPrimaryDB
{
    my $self = shift;
    my $group = shift;
    my @dbs;

    logger($self->{_debug}, "Entering Databases::getPrimaryDB",1);
    for my $dbname ( $self->getDBList() ) {
        my $dbobj = $self->getDB($dbname);
        if ( $dbobj->isReplica() eq 'NO' ) {
            push (@dbs, $dbname)
        }
    }

    return  ( @dbs );
}


# Procedure generateHierarchy
# parameters:
# - remote - mapping of local / parent objects
# - databases_parent - parent databases

sub generateHierarchy
{
    my $self = shift;
    my $remote = shift;
    my $databases_parent = shift;
    logger($self->{_debug}, "Entering Databases::generateHierarchy",1);



    my %hierarchy;

    for my $dbitem ( $self->getDBList() ) {

      my $dbobj = $self->getDB($dbitem);

      my $parent_ref = $dbobj->getParentContainer();

      # if there is no parent, but VDB is replicated
      # we need to add parent container from source or set to no local
      # if VDB is not replicated but there is no parent - set parent to deleted
      if ($parent_ref eq '') {
        if ($dbobj->getType() eq 'VDB') {
          if ($dbobj->isReplica() eq 'YES') {
            if (defined($remote->{$dbitem})) {
              $parent_ref = $remote->{$dbitem};
            } else {
              $parent_ref = 'notlocal'
            }
          }
          else {
            $parent_ref = 'deleted';
          }
        }

      } else {
        # @l is needed as we may have same container id between engines
        $parent_ref = $parent_ref . "\@l";
      }


      my $dbitemext = $dbitem . "\@l" ;


      $hierarchy{$dbitemext}{parent} = $parent_ref;
      $hierarchy{$dbitemext}{source} = 'l';

    }

    if (defined($databases_parent)) {

      for my $dbitem ( $databases_parent->getDBList() ) {
        my $dbobj = $databases_parent->getDB($dbitem);
        my $parent_ref = $dbobj->getParentContainer();
        $hierarchy{$dbitem}{parent} = $parent_ref;
        $hierarchy{$dbitem}{source} = 'p';

      }

    }

    logger($self->{_debug}, \%hierarchy, 2);

    return \%hierarchy;

}


# Procedure finddSource
# parameters:
# - ref - VDB refrerence
# - hier - hierarchy hash
# Return a dSource and child timeflows


sub finddSource
{
    my $self = shift;
    my $ref = shift;
    my $hier = shift;
    my $clean = shift;

    logger($self->{_debug}, "Entering Databases::finddSource",1);

    my $local_ref = $ref . "\@l";
    my $child;
    my $parent;


    logger($self->{_debug}, "Find dSource for " . $local_ref, 2);

    #leave loop if there is no parent, parent is deleted or not local
    #local_ref - is pointed to a timeflow without parent (dSource)
    #child - is a child timeflow of local_ref

    do {
      $parent = $hier->{$local_ref}->{parent};

      if (!defined($hier->{$local_ref}->{parent})) {
        return ('deleted', undef);
      }

      logger($self->{_debug}, "Parent " . Dumper $parent . " for " . $local_ref, 2);

      if (($parent ne '') && ($parent ne 'deleted') && ($parent ne 'notlocal') ) {
          $child = $local_ref;
          $local_ref = $parent;
      }

    } while (($parent ne '') && ($parent ne 'deleted') && ($parent ne 'notlocal'));

    if ($parent eq 'deleted') {
      $local_ref = 'deleted';
      undef $child;
    }

    if ($parent eq 'notlocal') {
      $local_ref = 'notlocal';
      undef $child;
    }


    my $retref;
    my $retchild;

    if (defined($clean)) {
      if (($retref) = $local_ref =~ /(.*)\@l/ ) {
        $retref = $retref
      } else {
        $retref = $local_ref;
      }
      if (defined($child) && (($retchild) = $child =~ /(.*)\@l/ )) {
        $retchild = $retchild
      } else {
        $retchild = $child;
      }
    } else {
      $retref = $local_ref;
      $retchild = $child;
    }

    return ($retref, $retchild);

}


# Procedure returnHierarchy
# parameters:
# - ref - VDB refrerence
# - hier - hierarchy hash
# Return a array with databases hashes

sub returnHierarchy
{
    my $self = shift;
    my $ref = shift;
    my $hier = shift;

    logger($self->{_debug}, "Entering Databases::returnHierarchy",1);

    my $local_ref = $ref . "\@l";
    my $child;
    my $parent;

    my @retarr;

    logger($self->{_debug}, "Find dSource for " . $local_ref, 2);

    #leave loop if there is no parent, parent is deleted or not local
    #local_ref - is pointed to a timeflow without parent (dSource)
    #child - is a child timeflow of local_ref

    my $cleanref;

    do {
      $parent = $hier->{$local_ref}->{parent};
      my %hashpair;


      if (($cleanref) = $local_ref =~ /(.*)\@l/) {
        $cleanref = $cleanref;
      } else {
        $cleanref = $local_ref;
      }

      $hashpair{ref} = $cleanref;
      $hashpair{source} = $hier->{$local_ref}->{source};
      push(@retarr, \%hashpair);
      if (($parent ne '') && ($parent ne 'deleted') && ($parent ne 'notlocal') ) {
          $child = $local_ref;
          $local_ref = $parent;
      }

    } while (($parent ne '') && ($parent ne 'deleted') && ($parent ne 'notlocal'));

    return \@retarr;

}


sub get_sourceconfigs {
  my $self = shift;
  logger($self->{_debug}, "Entering Databases::get_sourceconfigs",1);
  return $self->{_sourceconfigs};
}


#
# End of package


1;
