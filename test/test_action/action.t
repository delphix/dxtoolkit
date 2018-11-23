use strict;
use Data::Dumper;
use Test::More tests => 5;
use Test::Script;
use LWP::UserAgent;
use lib '../../lib/';
use lib '../';
use lib '.';
use server;



my $server = server->new(8080);
$server->host('127.0.0.1');
$server->background();

 
script_compiles('../../bin/dx_get_audit.pl');

script_runs(['../../bin/dx_get_audit.pl', '-d', 'local', '-format','csv','-st','2017-10-06 15:26:48'] ,  "get audit data");

my $expected_stdout = <<EOF;
#Appliance,StartTime,State,User or Policy,Type,Details
local,2017-10-09 09:37:54 IST,COMPLETED,default retention,SNAPSHOT_DELETE,Delete snapshot "\@2017-09-28T10:20:19.295Z".
local,2017-10-09 09:37:54 IST,COMPLETED,default retention,SNAPSHOT_DELETE,Delete snapshot "\@2017-09-28T10:03:28.953".
local,2017-10-09 09:37:54 IST,FAILED,default snapshot,DB_SYNC,Run SnapSync for database "tests".
local,2017-10-09 09:37:55 IST,COMPLETED,default retention,SNAPSHOT_DELETE,Delete snapshot "\@2017-09-28T10:30:21.442Z".
local,2017-10-09 09:37:55 IST,COMPLETED,default retention,SNAPSHOT_DELETE,Delete snapshot "\@2017-09-28T10:32:13.950Z".
local,2017-10-09 09:37:56 IST,COMPLETED,default retention,SNAPSHOT_DELETE,Delete snapshot "\@2017-09-28T10:34:10.771Z".
local,2017-10-09 09:37:56 IST,COMPLETED,default retention,SNAPSHOT_DELETE,Delete snapshot "\@2017-09-28T10:22:27.148Z".
local,2017-10-09 09:37:57 IST,COMPLETED,default retention,SNAPSHOT_DELETE,Delete snapshot "\@2017-09-28T10:23:57.606Z".
local,2017-10-09 09:37:57 IST,COMPLETED,default retention,SNAPSHOT_DELETE,Delete snapshot "\@2017-09-28T10:25:00.263Z".
local,2017-10-09 09:37:57 IST,COMPLETED,default retention,SNAPSHOT_DELETE,Delete snapshot "\@2017-09-28T10:26:05.291Z".
local,2017-10-09 09:52:52 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "127.0.0.1".
local,2017-10-09 10:03:23 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "172.16.180.1".
local,2017-10-09 10:20:46 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "172.16.180.1".
local,2017-10-09 10:20:57 IST,COMPLETED,delphix_admin,MASKINGJOB_FETCH,Fetching all Masking Jobs from the local Delphix Masking Engine instance.
local,2017-10-09 11:01:08 IST,COMPLETED,delphix_admin,USER_LOGOUT,Log out user "delphix_admin".
local,2017-10-09 12:50:48 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "172.16.180.1".
local,2017-10-09 12:50:59 IST,COMPLETED,delphix_admin,MASKINGJOB_FETCH,Fetching all Masking Jobs from the local Delphix Masking Engine instance.
local,2017-10-09 12:53:38 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "172.16.180.1".
local,2017-10-09 12:53:51 IST,COMPLETED,delphix_admin,USER_LOGOUT,Log out user "delphix_admin".
local,2017-10-09 12:53:55 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "172.16.180.1".
local,2017-10-09 18:11:33 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "172.16.180.1".
local,2017-10-09 18:52:40 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "172.16.180.1".
local,2017-10-10 09:17:20 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "127.0.0.1".
local,2017-10-10 09:31:55 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "172.16.180.1".
local,2017-10-10 09:32:08 IST,COMPLETED,delphix_admin,MASKINGJOB_FETCH,Fetching all Masking Jobs from the local Delphix Masking Engine instance.
local,2017-10-10 09:32:37 IST,COMPLETED,delphix_admin,POLICY_APPLY,Apply policy "aaa" on target "Oracle dsource".
local,2017-10-10 09:33:02 IST,COMPLETED,delphix_admin,POLICY_UPDATE,Update policy "aaa".
local,2017-10-10 09:33:20 IST,COMPLETED,delphix_admin,POLICY_APPLY,Apply policy "aaa" on target "si4rac".
local,2017-10-10 09:33:34 IST,COMPLETED,delphix_admin,DB_SYNC,Run SnapSync for database "Oracle dsource".
local,2017-10-10 09:34:04 IST,COMPLETED,delphix_admin,JETSTREAM_USER_CONTAINER_DELETE,Delete Jet Stream data container "cs".
local,2017-10-10 09:34:11 IST,COMPLETED,delphix_admin,JETSTREAM_ADMIN_TEMPLATE_DELETE,Delete Jet Stream data template "st".
local,2017-10-10 09:34:27 IST,COMPLETED,delphix_admin,MASKINGJOB_FETCH,Fetching all Masking Jobs from the local Delphix Masking Engine instance.
local,2017-10-10 09:34:39 IST,COMPLETED,delphix_admin,POLICY_APPLY,Apply policy "aaa" on target "tests".
local,2017-10-10 09:34:55 IST,FAILED,delphix_admin,DB_SYNC,Run SnapSync for database "tests".
local,2017-10-10 09:35:03 IST,FAILED,delphix_admin,SOURCE_STOP,Stop dataset "tests".
local,2017-10-10 09:35:29 IST,COMPLETED,delphix_admin,SOURCE_START,Start dataset "si4rac".
local,2017-10-10 09:35:33 IST,FAILED,delphix_admin,DB_DELETE,Delete dataset "tests".
local,2017-10-10 09:35:47 IST,COMPLETED,delphix_admin,DB_DELETE,Delete dataset "tests".
local,2017-10-10 09:36:02 IST,COMPLETED,delphix_admin,CAPACITY_RECLAMATION,Space is being reclaimed.
local,2017-10-10 09:37:32 IST,FAILED,delphix_admin,DB_SYNC,Run SnapSync for database "si4rac".
local,2017-10-10 09:38:29 IST,FAILED,delphix_admin,DB_SYNC,Run SnapSync for database "si4rac".
local,2017-10-10 09:39:03 IST,COMPLETED,delphix_admin,DB_SYNC,Run SnapSync for database "si4rac".
local,2017-10-10 09:39:10 IST,COMPLETED,delphix_admin,DB_SYNC,Run SnapSync for database "si4rac".
local,2017-10-10 09:39:32 IST,COMPLETED,delphix_admin,SOURCE_STOP,Stop dataset "si4rac".
local,2017-10-10 09:40:19 IST,COMPLETED,delphix_admin,DB_PROVISION,Provision virtual database "snapsize".
local,2017-10-10 09:40:29 IST,COMPLETED,delphix_admin,POLICY_APPLY,Apply policy "Default Snapshot" on target "snapsize".
local,2017-10-10 09:42:25 IST,COMPLETED,delphix_admin,ORACLE_UPDATE_REDOLOGS,Update Oracle online redo log files for virtual database "snapsize".
local,2017-10-10 09:42:45 IST,COMPLETED,delphix_admin,DB_SYNC,Run SnapSync for database "snapsize".
local,2017-10-10 09:49:10 IST,COMPLETED,delphix_admin,USER_LOGOUT,Log out user "delphix_admin".
local,2017-10-10 09:49:32 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "172.16.180.1".
local,2017-10-10 10:03:37 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "127.0.0.1".
local,2017-10-10 10:34:04 IST,COMPLETED,delphix_admin,USER_LOGOUT,Log out user "delphix_admin".
local,2017-10-10 10:36:48 IST,COMPLETED,delphix_admin,SOURCE_STOP,Stop dataset "snapsize".
local,2017-10-10 10:37:10 IST,COMPLETED,delphix_admin,DB_PROVISION,Provision virtual database "depend".
local,2017-10-10 10:37:18 IST,COMPLETED,delphix_admin,POLICY_APPLY,Apply policy "Default Snapshot" on target "depend".
local,2017-10-10 10:38:22 IST,COMPLETED,delphix_admin,DB_SYNC,Run SnapSync for database "depend".
local,2017-10-10 10:41:54 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "127.0.0.1".
local,2017-10-10 10:43:40 IST,COMPLETED,delphix_admin,DB_DELETE,Delete dataset "autotest".
local,2017-10-10 10:43:40 IST,COMPLETED,delphix_admin,SOURCE_STOP,Stop dataset "autotest".
local,2017-10-10 10:43:49 IST,COMPLETED,delphix_admin,CAPACITY_RECLAMATION,Space is being reclaimed.
local,2017-10-10 10:46:39 IST,COMPLETED,delphix_admin,DB_PROVISION,Provision virtual database "autotest".
local,2017-10-10 10:46:48 IST,COMPLETED,delphix_admin,POLICY_APPLY,Apply policy "Default Snapshot" on target "autotest".
local,2017-10-10 10:46:52 IST,COMPLETED,delphix_admin,SOURCE_STOP,Stop dataset "depend".
local,2017-10-10 10:47:49 IST,COMPLETED,delphix_admin,DB_SYNC,Run SnapSync for database "autotest".
local,2017-10-10 10:49:29 IST,FAILED,delphix_admin,SNAPSHOT_DELETE,Delete snapshot "\@2017-09-28T10:21:08.908Z".
local,2017-10-10 10:50:43 IST,FAILED,delphix_admin,SNAPSHOT_DELETE,Delete snapshot "\@2017-09-28T10:21:08.908Z".
local,2017-10-10 10:56:57 IST,COMPLETED,delphix_admin,USER_LOGOUT,Log out user "delphix_admin".
local,2017-10-10 11:30:00 IST,COMPLETED,default snapshot,DB_SYNC,Run SnapSync for database "autotest".
local,2017-10-10 12:49:10 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "127.0.0.1".
local,2017-10-10 12:50:05 IST,COMPLETED,delphix_admin,SOURCE_STOP,Stop dataset "autotest".
local,2017-10-10 12:50:47 IST,COMPLETED,delphix_admin,POLICY_APPLY,Apply policy "Default Retention" on target "si4rac".
local,2017-10-10 12:51:23 IST,FAILED,delphix_admin,SNAPSHOT_DELETE,Delete snapshot "\@2017-10-10T08:39:04.451Z".
local,2017-10-10 12:51:23 IST,COMPLETED,delphix_admin,SNAPSHOT_DELETE,Delete snapshot "\@2017-10-10T08:39:12.008Z".
local,2017-10-10 12:51:38 IST,COMPLETED,delphix_admin,DB_DELETE,Delete dataset "snapsize".
local,2017-10-10 12:51:38 IST,COMPLETED,delphix_admin,SOURCE_STOP,Stop dataset "snapsize".
local,2017-10-10 12:51:49 IST,COMPLETED,delphix_admin,CAPACITY_RECLAMATION,Space is being reclaimed.
local,2017-10-10 12:52:00 IST,COMPLETED,default retention,SNAPSHOT_DELETE,Delete snapshot "\@2017-09-25T12:12:29.641Z".
local,2017-10-10 12:52:02 IST,FAILED,delphix_admin,SNAPSHOT_DELETE,Delete snapshot "\@2017-10-10T08:39:04.451Z".
local,2017-10-10 12:53:09 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "172.16.180.1".
local,2017-10-10 13:19:18 IST,COMPLETED,delphix_admin,USER_LOGOUT,Log out user "delphix_admin".
local,2017-10-10 13:33:49 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "172.16.180.1".
local,2017-10-10 16:17:39 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "172.16.180.1".
local,2017-10-11 17:10:17 IST,COMPLETED,delphix_admin,MASKINGJOB_FETCH,Fetching all Masking Jobs from the local Delphix Masking Engine instance.
local,2017-10-11 17:10:31 IST,COMPLETED,delphix_admin,SOURCE_ENABLE,Enable dataset "racdba".
local,2017-10-11 17:11:00 IST,COMPLETED,delphix_admin,ENVIRONMENT_REFRESH_AND_DISCOVER,Refresh and discover environment "racattack-cl".
local,2017-10-11 17:11:00 IST,COMPLETED,delphix_admin,ENVIRONMENT_REFRESH,Refresh environment "racattack-cl".
local,2017-10-11 17:11:00 IST,COMPLETED,delphix_admin,SOURCES_DISABLE,Disable a list of datasets in environment "racattack-cl".
local,2017-10-11 17:11:00 IST,COMPLETED,delphix_admin,SOURCE_DISABLE,Disable dataset "racdba".
local,2017-10-11 17:11:00 IST,COMPLETED,delphix_admin,HOSTS_REFRESH_BY_ENVIRONMENT,Refresh hosts in environment "racattack-cl".
local,2017-10-11 17:11:01 IST,COMPLETED,delphix_admin,HOST_REFRESH,Refresh host "172.16.180.61".
local,2017-10-11 17:11:01 IST,COMPLETED,delphix_admin,HOST_REFRESH,Refresh host "172.16.180.62".
local,2017-10-11 17:12:26 IST,COMPLETED,delphix_admin,SOURCES_ENABLE,Enable a list of datasets in environment "racattack-cl".
local,2017-10-11 17:12:26 IST,COMPLETED,delphix_admin,SOURCE_ENABLE,Enable dataset "racdba".
local,2017-10-11 17:12:26 IST,COMPLETED,delphix_admin,ENVIRONMENT_DISCOVER,Discover information and objects for environment "racattack-cl".
local,2017-10-11 17:12:33 IST,COMPLETED,delphix_admin,ENVIRONMENT_UPDATE,Update environment "racattack-cl".
local,2017-10-11 17:12:33 IST,COMPLETED,delphix_admin,ORACLE_CLUSTER_NODES_REFRESH,Refresh cluster nodes for cluster "racattack-cl".
local,2017-10-11 17:12:33 IST,COMPLETED,delphix_admin,ORACLE_CLUSTER_NODE_UPDATE,Update Oracle cluster node "collabn1".
local,2017-10-11 17:12:33 IST,COMPLETED,delphix_admin,ORACLE_CLUSTER_NODE_UPDATE,Update Oracle cluster node "collabn2".
local,2017-10-11 17:12:37 IST,COMPLETED,delphix_admin,ORACLE_LISTENER_UPDATE,Update Oracle listener "LISTENER_SCAN1".
local,2017-10-11 17:12:40 IST,COMPLETED,delphix_admin,ORACLE_LISTENER_UPDATE,Update Oracle listener "LISTENER".
local,2017-10-11 17:12:40 IST,COMPLETED,delphix_admin,ORACLE_LISTENER_UPDATE,Update Oracle listener "MGMTLSNR".
local,2017-10-11 17:12:44 IST,COMPLETED,delphix_admin,ORACLE_LISTENER_UPDATE,Update Oracle listener "LISTENER".
local,2017-10-11 17:12:44 IST,COMPLETED,delphix_admin,REPOSITORY_UPDATE,Update repository "/u01/app/oracle/12.1.0.2/rachome1".
local,2017-10-11 17:12:59 IST,COMPLETED,delphix_admin,SOURCE_CONFIG_UPDATE,Update source config "racdba".
local,2017-10-11 17:12:59 IST,COMPLETED,delphix_admin,ORACLE_CLUSTER_NODES_DELETE_NON_EXISTING,Delete non-existing cluster nodes for cluster "racattack-cl".
local,2017-10-11 17:12:59 IST,COMPLETED,delphix_admin,ENVIRONMENT_UPDATE_SOURCES,Update all sources in environment "racattack-cl".
local,2017-10-11 17:13:00 IST,COMPLETED,delphix_admin,SOURCES_DISABLE,Disable a list of datasets in environment "racattack-cl".
local,2017-10-11 17:13:00 IST,COMPLETED,delphix_admin,SOURCE_DISABLE,Disable dataset "racdba".
local,2017-10-11 17:13:00 IST,COMPLETED,delphix_admin,SOURCES_ENABLE,Enable a list of datasets in environment "racattack-cl".
local,2017-10-11 17:13:00 IST,COMPLETED,delphix_admin,SOURCE_ENABLE,Enable dataset "racdba".
local,2017-10-11 17:13:44 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "172.16.180.1".
local,2017-10-11 17:13:45 IST,FAILED,delphix_admin,DB_SYNC,Run SnapSync for database "racdba".
local,2017-10-11 17:15:55 IST,COMPLETED,delphix_admin,SOURCE_CONFIG_UPDATE,Update source config "racdba".
local,2017-10-11 17:16:12 IST,FAILED,delphix_admin,DB_SYNC,Run SnapSync for database "racdba".
local,2017-10-11 17:21:05 IST,COMPLETED,delphix_admin,DB_SYNC,Run SnapSync for database "racdba".
local,2017-10-11 17:27:18 IST,COMPLETED,delphix_admin,DB_REFRESH,Refresh database "si4rac".
local,2017-10-11 17:27:29 IST,COMPLETED,delphix_admin,SOURCE_STOP,Stop dataset "si4rac".
local,2017-10-11 17:29:31 IST,COMPLETED,delphix_admin,DB_SYNC,Run SnapSync for database "si4rac".
local,2017-10-11 17:33:51 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "127.0.0.1".
local,2017-10-11 17:36:03 IST,COMPLETED,delphix_admin,DB_SYNC,Run SnapSync for database "racdba".
local,2017-10-11 17:37:30 IST,COMPLETED,delphix_admin,DB_SYNC,Run SnapSync for database "racdba".
local,2017-10-11 17:38:27 IST,COMPLETED,delphix_admin,DB_SYNC,Run SnapSync for database "racdba".
local,2017-10-11 17:40:51 IST,COMPLETED,delphix_admin,DB_DELETE,Delete dataset "si4rac".
local,2017-10-11 17:40:51 IST,COMPLETED,delphix_admin,SOURCE_STOP,Stop dataset "si4rac".
local,2017-10-11 17:41:05 IST,COMPLETED,delphix_admin,CAPACITY_RECLAMATION,Space is being reclaimed.
local,2017-10-11 17:41:15 IST,COMPLETED,delphix_admin,DB_DELETE,Delete dataset "racdba".
local,2017-10-11 17:41:19 IST,COMPLETED,delphix_admin,CAPACITY_RECLAMATION,Space is being reclaimed.
local,2017-10-11 17:41:43 IST,COMPLETED,delphix_admin,SOURCE_CONFIG_UPDATE,Update source config "racdba".
local,2017-10-11 17:41:57 IST,COMPLETED,delphix_admin,DB_LINK,Link dSource "racdba" from source "racdba".
local,2017-10-11 17:42:08 IST,COMPLETED,delphix_admin,POLICY_APPLY,Apply policy "Default SnapSync" on target "racdba".
local,2017-10-11 17:42:08 IST,COMPLETED,delphix_admin,POLICY_APPLY,Apply policy "aaa" on target "racdba".
local,2017-10-11 17:42:09 IST,FAILED,delphix_admin,DB_SYNC,Run SnapSync for database "racdba".
local,2017-10-11 17:44:09 IST,COMPLETED,delphix_admin,USER_LOGOUT,Log out user "delphix_admin".
local,2017-10-11 17:46:53 IST,COMPLETED,delphix_admin,DB_SYNC,Run SnapSync for database "racdba".
local,2017-10-11 17:54:53 IST,COMPLETED,delphix_admin,DB_SYNC,Run SnapSync for database "racdba".
local,2017-10-11 17:56:38 IST,COMPLETED,delphix_admin,SOURCE_DISABLE,Disable dataset "racdba".
local,2017-10-11 18:00:46 IST,COMPLETED,delphix_admin,DB_PROVISION,Provision virtual database "si4rac".
local,2017-10-11 18:00:56 IST,COMPLETED,delphix_admin,POLICY_APPLY,Apply policy "Default Snapshot" on target "si4rac".
local,2017-10-11 18:02:31 IST,COMPLETED,delphix_admin,DB_SYNC,Run SnapSync for database "si4rac".
local,2017-10-12 16:37:27 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "172.16.180.1".
local,2017-10-12 16:38:02 IST,FAILED,default snapshot,DB_SYNC,Run SnapSync for database "si4rac".
local,2017-10-13 13:46:41 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "127.0.0.1".
local,2017-10-13 13:56:48 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "172.16.180.1".
local,2017-10-13 13:56:57 IST,COMPLETED,delphix_admin,MASKINGJOB_FETCH,Fetching all Masking Jobs from the local Delphix Masking Engine instance.
local,2017-10-13 13:57:49 IST,COMPLETED,delphix_admin,DB_DELETE,Delete dataset "autotest".
local,2017-10-13 13:57:49 IST,COMPLETED,delphix_admin,SOURCE_STOP,Stop dataset "autotest".
local,2017-10-13 13:58:09 IST,COMPLETED,delphix_admin,CAPACITY_RECLAMATION,Space is being reclaimed.
local,2017-10-13 13:58:49 IST,COMPLETED,delphix_admin,DB_DELETE,Delete dataset "depend".
local,2017-10-13 13:58:49 IST,COMPLETED,delphix_admin,SOURCE_STOP,Stop dataset "depend".
local,2017-10-13 13:58:59 IST,COMPLETED,delphix_admin,CAPACITY_RECLAMATION,Space is being reclaimed.
local,2017-10-13 13:59:06 IST,COMPLETED,delphix_admin,DB_DELETE,Delete dataset "Oracle dsource".
local,2017-10-13 13:59:07 IST,COMPLETED,delphix_admin,CAPACITY_RECLAMATION,Space is being reclaimed.
local,2017-10-13 13:59:46 IST,COMPLETED,delphix_admin,SOURCE_CONFIG_UPDATE,Update source config "orcl".
local,2017-10-13 14:00:16 IST,COMPLETED,delphix_admin,DB_LINK,Link dSource "Oracle dsource" from source "Oracle dsource".
local,2017-10-13 14:00:17 IST,COMPLETED,delphix_admin,POLICY_APPLY,Apply policy "aaa" on target "Oracle dsource".
local,2017-10-13 14:00:17 IST,COMPLETED,delphix_admin,POLICY_APPLY,Apply policy "Default SnapSync" on target "Oracle dsource".
local,2017-10-13 14:00:17 IST,WAITING,delphix_admin,DB_SYNC,Run SnapSync for database "Oracle dsource".
local,2017-10-13 14:17:50 IST,COMPLETED,delphix_admin,USER_LOGOUT,Log out user "delphix_admin".
local,2017-10-13 15:26:24 IST,COMPLETED,delphix_admin,USER_LOGIN,Log in as user "delphix_admin" from IP "172.16.180.1".
EOF

script_stdout_is $expected_stdout, "get audit results compare";


script_runs(['../../bin/dx_get_audit.pl', '-d', 'local', '-format','csv','-st','2017-10-06 15:36:41','-state','FAILED'] ,  "get audit data with state");

my $expected_stdout = <<EOF;
#Appliance,StartTime,State,User or Policy,Type,Details
local,2017-10-09 09:37:54 IST,FAILED,default snapshot,DB_SYNC,Run SnapSync for database "tests".
local,2017-10-10 09:34:55 IST,FAILED,delphix_admin,DB_SYNC,Run SnapSync for database "tests".
local,2017-10-10 09:35:03 IST,FAILED,delphix_admin,SOURCE_STOP,Stop dataset "tests".
local,2017-10-10 09:35:33 IST,FAILED,delphix_admin,DB_DELETE,Delete dataset "tests".
local,2017-10-10 09:37:32 IST,FAILED,delphix_admin,DB_SYNC,Run SnapSync for database "si4rac".
local,2017-10-10 09:38:29 IST,FAILED,delphix_admin,DB_SYNC,Run SnapSync for database "si4rac".
local,2017-10-10 10:49:29 IST,FAILED,delphix_admin,SNAPSHOT_DELETE,Delete snapshot "\@2017-09-28T10:21:08.908Z".
local,2017-10-10 10:50:43 IST,FAILED,delphix_admin,SNAPSHOT_DELETE,Delete snapshot "\@2017-09-28T10:21:08.908Z".
local,2017-10-10 12:51:23 IST,FAILED,delphix_admin,SNAPSHOT_DELETE,Delete snapshot "\@2017-10-10T08:39:04.451Z".
local,2017-10-10 12:52:02 IST,FAILED,delphix_admin,SNAPSHOT_DELETE,Delete snapshot "\@2017-10-10T08:39:04.451Z".
local,2017-10-11 17:13:45 IST,FAILED,delphix_admin,DB_SYNC,Run SnapSync for database "racdba".
local,2017-10-11 17:16:12 IST,FAILED,delphix_admin,DB_SYNC,Run SnapSync for database "racdba".
local,2017-10-11 17:42:09 IST,FAILED,delphix_admin,DB_SYNC,Run SnapSync for database "racdba".
local,2017-10-12 16:38:02 IST,FAILED,default snapshot,DB_SYNC,Run SnapSync for database "si4rac".
EOF

script_stdout_is $expected_stdout, "get audit with state results compare";

#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
