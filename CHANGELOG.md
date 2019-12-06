## 2.4.4

### Added
- [fix for #100](https://github.com/delphix/dxtoolkit/issues/100) - Request for new dxtoolkit command that can do port validation

### Changed
- [fix for #105](https://github.com/delphix/dxtoolkit/issues/105) - dx_get_db_env: Get database name (the one in the instance) for SQL databases
- [fix for #106](https://github.com/delphix/dxtoolkit/issues/106) - fix for db_get_db_env -hostenv and -config does not work together

## 2.4.3

### Changed
- [fix for #110](https://github.com/delphix/dxtoolkit/issues/110) - Listener selection for provisioning database doesn't work with 5.2 and 5.3 engines
- [fix for #108](https://github.com/delphix/dxtoolkit/issues/108) - add an option for dx_provision_db to use script for masking
- [fix for #107](https://github.com/delphix/dxtoolkit/issues/107) - fix for dx_ctl_env - disable env fails

## 2.4.2

New, smaller dxtoolkit package with installed.
It's initial phase of revisiting [an old issue](https://github.com/delphix/dxtoolkit/issues/8).
The trick is based on one binary created with symlinks on Linux platorm and hard links on Windows platform.

Packages named with installer, requires the following steps:
- uncompress package
- change working directory to one with uncompressed package
- run ./install.sh or install.cmd command

### Changed
- [fix for #90](https://github.com/delphix/dxtoolkit/issues/90) - Allow dx_get_analytics -t all to ignore missing metrics. 2.4.2 will skip broken analytics unless parameter -stoponinvalid is specified
- [fix for #103](https://github.com/delphix/dxtoolkit/issues/103) - Policy files can be loaded in newer engines now
- [fix for #104](https://github.com/delphix/dxtoolkit/issues/104) - fix for vCDB and dx_get_db_env with -config


## 2.4.1

Configuration files with an encrypted passwords created before version 2.4.0 has to be regenerated due
to changes with encryption keys and adding a checksum to password encryption.
Configuration files encrypted using version 2.4.0+ should work without any changes.

### Added

- dx_get_engine_time script to display an engine time using a time zone defined in Delphix Engine
- [fix for #84](https://github.com/delphix/dxtoolkit/issues/84) - shared / unshare action added to dx_ctl_js_bookmarks
- [fix for #94](https://github.com/delphix/dxtoolkit/issues/94) - shared / unshare action added to dx_ctl_js_bookmarks
- [fix for #96](https://github.com/delphix/dxtoolkit/issues/96) - support for changing a host in an environment added
- [fix for #97](https://github.com/delphix/dxtoolkit/issues/97) - dx_ctl_users enhancement - adding a SSH key to user

### Changed
- better support for log in errors ( cookie clean up )
- dx_get_js_snapshots will skip a template bookmarks
- fix for disabled databases
- [fix for #93](https://github.com/delphix/dxtoolkit/issues/93) - fix for file upload in dx_ctl_engine_upgrade. TODO: apply needs to detect engine reboot
- [fix for #98](https://github.com/delphix/dxtoolkit/issues/98) - fix for session user check if user has a domain specified


## 2.4.0

Configuration files with an encrypted passwords has to be regenerated due
to changes with encryption keys and adding a checksum to password encryption.

### Added

- [fix for #66](https://github.com/delphix/dxtoolkit/issues/66) - dx_get_osversions and dx_ctl_engine_upgrade scripts added
- [fix for #72](https://github.com/delphix/dxtoolkit/issues/72) - possibility to add Windows cluster environment
- [fix for #73](https://github.com/delphix/dxtoolkit/issues/73) - filter by rdbms in dx_get_db_env

### Changed

- [fix for #63](https://github.com/delphix/dxtoolkit/issues/63)
- [fix for #65](https://github.com/delphix/dxtoolkit/issues/65)
- [fix for #76](https://github.com/delphix/dxtoolkit/issues/76)
- [fix for #77](https://github.com/delphix/dxtoolkit/issues/77)
- [fix for #79](https://github.com/delphix/dxtoolkit/issues/79)
- [fix for #80](https://github.com/delphix/dxtoolkit/issues/80) - branch selection improvement
- [fix for #88](https://github.com/delphix/dxtoolkit/issues/88)
- update a validated mode sync mode. log sync and backup path using dx_ctl_dsource
- fix for logsync support for non-MT and MT Oracle dSource
- fix for MS SQL hooks (PR-2)
- Encryption changes:
 - checksum is added to encrypted password
 - shared parameter added to dx_encrypt to enable encryption without host name for shared configs
- security fix - dxtoolkit is checking if cookie is set for user declared in configuration file
- dx_get_vdbthroughput fix plus documentation fix
- fix for PDB status with physical CDB

## 2.3.9.1

### Changed
- dx_ctl_users - fix for Self Service users support in 5.3

## 2.3.9

### Changed
- small bug fixes

## 2.3.9-rc2

### Added
- DB2 support added
- dx_ctl_dsource - action update added to allows change of backup_path and validated sync mode for an Oracle and Sybase
- dx_ctl_users - added possibility to set timeout for one or all users, option force added to remove a JS container ownership and delete user

### Changed
- dx_ctl_users - is running actions for users in same domain as user used for connection


## 2.3.9-rc1

### Added
- dx_get_js_snapshot - list a Self service bookmarks and timelines with corresponding parent snapshots

### Changed
- fix for #68 - dx_get_appliance utilization column was not displaying a percentage but raw value
- fix for #69 - rc should be 0 when engine name not found

## 2.3.8.1

### Changed
- dx_get_capacity fixed
- dx_get_instance fix for PDB dSource
- fixes for UTF8 support in names
- elimination of replicated dSource from backup metadata
- support for same user name in DOMAIN and SYSTEM namespace

## 2.3.7

### Changed
- changes in API version check to make it compatible with 5.3
- various fixes for 5.3 support
- fix for importing VDB templates

## 2.3.7-rc1

### Added
- dx_ctl_js_branch - create/delete/activate a Self Service branch
- dx_get_js_branch - list a Self Service branches
- dx_get_vdbthrogput - extract VDB throughout for last 24 hours
- dx_get_dsourcesize - show dSource sizes without compression
- support for user logging with @DOMAIN and @SYSTEM
- new filer oldthan added to dx_get_db_env/dx_refresh_db/dx_rewind_db to refresh databases older than date
- option -notime added to dx_get_snapshot to quickly list a list of snapshot without provision time
- dx_ctl_js_container has an action to enable and disable container

### Changed
- bugs fixed for held space and deleted objects in dx_get_capacity
- dx_ctl_dSource fixed with new API of 5.2.5


### Changed
- fix for file mapping API changes in 5.2

## 2.3.6

### Changed
- fix for file mapping API changes in 5.2


## 2.3.6-rc2

### Added
- dx_ctl_replication - "safe" option added to replication, replication job won't kick off if VDB was deleted since last replication

## 2.3.6-rc1

### Added
- dx_get/ctl_template - support to display template parameters and compare it with init files
- dx_ctl_dsource - creategroup and PDB documentation added
- dx_provision_db - mount point can be specified for Sybase VDB (5.2 required)
- dx_ctl_dsource - mount point for staging database for Sybase dSource (5.2 required) is supported
- dx_snapshot_db - uuid can be specified for MS SQL backup
- dx_snapshot_db - full backup and doublesync option added for an Oracle
- dx_get_users - engine name is displayed, sysadmin user is displayed with S in admin_priv column
- dx_ctl_users - password change, enable and disable added

### Changed
- bug fixes

## 2.3.5.2

### Changed
- dx_provision_vdb hook fix
- dx_get_hierarchy and dx_get_db_env fix for detached dSources


## 2.3.5

### Changed
- bugs fixes

## 2.3.5-rc2

### Added
- support for operation template in provision script
- dx_get_dbhooks add dx_ctl_dbhooks
- use LATEST_PROVISIONABLE_SNAPSHOT added for timestamp definition

### Changed
- old dx_ctl_hooks renamed into dx_ctl_op_template
- old dx_get_hooks renamed into dx_get_op_template
- Database level hooks operations moved from dx_xxx_hooks into dx_xxx_dbhooks

## 2.3.5-rc1

### Changed

* fix JS container operations with 5.2
* timestamp with timezone support moved into Toolkit_helpers to have a consistent support for timezones defined with offset only

## 2.3.4.1

### Changed

* fix for MS SQL snapshot in 5.2

## 2.3.4

### Added

* verification with new Version 5.2
* support for SI vCDB for provisoning and metadata backup
* snapshot size and object dependency added to dx_get_snapshot
* dx_get_capacity is displaying held objects (only in version >=5.2)
* expiration date can be set for new bookmarks using dx_ctl_js_bookmarks

### Changed

* help fixes
* Various bug fixes
* dx_get_capacity is able to use new cached values (only in version >=5.2)
* reversed order added to printhierarchy
* multi NIC output in Analytics
* dx_get_user can display last loggin time and account status


# Change Log

## 2.3.3.1

### Changed

* timezone bug fix

## 2.3.3

### Changed

* Bug fix

## 2.3.3-rc3

### Added

* v2p enhancements
* read password from command line
* location of config file can be set by parameter or environment variable

### Changed

* help fixes
* Various bug fixes

## 2.3.3-rc2

### Added

* adding error output for dx_get_jobs

### Changed

* fix for caching
* Various bug fixes

## 2.3.3-rc1

### Added

* dx_syslog
* dx_ctl_users allow to set current role to None

### Changed

* fix for users support
* fix for JS containers
* fix for timezones defined with offset
* debug enhancements
* Various bug fixes


## 2.3.2.1

### Changed

* fix to skip display of AUX_CDB containers

## 2.3.2

### added

* vCPU and vMem information into dx_get_appliance
* Parent time in dx_get_db_env will display a real provisioning time, SCN/LSN or parent snapshot time - depend what was used to deploy
* added create and remove JetStream container actions in dx_ctl_js_container
* new script dx_ctl_js_template to create and remove JetStream template
* added functionalty to restore container from template timeline
* new script dx_get_capacity_history to extract historical capacity

### Changed

* fix for RAC to nonRAC provisioning
* missing cdb parameter in help
* Various bug fixes

## 2.3.1

### Added

* Round time trip extract for TCP analytics for 5.1.X engines
* dx_remove_env - script to remove environment
* dx_get_hierarchy - script to display hierarchy - first release
* dx_get_autostart - script to display autostart flag
* dx_set_autostart - script to set autostart flag
* Autostart flag added to provisioning script
* Adding a support for adding database, vfiles manually using dx_ctl_env

### Changed
* Various bug fixes

## 2.3.0

No changes between this release. Check list below

## 2.3.0-rc3

### Added

* Masking job added to provision vdb script
* Enhancement of dx_ctl_env by:
  * adding environemnt users
  * manually adding repository (Oracle Home)
  * manually adding database with jdbc string
* Masking job support on virtualization engine
  * dx_get_maskingjob
  * dx_ctl_maskingjob
* instance name added as filter

### Changed
* Various bug fixes


## 2.3.0-rc1

### Added
* Display masked status plus job name - dx_get_db_env
* NFS IOPS added to dx_get_analytic output
* New timestamp format supported for start / end date.
Ex. "-5day" to specify a time now minus 5 days and "-10min" to specify a time now minus 10 min.
* Support for Oracle PDB - provision and metadata backup
* Snapshot and retention policy added to dx_provision_vdb
* Extract system configuration - dx_get_config
* Kick off replication job - dx_ctl_replication
* List replication profiles and last replication status in dx_get_replication
* Display a OS version in dx_get_env

### Changed
* Impoved output for config of databases in dx_get_db_env
* Support for Delphix Engine version 5.1.X
* Various bug fixes
