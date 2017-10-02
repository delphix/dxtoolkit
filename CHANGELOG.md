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
