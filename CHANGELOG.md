# Change Log

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
