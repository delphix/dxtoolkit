pioro-mbp:bin mprzepiorowski$ perl dx_ctl_dsource.pl -d local -action create -group "Sources" -creategroup -dsourcename "tpcc"  -type mssql -sourcename "tpcc" -sourceinst "KVMTARGET2012" -sourceenv "WIN2012N1STD" -source_os_user "DELPHIX\delphix_admin" -dbuser delphixdb -password delphixdb -logsync yes -stageinst "KVMTARGET2012" -stageenv "WIN2012N1STD" -stage_os_user "DELPHIX\delphix_admin" -validatedsync TRANSACTION_LOG -backup_dir ""
Waiting for all actions to complete. Parent action is ACTION-273180
Action completed with success
pioro-mbp:bin mprzepiorowski$
pioro-mbp:bin mprzepiorowski$
pioro-mbp:bin mprzepiorowski$ perl dx_ctl_dsource.pl -d local -action create -group "Sources" -creategroup -dsourcename "simple"  -type mssql -sourcename "simple" -sourceinst "KVMTARGET2012" -sourceenv "WIN2012N1STD" -source_os_user "DELPHIX\delphix_admin" -dbuser delphixdb -password delphixdb -logsync no -stageinst "KVMTARGET2012" -stageenv "WIN2012N1STD" -stage_os_user "DELPHIX\delphix_admin" -delphixmanaged yes
Waiting for all actions to complete. Parent action is ACTION-273200
Action completed with success
