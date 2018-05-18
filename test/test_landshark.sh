DE=Landshark51
VDB=autotest


check_code() {
	RC=$1
	MSG=$2
	if [ $RC -ne 0 ]; then
		echo $2;
		exit; 
	fi
}

if [ -z $1 ]; then
	echo "Type of database is required $1";
fi

if [ $1 == 'oracle' ]; then
	# oracle set
	DSOURCE="test121"
	SOURCEDB=test121
	SOURCEENV=LINUXSOURCE
	SOURCEINST="/u01/app/oracle/12.1.0.2/db1"
	VDBTYPE=oracle
	ENVINST="/u01/app/oracle/12.1.0.2/db1"
	TARGETENV=LINUXTARGET	
elif [ $1 == 'sybase' ]; then
	# sybase set
	DSOURCE="pubs3"
	VDBTYPE=sybase
	ENVINST=LINUXTARGET
	TARGETENV=LINUXTARGET
	SOURCEDB=pubs3
	SOURCEENV=LINUXSOURCE
	SOURCEINST=LINUXSOURCE
elif [ $1 == 'mssql' ]; then
	# mssql set
	DSOURCE="AdventureWorksLT2008R2"
	SOURCEDB="AdventureWorksLT2008R2"
	VDBTYPE="mssql"
	ENVINST="MSSQLSERVER"
	TARGETENV=WINDOWSTARGET
	SOURCEINST="MSSQLSERVER"
	SOURCEENV=WINDOWSSOURCE
else 
	echo "Unknown database";
	exit;
fi


# webapp
#DSOURCE="Employee Web Application"
#VDBTYPE=appdata
#ENVINST=LINUXTARGET
#TARGETENV=LINUXTARGET


#perl dx_ctl_env.pl -d ${DE} -action refresh -name ${TARGETENV} -parallel 2
#perl dx_ctl_env.pl -d ${DE} -action refresh -name ${SOURCEENV} -parallel 2

perl dx_get_db_env.pl -d ${DE} -name ${VDB}


if [[ $? -eq 0 ]]; then
	perl dx_remove_db.pl -d ${DE} -name ${VDB} -skip
		
	if [[ $? -ne 0 ]]; then
		echo "Can't remove VDB - ${VDB}"
		echo "try with force - ${VDB}"
	  perl dx_remove_db.pl -d ${DE} -name ${VDB} -skip -force
	  RC=$?
	  check_code $RC "Can't delete database with force"
		
	fi
	
fi

# check if there is any VDB based on dSource

perl dx_get_db_env.pl -d ${DE} -dsource "${DSOURCE}"

if [[ $? -eq 0 ]]; then
	echo "Stop. There are VDB based on test dSource";
	exit 1;
fi

perl dx_get_db_env.pl -d ${DE} -name "${DSOURCE}"

if [[ $? -eq 0 ]]; then 

	perl dx_remove_db.pl -d ${DE} -name "${DSOURCE}" -skip
	if [[ $? -ne 0 ]]; then
		echo "Can't remove dSource - ${DSOURCE}"
		echo "try with force - ${DSOURCE}"
	  perl dx_remove_db.pl -d ${DE} -name "${DSOURCE}" -skip -force
		RC=$?
		check_code $RC "Stop. Can't delete dSource with force";	
	fi
	
fi


if [ ${VDBTYPE} == "oracle" ]; then
	#creade dSource

	# create dSource
	perl dx_ctl_dsource.pl -d ${DE} -type oracle -sourcename ${SOURCEDB} -sourceinst "${SOURCEINST}" -sourceenv "${SOURCEENV}" -source_os_user oracle -dbuser delphixdb -password delphixdb -group Sources -dsourcename "${DSOURCE}"  -action create
	RC=$?
	check_code $RC "Can't create dSource";	

	# unlink dSource
	perl dx_ctl_dsource.pl -d ${DE} -type oracle -group Sources -dsourcename "${DSOURCE}"  -action detach
	RC=$?
	check_code $RC "Can't detach dSource";	

	# #link dSource
	perl dx_ctl_dsource.pl -d ${DE} -type oracle -sourcename ${SOURCEDB} -source_os_user delphix -dbuser delphixdb -password delphixdb -group Sources -dsourcename "${DSOURCE}"  -action attach
	RC=$?
	check_code $RC "Can't attach dSource";		

	#check snapshot on dSource
	perl dx_snapshot_db.pl -d ${DE} -name "${DSOURCE}"
	RC=$?
	check_code $RC "Can't snapshot dSource";	

	perl dx_get_template.pl -d ${DE} -name "auto"

	if [[ $? -eq 0 ]]; then
		perl dx_ctl_template.pl -d ${DE} -update -filename test.template	
	else
		perl dx_ctl_template.pl -d ${DE} -import -filename test.template
	fi

	#perl dx_provision_vdb.pl -d ${DE} -group Analytics -sourcename "${DSOURCE}" -targetname ${VDB} -dbname ${VDB} -environment ${TARGETENV} -type oracle -envinst ${ENVINST} -template new 
	perl dx_provision_vdb.pl -d ${DE} -group Analytics -sourcename "${DSOURCE}" -targetname ${VDB} -dbname ${VDB} -environment ${TARGETENV} -type oracle -envinst ${ENVINST} -envUser oracle

	# provision vPDB
    #    perl dx_provision_vdb.pl -d ${DE} -group Analytics -sourcename PDB1 -targetname vPDBtest -type oracle -dbname vPDBtest -envUser oracle -environment LINUXTARGET -envinst "/u01/app/oracle/12.1.0.2/db1"  -mntpoint "/mnt/provision" -cdb targetcon

	# provision SI from RAC
    #    perl dx_provision_vdb.pl -d Landshark51 -group Analytics -sourcename racdba -targetname si4rac -type oracle -dbname si4rac -envUser oracle -uniqname si4rac -environment LINUXTARGET -envinst "/u01/app/oracle/12.1.0.2/db1"  -mntpoint "/mnt/provision" -autostart yes

fi

if [ ${VDBTYPE} == "mssql" ]; then
	
	# create dSource
	perl dx_ctl_dsource.pl -d ${DE} -type mssql  -sourcename ${SOURCEDB} -sourceinst "${SOURCEINST}" -sourceenv "${SOURCEENV}"  -group Sources -dsourcename "${DSOURCE}"  -action create -source_os_user "DELPHIX\delphix_admin" -dbuser aw -password delphixdb -stage_os_user "DELPHIX\delphix_admin" -stageinst MSSQLSERVER -stageenv WINDOWSTARGET -backup_dir "\\\\172.16.180.133\\backups"
	RC=$?
	check_code $RC "Can't create dSource";	

	# unlink dSource
	perl dx_ctl_dsource.pl -d ${DE} -type mssql -group Sources -dsourcename "${DSOURCE}"  -action detach
	RC=$?
	check_code $RC "Can't detach dSource";	
	
	# #link dSource
	perl dx_ctl_dsource.pl -d ${DE} -type mssql -sourcename ${SOURCEDB} -sourceinst "${SOURCEINST}" -sourceenv "${SOURCEENV}" -source_os_user "DELPHIX\delphix_admin" -dbuser aw -password delphixdb -group Sources -dsourcename "${DSOURCE}" -stage_os_user "DELPHIX\delphix_admin" -stageinst MSSQLSERVER -stageenv WINDOWSTARGET -backup_dir "\\\\172.16.180.133\\backups" -action attach 
	RC=$?
	check_code $RC "Can't attach dSource";		
	
	#check snapshot on dSource with a new backup
	perl dx_snapshot_db.pl -d ${DE} -name "${DSOURCE}"
	RC=$?
	check_code $RC "Can't snapshot dSource with a new backup";	
	
	
	
	perl dx_provision_vdb.pl -d ${DE} -group Analytics -sourcename "${DSOURCE}" -targetname ${VDB} -dbname ${VDB} -environment ${TARGETENV} -type mssql -envinst ${ENVINST}
fi

if [ ${VDBTYPE} == "sybase" ]; then
	

	# create dSource
	perl dx_ctl_dsource.pl -d ${DE} -type sybase -sourcename ${SOURCEDB} -sourceinst "${SOURCEINST}" -sourceenv "${SOURCEENV}" -source_os_user delphix -dbuser sa -password delphix -group Sources -dsourcename "${DSOURCE}" -stage_os_user delphix -stageinst LINUXTARGET -stageenv LINUXTARGET -backup_dir "/u02/sybase_back" -action create 
	RC=$?
	check_code $RC "Can't create dSource";	

	# unlink dSource
	#perl dx_ctl_dsource.pl -d ${DE} -type sybase -group Sources -dsourcename "${DSOURCE}"  -action detach
	#RC=$?
	#check_code $RC "Can't detach dSource";	
	
	# #link dSource
	#perl dx_ctl_dsource.pl -d ${DE} -type sybase -sourcename ${SOURCEDB} -sourceinst "${SOURCEINST}" -sourceenv "${SOURCEENV}" -source_os_user delphix -dbuser sa -password delphixdb -group Sources -dsourcename "${DSOURCE}" -stage_os_user delphix -stageinst LINUXTARGET -stageenv LINUXTARGET -backup_dir "/u02/sybase_back" -action attach 
	#RC=$?
	#check_code $RC "Can't attach dSource";		
	
	#check snapshot on dSource with a new backup
	perl dx_snapshot_db.pl -d ${DE} -name "${DSOURCE}"
	RC=$?
	check_code $RC "Can't snapshot dSource with a new backup";	

	perl dx_provision_vdb.pl -d ${DE} -group Analytics -sourcename "${DSOURCE}" -targetname ${VDB} -dbname ${VDB} -environment ${TARGETENV} -type sybase -envinst ${ENVINST}
fi

RC=$?
check_code $RC "Provision failure";



STATUS=''
while [ "${STATUS}" != "RUNNING" ] ; do
	        STATUS=`perl dx_get_db_env.pl -d ${DE} -name ${VDB} -format csv -nohead | awk -F',' '{print $9};'`
	        echo $STATUS
	        sleep 10
done

sleep 60
perl dx_snapshot_db.pl -d ${DE} -name ${VDB}
sleep 60
perl dx_snapshot_db.pl -d ${DE} -name ${VDB}
sleep 60
perl dx_snapshot_db.pl -d ${DE} -name ${VDB}
sleep 5


perl dx_ctl_bookmarks.pl -d ${DE} -name boomarktest -action delete
perl dx_ctl_bookmarks.pl -d ${DE} -dbname ${VDB} -name boomarktest -timestamp latest -action create
#

#start/stop test

STATUS=''

perl dx_ctl_db.pl -d ${DE} -action stop -name ${VDB}

while [ "${STATUS}" != "INACTIVE" ] ; do
	STATUS=`perl dx_get_db_env.pl -d ${DE} -name ${VDB} -format csv -nohead | awk -F',' '{print $9};'`
	echo $STATUS
	sleep 10
done


perl dx_ctl_db.pl -d ${DE} -action start -name ${VDB}

while [ "${STATUS}" != "RUNNING" ] ; do
        STATUS=`perl dx_get_db_env.pl -d ${DE} -name ${VDB} -format csv -nohead | awk -F',' '{print $9};'`
	echo $STATUS
	sleep 10
done


perl dx_ctl_db.pl -d ${DE} -action disable -name ${VDB}

while [ "${STATUS}" != "disabled" ] ; do
	        STATUS=`perl dx_get_db_env.pl -d ${DE} -name ${VDB} -format csv -nohead | awk -F',' '{print $10};'`
		echo $STATUS
		sleep 10
done

perl dx_ctl_db.pl -d ${DE} -action enable -name ${VDB}

while [ "${STATUS}" != "disabled" ] ; do
	        STATUS=`perl dx_get_db_env.pl -d ${DE} -name ${VDB} -format csv -nohead | awk -F',' '{print $10};'`
		echo $STATUS
		sleep 10
done

while [ "${STATUS}" != "RUNNING" ] ; do
	        STATUS=`perl dx_get_db_env.pl -d ${DE} -name ${VDB} -format csv -nohead | awk -F',' '{print $9};'`
	        echo $STATUS
	        sleep 10
done


echo REFRESH TEST
SNAPSHOT_TIME=`perl dx_get_snapshots.pl -d ${DE} -name "${DSOURCE}" -format csv -nohead | tail -1 | awk -F',' '{print $5};'`
SNAPSHOT=${SNAPSHOT_TIME:0:16}
echo $SNAPSHOT
perl dx_refresh_db.pl -d ${DE} -name ${VDB} -timestamp "$SNAPSHOT"

echo Bookmark rewind - to prerefresh state
perl dx_rewind_db.pl -d ${DE} -name ${VDB} -timestamp boomarktest

#sleep 65
#perl dx_snapshot_db.pl -d ${DE} -name ${VDB}
#sleep 65
#perl dx_snapshot_db.pl -d ${DE} -name ${VDB}
#sleep 65
#perl dx_snapshot_db.pl -d ${DE} -name ${VDB}
#sleep 5


echo Rewind to a last snapshot
perl dx_get_snapshots.pl -d ${DE} -name ${VDB}

SNAPSHOT_TIME=`perl dx_get_snapshots.pl -d ${DE} -name ${VDB} -format csv -nohead | head -1 | awk -F',' '{print $5};'` 
SNAPSHOT=${SNAPSHOT_TIME:0:16}

echo $SNAPSHOT

perl dx_rewind_db.pl -d ${DE} -name ${VDB} -timestamp "$SNAPSHOT"



echo refresh on snapshotname

SNAPSHOT_TIME=`perl dx_get_snapshots.pl -d ${DE} -name "${DSOURCE}" -format csv -nohead | tail -1 | awk -F',' '{print $4};'`

echo $SNAPSHOT_TIME

perl dx_refresh_db.pl -d ${DE} -name ${VDB} -timestamp "$SNAPSHOT_TIME"
