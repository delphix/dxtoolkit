#!/bin/bash


##############
#   Author: Jatinder Luthra
#   Pre-req: dxtoolkit
##############

engine_name=$1
backup_dir_name=$2

## Validating both arguments are passed ##

if [ $# -lt 2 ]; then
    echo ""
	echo "!!!! ERROR:: Pass both arguments, Delphix Engine Name and Backup Directory Name !!!!!"
	echo""
	echo "Example: ./import_metadata.sh <engineName> <backupDirectoryName>"
	echo ""
    exit 1
fi

###### change below variable values as per your environment #######

dxtoolkit_dir="/Users/jatinder.luthra/Desktop/Customers/BOA/dxtoolkit2"

backup_path="/Users/jatinder.luthra/Desktop/Scripts/DR_Scripts/DR_backups"

env_config="/Users/jatinder.luthra/Desktop/Scripts/DR_Scripts/env_config.csv"

db_config="/Users/jatinder.luthra/Desktop/Scripts/DR_Scripts/db_config.csv"

# (Optional) Only required for 12c Multitenant VDBs using physical containers on target server

vdb_config="/Users/jatinder.luthra/Desktop/Scripts/DR_Scripts/vdb_config.csv"

####

default_os_pwd="welcome123"

default_db_pwd="welcome123"

####### no changes required below this point ##########

import_date=$(date '+%m%d%Y-%H%M%S')

backup_dir="${backup_path}/${backup_dir_name}"

echo ""
echo "######################################################################################################################"
echo "############## Metadata Restore of engine, ${engine_name} started from directory, ${backup_dir} on ${import_date} ####"
echo "######################################################################################################################"
echo ""

##### Checking Engine Status #######

echo ""
echo "######### Checking Delphix Engine Status #########"

chk_status=`${dxtoolkit_dir}/dx_get_appliance -d ${engine_name}`
echo $chk_status > ${backup_dir}/status.txt

if [[ ${chk_status} =~ "Can't check session status" || ${chk_status} =~ "Can't find" ]]; then
echo "Either Delphix Engine, ${engine_name} is not up OR ${engine_name} entry not present in conf file"
exit 0
else
echo "Delphix Engine ${engine_name} is acessible. Proceeding with restore"
fi

###### Creating Users and Profiles #######

echo ""
echo "#######################################################################"
echo "######### Restoring Delphix Engine Users and Profiles Started #########"
echo "#######################################################################"


users_file=users.csv
profiles_file=profile.csv
users_dir=${backup_dir}/users

${dxtoolkit_dir}/dx_ctl_users -d ${engine_name} -file ${users_dir}/${users_file} -profile ${users_dir}/${profiles_file}

echo ""
echo "######### Restoring Delphix Engine Users and Profiles Finished #########"

#### switch to dxtoolkit directory ####

cd ${dxtoolkit_dir}

######  Creating environments  #######

echo ""
echo "##################################################"
echo "######### Restoring Environments Started #########"
echo "##################################################"


env_dir=${backup_dir}/environments
env_res_file=${env_dir}/backup_env.sh

##### Update the correct passwords for OS users before restoring  environments#####
##### Validate env_config.csv file resides in backupDir/environments directory #####

INPUT=${env_config}
OLDIFS=$IFS
IFS=','
[ ! -f $INPUT ] && { echo "$INPUT file not found"; exit 99; }
while read envName osUser pwd
do

sed -r -i "/(-envname \"$envName\") .*(-username \"$osUser\")/s/-password $default_os_pwd/-password $pwd/g"  ${env_res_file}

echo "################"
done < $INPUT
IFS=$OLDIFS

### Restoring environments from final file ###
${env_res_file}

echo ""
echo "###################################################"
echo "######### Restoring Environments Finished #########"
echo "###################################################"


######  Creating dSources  #######

echo ""
echo "##################################################"
echo "########## Restoring dSources Started #############"
echo "##################################################"


db_dir=${backup_dir}/db_objects
db_res_file=${db_dir}/backup_metadata_dsource.sh

##### Update the correct passwords for DB users before restoring database #####
##### Validate db_config.csv file resides in backupDir/db_objects directory #####

INPUT=${db_config}
OLDIFS=$IFS
IFS=','
[ ! -f $INPUT ] && { echo "$INPUT file not found"; exit 99; }
while read dSource envName dbUser pwd
do

sed -r -i "/(-dsourcename \"$dSource\") .*(-sourceenv \"$envName\") .*(-dbuser $dbUser)/s/-password $default_db_pwd/-password $pwd/g" ${db_res_file}

sed -r -i "/(-dsourcename \"$dSource\") .*(-sourceenv \"$envName\") .*(-cdbuser \"$dbUser\")/s/-cdbpass $default_db_pwd/-cdbpass $pwd/g" ${db_res_file}

done < $INPUT
IFS=$OLDIFS

### Restoring dSource from final file ###
${db_res_file}

echo ""
echo "##################################################"
echo "########### Restoring dSources Finished ##########"
echo "##################################################"

######  Restore VDB Config Templates  #######

echo ""
echo "###############################################################"
echo "########## Restoring VDB Config Templates Started #############"
echo "###############################################################"


template_dir=${backup_dir}/config_templates

FILES=${template_dir}/*
for template in $FILES
do
  echo "## Restoring VDB Config template, ${template} ##"

  ${dxtoolkit_dir}/dx_ctl_template -d ${engine_name} -import -filename "${template}"
done

echo ""
echo "#################################################################"
echo "########## Restoring VDB Config Templates Finished #############"
echo "#################################################################"

######  Restore Hook Templates  #######

echo ""
echo "###############################################################"
echo "########## Restoring Hook Templates Started #############"
echo "###############################################################"


hooks_dir=${backup_dir}/hook_templates

${dxtoolkit_dir}/dx_ctl_op_template -d ${engine_name} -importHook -indir ${hooks_dir}

echo ""
echo "#################################################################"
echo "########## Restoring Hook Templates Finished #############"
echo "#################################################################"

######  Creating VDBs  #######

echo ""
echo "##################################################"
echo "########## Restoring VDBs Started #############"
echo "##################################################"


db_dir=${backup_dir}/db_objects
vdb_res_file=${db_dir}/backup_metadata_vdb.sh


### For VDBs using physical container###

INPUT=${vdb_config}

### If file exists append the physical container credentials with the VDB creation command ###

if test -f "$INPUT"; then
	echo "VDB Config File, ${vdb_config} exists"
	OLDIFS=$IFS
	IFS=','
	[ ! -f $INPUT ] && { echo "$INPUT file not found"; exit 99; }
	while read cdbName envName cdbuser cdbpwd
	do

	sed -r -i "/(-environment \"$envName\") .*(-cdb $cdbName)/s/$/ -cdbuser \"$cdbuser\" -cdbpass \"$cdbpwd\"/" ${vdb_res_file}

	done < $INPUT
	IFS=$OLDIFS
  else
  	echo "VDB Config File, ${vdb_config} doesn't exists"
fi

### Restoring VDBs from file ###
${vdb_res_file}

echo ""
echo "##################################################"
echo "########### Restoring VDBs Finished ##########"
echo "##################################################"

######  Restore Policies  #######

echo ""
echo "###################################################"
echo "########## Restoring Policies Started #############"
echo "###################################################"


policy_dir=${backup_dir}/policies
policy_file=policy.mapping

## Create custom policies

${dxtoolkit_dir}/dx_ctl_policy -d ${engine_name} -import -indir ${policy_dir}

## Update default policies

${dxtoolkit_dir}/dx_ctl_policy -d ${engine_name} -update -indir ${policy_dir}

## Apply policies on objects

${dxtoolkit_dir}/dx_ctl_policy -d ${engine_name} -mapping ${policy_dir}/${policy_file}

echo ""
echo "####################################################"
echo "########## Restoring Policies Finished #############"
echo "#####################################################"

######  Restore Self Service Objects  #######

echo ""
echo "###################################################"
echo "########## Restoring Self Service Started #############"
echo "###################################################"


ss_dir=${backup_dir}/ss_objects

temp_res_file=${ss_dir}/backup_selfservice_templates.sh
cont_res_file=${ss_dir}/backup_selfservice_containers.sh

####### Restoring Templates #####

${temp_res_file}

####### Restoring Containers #####

${cont_res_file}

echo ""
echo "####################################################"
echo "########## Restoring Self Service Finished #############"
echo "#####################################################"

echo ""
echo "#####################################################################################################################################"
echo "######### Metadata Restore of engine, ${engine_name} finished from directory, ${backup_dir} on $(date '+%m%d%Y-%H%M%S')  ############"
echo "##   Check backup files under directory, ${backup_dir}    ##"
echo "#####################################################################################################################################"
echo ""
exit 0
