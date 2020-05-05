#!/bin/bash

######################################################################################
##### Backup of Delphix Engine metadata include only metadata related to objects #####
###### and not Delphix Engine configuration itself. Dxtoolkit can backup users, ######
###### polices, templates and operation templates and can generate a scripts to ######
############ recreate other objects like environments, dSources and VDB’s ############
######################################################################################
############ Passwords are not exported and are set to default values like ###########
##################        xxxxxxxx for Delphix users         #########################
##################       xxxxxxxx for Environment users      #########################
##################        xxxxxxxx for Database users      #########################
##################  xxxxxxxx for OracleMT root DB user (c##user) ####################
######################################################################################

##############
#   Author: Jatinder Luthra
#   Pre-req: dxtoolkit
##############

engine_name=$1

## Validating argument is passed ##

if [[ $# -eq 0 ]]; then
echo ""
echo "!!!! Error:: Pass Delphix Engine Name as in dxtools.conf file !!!!"
echo ""
exit 0
fi

###### change below variable values as per your environment #######

dxtoolkit_dir="/Users/jatinder.luthra/Desktop/Customers/BOA/dxtoolkit2"

backup_path="/Users/jatinder.luthra/Desktop/Scripts/DR_Scripts/DR_backups"

default_os_pwd="welcome123"

default_db_pwd="welcome123"

####### no changes required below this point ##########

backup_date=$(date '+%m%d%Y-%H%M%S')

backup_dir="${backup_path}/${engine_name}-metadata-backup-${backup_date}"


echo ""
echo "####################################################################################################"
echo "############## Metadata Backup of engine, ${engine_name} started under directory, ${backup_dir} ####"
echo "####################################################################################################"
echo ""

###### create backup directory ######

echo "###### Creating backup directory, ${backup_dir} ######"

mkdir ${backup_dir}

##### Checking Engine Status #######

echo ""
echo "######### Checking Delphix Engine Status #########"

chk_status=`${dxtoolkit_dir}/dx_get_appliance -d ${engine_name}`
echo $chk_status > ${backup_dir}/status.txt

if [[ ${chk_status} =~ "Can't check session status" || ${chk_status} =~ "Can't find" ]]; then
echo "Either Delphix Engine, ${engine_name} is not up OR ${engine_name} entry not present in conf file"
exit 0
else
echo "Delphix Engine ${engine_name} is acessible. Proceeding with metadata backup"
fi

###### Backup Engine Configuration #######

echo ""
echo "######### Backing up Delphix Engine Configuration #########"

engine_sys_name=${engine_name}_sys
sys_config_dir=${backup_dir}/sys_config
mkdir ${sys_config_dir}
sys_config_file=sys_config.csv
appliance_info=appliance_info.csv
latency_info=network_latency.csv
throughput_info=network_throughput.csv
hierarchy_info=hierarchy_info.txt

echo "Backup exported into ${sys_config_dir}"

${dxtoolkit_dir}/dx_get_config -d ${engine_sys_name} -format csv > ${sys_config_dir}/${sys_config_file}

${dxtoolkit_dir}/dx_get_appliance -d ${engine_name} -details -format csv > ${sys_config_dir}/${appliance_info}

${dxtoolkit_dir}/dx_get_network_tests -d ${engine_name} -type latency -format csv > ${sys_config_dir}/${latency_info}

${dxtoolkit_dir}/dx_get_network_tests -d ${engine_name} -type throughput -format csv > ${sys_config_dir}/${throughput_info}

###### Backup Users and Profiles #######

echo ""
echo "######### Backing up Delphix Engine Users and Profiles #########"

users_file=users.csv
profiles_file=profile.csv
users_dir=${backup_dir}/users
mkdir ${users_dir}

echo "Backup exported into ${users_dir}"

${dxtoolkit_dir}/dx_get_users -d ${engine_name} -export ${users_dir}/${users_file} -profile ${users_dir}/${profiles_file}

###### Backup Engine policies  #######

echo ""
echo "######### Backing up Delphix Engine Policies #########"

policy_file=policy.mapping
policy_dir=${backup_dir}/policies
mkdir ${policy_dir}

${dxtoolkit_dir}/dx_get_policy -d ${engine_name} -export -outdir ${policy_dir} -mapping ${policy_dir}/${policy_file}

 ###### Backup  Config templates  ######

echo ""
echo "######### Backing up Config Templates #########"


template_dir=${backup_dir}/config_templates
mkdir ${template_dir}

${dxtoolkit_dir}/dx_get_template -d ${engine_name} -export -outdir ${template_dir}

 ###### Backup hooks  ######

echo ""
echo "######### Backing up Operation Templates (Hooks) #########"


hooks_dir=${backup_dir}/hook_templates
mkdir ${hooks_dir}

${dxtoolkit_dir}/dx_get_op_template -d ${engine_name} -exportHook -outdir ${hooks_dir}

###### Generate environment creation scripts ######

echo ""
echo "######### Backing up environments metadata #########"

env_dir=${backup_dir}/environments
mkdir ${env_dir}

${dxtoolkit_dir}/dx_get_env -d ${engine_name} -backup ${env_dir}

#### add executable at start of each line #####
#### Update the correct passwords for OS users ####

sed "s/^/.\//; s/xxxxxxxx/${default_os_pwd}/g" ${env_dir}/backup_env.txt > ${env_dir}/backup_env.sh

###### Generate dSource and VDB creation scripts ######

echo ""
echo "######### Backing up dSource and VDB metadata #########"

db_dir=${backup_dir}/db_objects
mkdir ${db_dir}

${dxtoolkit_dir}/dx_get_db_env -d ${engine_name} -backup ${db_dir}

#### add executable at start of each line #####

sed "s/^/.\//; s/xxxxxxxx/${default_db_pwd}/g" ${db_dir}/backup_metadata_dsource.txt > ${db_dir}/backup_metadata_dsource.sh

sed "s/^/.\//" ${db_dir}/backup_metadata_vdb.txt > ${db_dir}/backup_metadata_vdb.sh

echo ""
echo "######### Backing up Self Service metadata #########"

ss_dir=${backup_dir}/ss_objects
mkdir ${ss_dir}

echo "Backup exported into ${ss_dir}"

${dxtoolkit_dir}/dx_get_js_templates -d ${engine_name} -backup ${ss_dir}

${dxtoolkit_dir}/dx_get_js_containers -d ${engine_name} -backup ${ss_dir}

#### add executable at start of each line #####

sed "s/^/.\//" ${ss_dir}/backup_selfservice_templates.txt > ${ss_dir}/backup_selfservice_templates.sh

sed "s/^/.\//" ${ss_dir}/backup_selfservice_containers.txt > ${ss_dir}/backup_selfservice_containers.sh

### Adding execute permissions to backup directory ###

chmod -R 770 ${backup_dir}


echo ""
echo "####################################################################################################"
echo "############## Metadata Backup of engine, ${engine_name} finished under directory       ############"
echo "##   Check backup files under directory, ${backup_dir}    ##"
echo "####################################################################################################"
echo ""
exit 0
