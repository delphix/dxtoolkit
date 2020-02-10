#!/bin/bash


LIST_OF_SCRIPTS=(
    test_action
    test_capacity_51
    test_database_list
    test_dsource_mssql_1
    test_dsource_mssql_52_1
    test_env
    test_event
    test_fault
    test_hierarchy
    test_jobs
    test_provision_vcdb
    test_snapshot
    test_snapshot_size
    test_user
    test_vdb
)

CURRENT_DIR=`pwd`

FINALRC=0

for i in "${LIST_OF_SCRIPTS[@]}"
do
  echo $i
  cd $i
  perl *.t
  RC=$?
  FINALRC=$((FINALRC + RC))
  cd ..
done

echo $FINALRC
exit $FINALRC