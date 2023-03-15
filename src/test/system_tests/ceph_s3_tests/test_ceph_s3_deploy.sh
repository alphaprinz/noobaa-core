#!/bin/bash

export PS4='\e[36m+ ${FUNCNAME:-main}\e[0m@\e[32m${BASH_SOURCE}:\e[35m${LINENO} \e[0m'

if [ -z ${1} ]
then
    NOOBAA_DIR="noobaa-core"
else 
    NOOBAA_DIR=${1}
fi

CEPH_S3_TESTS_PATH="src/test/system_tests/ceph_s3_tests"
mkdir -p /${NOOBAA_DIR}/${CEPH_S3_TESTS_PATH}
cd /${NOOBAA_DIR}/${CEPH_S3_TESTS_PATH}

DIRECTORY="s3-tests"
CEPH_LINK="https://github.com/ceph/s3-tests.git"
# using a fixed version (commit) of ceph tests to avoid sudden changes. 
# we should retest and update the version once in a while
CEPH_TESTS_VERSION=114397c358c7e6b30e0ff2f5dd54607dad1ae8ce
if [ ! -d $DIRECTORY ]; then
    echo "Downloading Ceph S3 Tests..."
    git clone $CEPH_LINK
    cd ${DIRECTORY}
    git checkout ${CEPH_TESTS_VERSION}
    echo "Finished Downloading Ceph S3 Tests"

    echo "Manually fixing s3select tests"
    sed -i '14 i from . import get_new_bucket_name' ./s3tests_boto3/functional/test_s3select.py
    sed -i 's/bucket_name = \"test\"/bucket_name = get_new_bucket_name()/g' ./s3tests_boto3/functional/test_s3select.py

fi

commit_epoch=$(git show -s --format=%ci ${CEPH_TESTS_VERSION} | awk '{print $1}')
commit_date=$(date -d ${commit_epoch} +%s)
current_date=$(date +%s)

max_days="180"
if [ $((current_date-commit_date)) -gt $((3600*24*${max_days})) ]
then
    echo "ceph tests were not updated for ${max_days} days, Exiting"
    exit 1
fi
