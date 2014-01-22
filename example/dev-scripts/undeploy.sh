#!/bin/bash

source ./set-env.sh

echo "Undeploying ... "
pushd ../
mvn os:undeploy -Dgroups=$GROUP -Dlocators=$LOCATORS
popd