#!/bin/bash

source ./set-env.sh

echo "Deploying mirror service..."
pushd ../mirror
mvn os:deploy -Dgroups=$GROUP -Dlocators=$LOCATORS
popd

echo "Deploying processor..."
pushd ../processor
mvn os:deploy -Dgroups=$GROUP -Dlocators=$LOCATORS -Dsla=./../dev-scripts/sla/processor-sla.xml
popd

echo "Deploying feeder..."
pushd ../feeder
mvn os:deploy -Dgroups=$GROUP -Dlocators=$LOCATORS
popd

echo "Deploying consumer..."
pushd ../consumer
mvn os:deploy -Dgroups=$GROUP -Dlocators=$LOCATORS
popd