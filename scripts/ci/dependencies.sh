#!/usr/bin/env bash

sudo apt-get update -qq

# install git
sudo apt-get install -y git

# build incubator-twill
git clone https://github.com/Lab41/incubator-twill.git
cd incubator-twill
git checkout sync
mvn install -DskipTests
cd ..
