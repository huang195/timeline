#!/bin/bash

# Change yum repo mirror and local directory
rsync --progress -av --delete --delete-excluded --exclude "local" --exclude "isos" --exclude "i386" rsync://mirror.clarkson.edu/centos/6.5/ /home/haih/repo/centos/6.5/
