#!/bin/sh

MANUAL=../spatial-manual

if [ -d  "$MANUAL" ] ; then
  mvn install -Dmaven.test.skip=true && (cd $MANUAL ; mvn install)
else
  echo "No spatial-manual project found"
  echo "Try: cd .. ; git clone git@github.com:nawroth/spatial-manual.git"
fi
