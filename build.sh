#!/bin/bash

# Builds an executable zip file containing the collector script and pg8000

set -x
set -e

BRANCH=`git rev-parse --abbrev-ref HEAD`
REV=`git rev-parse --short HEAD`
FILENAME="/tmp/pganalyze-collector-$BRANCH-$REV.zip"

cp pganalyze-collector.py __main__.py

zip $FILENAME.tmp -r LICENSE __main__.py pg8000
echo "#!/usr/bin/env python" | cat - $FILENAME.tmp > $FILENAME
chmod +x $FILENAME
rm $FILENAME.tmp
rm __main__.py
