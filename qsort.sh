#!/bin/bash

if [[ ! -s $1 ]]; then echo "input file does not exist!" ; exit 1; fi
cat $1 | ./addhash | tr -d ' ' | sort -S10% -n | cut -d, -f2
