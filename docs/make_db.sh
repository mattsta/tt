#!/bin/sh

DB=$1

tcbmgr create -cz -tl $DB.tcb 256 512 120000 -1 13
