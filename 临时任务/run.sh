#!/bin/bash -e
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
bash ${baseDirForScriptSelf}/0_create_table.sh
bash ${baseDirForScriptSelf}/1_insert_table.sh nlp_dev