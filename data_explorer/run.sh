#!/bin/bash -e
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)

bash ${baseDirForScriptSelf}/0_create_table.sh nlp_dev template_final qianyu_20220318

bash ${baseDirForScriptSelf}/1_insert_data.sh nlp_dev nlp_dev template_final qianyu_20220318 2021-11