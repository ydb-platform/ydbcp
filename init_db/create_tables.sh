# create ydbcp tables
./ydb -e ${YDB_ENDPOINT} -d /local scripting yql -f init_db/schema/create_tables.yql

# create and fill user table kv_test
./ydb -e ${YDB_ENDPOINT} -d /local workload kv init
./ydb -e ${YDB_ENDPOINT} -d /local workload kv run upsert --rows 100