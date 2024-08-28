# create ydbcp tables
./ydb -e grpc://ydbcp-ydb-1.ydbcp-net:2136 -d /local scripting yql -f init_db/schema/create_tables.yql

# create and fill user table kv_test
./ydb -e grpc://ydbcp-ydb-1.ydbcp-net:2136 -d /local workload kv init
./ydb -e grpc://ydbcp-ydb-1.ydbcp-net:2136 -d /local workload kv run upsert --rows 100