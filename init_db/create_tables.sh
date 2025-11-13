# create ydbcp tables
./ydb -e ${YDB_ENDPOINT} -d /local scripting yql -f init_db/schema/create_tables.yql

# create and fill user table kv_test
./ydb -e ${YDB_ENDPOINT} -d /local workload kv init
./ydb -e ${YDB_ENDPOINT} -d /local workload kv run upsert --rows 100

# create and fill user tables: stock, orders, orderLines
./ydb -e ${YDB_ENDPOINT} -d /local workload stock init -p 10 -q 10 -o 10

# create directory for user tables
./ydb -e ${YDB_ENDPOINT} -d /local scheme mkdir stocks

# move user tables (stock, orders, orderLines)  to separate directory
./ydb -e ${YDB_ENDPOINT} -d /local tools rename \
  --item source=/local/stock,destination=/local/stocks/stock \
  --item source=/local/orders,destination=/local/stocks/orders \
  --item source=/local/orderLines,destination=/local/stocks/orderLines