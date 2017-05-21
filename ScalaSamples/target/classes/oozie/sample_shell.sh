cd "$(dirname "$0")"


hive -e "show databases">>list_of_databases.txt
