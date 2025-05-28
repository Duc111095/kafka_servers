#!/bin/bash
while IFS="" read -r p || [ -n "$p" ]
do
  servers=()
  for i in $p; do servers+=($i); done
  n_ma_bp=${servers[0]}
  IFS='\\' read -ra n_server <<< "${servers[1]}"
  n_hostname=${n_server[0]}
  n_instance=${n_server[1]}
  n_db_name=${servers[2]}

  generate_post_data()
  {
cat <<EOF
  	{
	"name": "notify-$n_ma_bp-connector",
    	"config": {
        	"connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
        	"database.hostname": "$n_hostname",
        	"database.instance": "$n_instance",
        	"database.port": "1433",
        	"database.user": "sa",
        	"database.password": "123456a@@",
        	"database.names": "$n_db_name",
        	"topic.prefix": "notify",
        	"table.include.list": "dbo.notify_zulip",
        	"schema.history.internal.kafka.bootstrap.servers": "localhost:9092",
        	"schema.history.internal.kafka.topic": "schemahistory.notify_zulip",
		"database.encrypt": "false",
 		"database.trustServerCertificate": "true"
    		}
	}
EOF
}
  curl -X POST localhost:8083/connectors -H 'Content-Type: application/json' -d "$(generate_post_data)"
done < /home/fbo/kafka/kafka_servers/utils/server.txt
