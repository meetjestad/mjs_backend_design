To run, you need a secrets.env with the TTN access key. e.g. something like:

	TTN_ACCESS_KEY=ttn-account-v2.xxxxxxxxxxxxxxxxxxxxx

This key should match the ttn application id in `docker-compose.yml`.

To start stuff:

	docker-compose up -d

This creates a number of related docker containers, whose names are prefixed
with the name of the current directory. On startup, the kafka clients will likely show some errors in the logs, since kafka needs a few seconds to initialize and start, but they should recover automatically (and silently).

To view logs, e.g.:

	docker logs -f mjsbackenddesign_ttn-kafka-producer_1


To view the data in a kafka queue, you can run a commandline consumer inside the kafka container:

	docker exec -it mjsbackenddesign_kafka_1 kafka-console-consumer --bootstrap-server localhost:9092 --topic ttndata.meet-je-stad-test

Add `--from-beginning` to see all historical data, rather than just new data as it comes in.
