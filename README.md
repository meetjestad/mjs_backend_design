To run, you need a secrets.env with the TTN access credentials. e.g. something
like:

	TTN_APP_ID=meet-je-stad-test
	TTN_ACCESS_KEY=ttn-account-v2.xxxxxxxxxxxxxxxxxxxxx

To start stuff:

	docker-compose up -d

This creates a number of related docker containers, whose names are prefixed
with the name of the current directory. On startup, the kafka clients will likely show some errors in the logs, since kafka needs a few seconds to initialize and start, but they should recover automatically (and silently).

To view logs, e.g.:

	docker logs -f mjsbackenddesign_ttn-kafka-producer_1


To view the data in a kafka queue, you can run a commandline consumer inside the kafka container:

	docker exec -it mjsbackenddesign_kafka_1 kafka-console-consumer --bootstrap-server localhost:9092 --topic ttndata.meet-je-stad-test

Add `--from-beginning` to see all historical data, rather than just new data as it comes in.

Updating containers
-------------------
After you made changes to the code, you can rebuild the images and update the
containers with:

	docker-compose up -d --build

If you just made changes to the docker-compose file or env files, you can omit
`--build` and docker will recreate (if needed) the running container with the
most recently build image.

Note that currently the kafka image has no persistent storage set up, so
recreating the kafka image will remove data from the kafka queues.

Running outside of docker
-------------------------
During development, it can be useful to run some scripts outside of docker. To
do so, a start script is provided that reads the same config as the docker
version, or has its own config where needed. For example, to run the kafka
producer change into the `ttn-kafka-producer` directory and run:

	$ pip install -r requirements.txt
	$ ./start

This first installs the dependencies, and then runs the script. You might need to run pip with `sudo`, with `--user` or create and activate a virtualenv beforehand to make sure you can actually install the dependencies. You can also install the dependencies using OS packages (e.g. using apt) instead.
