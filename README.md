Meet je stad backend experiments
================================
This repository contains a proof of concept for the [Meet Je
Stad](https://meetjestad.net) data backend, based on
[CSDIF](https://csdif.info) (using SensorThings API and SensorML).

This repository is based on docker compose, with different containers
working together to ingest data from The Things Network, save raw
messages into a postgresql database for archiving and decode the
messages into a SensorThings server (using FROST-Server). The processing
done by a few distinct python scripts, which communicate via redis
streams.


Initial setup
-------------
To run the stack, you can use the provided `docker-compose.yml` file.

Before running, you need to set up some credentials.

 1. Create a file called `secrets.env` in the repository root containing
    the credentials to receive data from TTN. e.g. something like:

		TTN_APP_ID=meet-je-stad
		TTN_ACCESS_KEY=NNSXS.xxxxxxxxxxxxxxxxxxxxx

    Here, the TTN credentials should be taken from the TTN console.

 2. To set up authentication for the FROST Server, run this script
    (normally before starting any containers, can also run after):

		./set-frost-passwords

    This will start frost to create the users table, and then generate
    random passwords for the users. The pw for the write user is saved
    to secrets.env to be used by the other scripts.

To then start everything, run:

	docker compose up -d

This creates a number of related docker containers, whose names are prefixed
with the name of the current directory. On startup, the redis clients
will likely show some errors in the logs, since redis needs a few
seconds to initialize and start, but they should recover automatically
(and silently).

To view logs of various containers:

	docker compose logs -f ttn-to-redis
	docker compose logs -f ttn-save-message
	docker compose logs -f legacy-convert
	docker compose logs -f frost-web
	docker compose logs -f frost-db

Accessing the API
-----------------
When started as above, the SensorThigns API is accessible below:

   http://localhost:8080/FROST-Server/v1.1/O

For example, to get a list of Things:

   http://localhost:8080/FROST-Server/v1.1/Things

Updating containers
-------------------
After you made changes to the code, you can rebuild the images and update the
containers with:

	docker compose up -d --build

If you just made changes to the docker-compose file or env files (and
not the code), you can omit `--build` and docker will recreate (if
needed) the running container with the most recently build image.

To rebuild just one container, add its name, e.g.:

	docker compose up -d --build legacy-convert

Quick restarts during development
---------------------------------
When working on the python code, it is cumbersome and wasteful to
rebuild the container images for every change.

Using a volume mount, the latest code can be inserted into an existing
container without recreating or rebuilding it. This is already done in
the `docker-compose-dev.yml` file. For example, when working on the
`legacy-convert` application, you can run:

	docker compose -f docker-compose-dev.yml up legacy-convert

If you run this while all containers are already running, this will
replace (recreate) just this one container with the latest code. To then
restart it with modified code, just quit it with ^C and then rerun the
above command.

Importing or replaying older messages
-------------------------------------
The ttn-utility tool can be used to do some maintenance, currently only
importing messages from file, or replaying messages from the db where they are
stored.

This utility is also contained in a docker compose container, which is not
started by default, but can be started explicitly:

```
docker compose -f docker-compose-dev.yml run ttn-utility --help
```

To import messages from file (zstd-encoded tab-separated values as exported from the current production mysql db):

```
docker compose -f docker-compose-dev.yml run --volume ./data:/data ttn-utility import-from-file /data/file.tsv.zstd
```

This queues messages into redis, expecting them to be processed by
ttn-save-msg. Currently there is no good mechanism for backpressure, which can
leead to very big redis queues. The utility will wait if the first queue fills
up, but if ttn-save-msg can keep up, but legacy-convert lags behind, the second
queue can still fill up. A current workaround is to modify ttn-save-msg to not
forward message to the second queue, but instead use a manual db replay
afterwards.

Since legacy-convert does not handle messages it has seen before (it assumes it
sees only new messages in chronological order), you should currently clear out
the FROST db and saved redis stream before replaying messages. Also recreate
legacy-convert to clear in-memory caches and load the new FROST password. It is
also advisable to stop the ttn-save-msg container to prevent mixing new and old
messages (they will still be fetch by ttn-to-redis and saved in the first redis
stream).

For example:

```
docker compose down frost-db frost-web -v
./set-frost-passwords
docker compose stop ttn-save-msg
docker compose exec redis redis-cli DEL saved.ttn.meet-je-stad 0
docker compose -f docker-compose-dev.yml up -d legacy-convert
docker compose -f docker-compose-dev.yml run ttn-utility replay-from-db --start-date 2024-11-29 --end-date 2024-11-30
docker compose start ttn-save-msg
```

This readds older messages to the redis stream, to be processed by legacy convert.

Useful commands
---------------
To delete all data in redis:

	docker compose exec redis redis-cli flushall

To view new data streaming in two streams:

    docker compose exec redis redis-cli -r 99999 XREAD BLOCK 0 STREAMS ttn.meet-je-stad saved.ttn.meet-je-stad "$" "$"

This has a small race condition because it reads one message at a time
and then retries with -r starting at the the last message "$" every
time, since redis-cli does not support proper streaming.

To view all existing data in a single stream:

    docker compose exec redis redis-cli XREAD STREAMS ttn.meet-je-stad 0
    docker compose exec redis redis-cli XREAD STREAMS saved.ttn.meet-je-stad 0

To see how many items there are in a stream:

    docker compose exec redis redis-cli XLEN ttn.meet-je-stad
    docker compose exec redis redis-cli XLEN saved.ttn.meet-je-stad

To get messages pending in a consumer group (i.e. processing was
attempted, but not finished or interrupted):

    docker compose exec redis redis-cli XPENDING ttn.meet-je-stad legacy-convert
    docker compose exec redis redis-cli XPENDING saved.ttn.meet-je-stad legacy-convert

Get info about a stream, including the size of the queue ("length") and
the number of pending (not acked) messages ("pel-length"):

    docker compose exec redis redis-cli XINFO STREAM ttn.meet-je-stad FULL COUNT 1
    docker compose exec redis redis-cli XINFO STREAM saved.ttn.meet-je-stad FULL COUNT 1

Query the decoder database:

    docker compose exec timescale psql -U postgres postgres --command "SELECT * FROM rawmessage LIMIT 1;"
