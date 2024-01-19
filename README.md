To run, you need to create a file called `secrets.env` in this directory
with the needed credentials. e.g. something like:

	TTN_APP_ID=meet-je-stad-test
	TTN_ACCESS_KEY=ttn-account-v2.xxxxxxxxxxxxxxxxxxxxx
	ME_CONFIG_BASICAUTH_USERNAME=root
	ME_CONFIG_BASICAUTH_PASSWORD=some_password

Here, the TTN credentials should be taken from the TTN console, whereas
the ME (Mongo Express) credentials will be used to configure ME and can
be used to login later.

To start stuff:

	docker-compose up -d

This creates a number of related docker containers, whose names are prefixed
with the name of the current directory. On startup, the redis clients
will likely show some errors in the logs, since redis needs a few
seconds to initialize and start, but they should recover automatically
(and silently).

To view logs, e.g. of the redis producer (TTN client):

	docker logs -f mjsbackenddesign_ttn-redis-producer_1

Or of the the redis decoder (Mongodb writer):

	docker logs -f mjs_backend_design_ttn-redis-decoder_1

To view the data in a redis queue, you can run a commandline consumer inside the redis container:

	docker exec -it mjsbackenddesign_redis_1 redis-console-consumer --bootstrap-server localhost:6379 --topic ttndata.meet-je-stad-test

Add `--from-beginning` to see all historical data, rather than just new data as it comes in.

To view data in mongo, you can use the webinterface bound by docker to
http://localhost:8081

Updating containers
-------------------
After you made changes to the code, you can rebuild the images and update the
containers with:

	docker-compose up -d --build

If you just made changes to the docker-compose file or env files, you can omit
`--build` and docker will recreate (if needed) the running container with the
most recently build image.

To rebuild just one container, add its name, e.g.:

	docker-compose up -d --build ttn-redis-decoder

Note that currently the redis and elasticsearch images have no
persistent storage set up, so recreating the redis image will remove
data from the redis queues.

Running outside of docker
-------------------------
During development, it can be useful to run some scripts outside of docker. To
do so, a start script is provided that reads the same config as the docker
version, or has its own config where needed. For example, to run the redis
producer change into the `ttn-redis-producer` directory and run:

	$ pip install -r requirements.txt
	$ ./start

This first installs the dependencies, and then runs the script. You
might need to run pip with `sudo`, with `--user` or create and activate
a virtualenv beforehand to make sure you can actually install the
dependencies. You can also install the dependencies using OS packages
(e.g. using apt) instead.
