build:
	docker build -t quay.io/ucsc_cgl/dcc-action-service .

run:
	# Apply migrations and then run using the built image in daemon mode
	sudo docker-compose up -d --build --force-recreate

populate:
	# Populate the ElasticSearch index
	sudo docker-compose exec dcc-action-service /app/test/populator.sh

unittests:
	# Run pytest inside the running container from run
	sudo docker-compose exec dcc-action-service py.test -p no:cacheprovider -s -x

stop:
	sudo docker-compose down --rmi 'all'

# This is to set up the environment for some quick prototyping. This will setup ElasticSearch and Kibana bounded on the
# local host loaded with some testing files
play: stop reset run
	sleep 60
	echo "Finished Sleeping 60 Seconds"
	$(MAKE) populate
#	echo "Kibana is now available on localhost:5601"

# Clears the whole system, reloads indexes, and runs the tests
testme: play unittests

reset:
	sudo docker-compose stop
	sudo docker-compose rm -f
