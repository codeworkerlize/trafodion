### EsgynDB Single Node Docker Image Builder

This image builder can help you to build a single node EsgynDB runtime environment quickly and easily, it is using Apache Hadoop2.7, Apache HBase1.1, Apache Hive1.2.2 as the backend.


#### How to use

> This guide assumes that docker application is installed on your machine.

##### 1. Build with any EsgynDB R2.7 or later release:

> Run below command under the Dockerfile folder:


```
export ESGYNDB_DOWNLOAD_LINK=<esgyndb_http_download_link>
export ESGYNDB_VERSION=<esgyndb_version>
export ESGYNDB_DOCKER_VERSION=<esgyndb_docker_version>
./build.sh
```

For example:
```
export ESGYNDB_DOWNLOAD_LINK=http://downloads.esgyn.local/esgyn/AdvEnt2.7/publish/daily/20190315_05-rh7-release/esgynDB_server-2.7.0-RH7-x86_64.tar.gz
export ESGYNDB_VERSION=2.7
export ESGYNDB_DOCKER_VERSION=esgyndb:2.7-daily190315
./build.sh
```

> If the $ESGYNDB_DOWNLOAD_LINK env is not provided, it will get the latest daily build release from downloads.esgyn.local for the EsgynDB version specified by $ESGYNDB_VERSION.
> If $ESGYNDB_VERSION is not provided, the default value will be 2.7
> If $ESGYNDB_DOCKER_VERSION is not provided, the default value will be esgyndb:2.7

So you can have a quick try without providing any envs, simply run `./build.sh`

##### 2. Start the single node EsgynDB docker container:

`./start.sh` or `./start.sh <hostIP>` (It will set up dcs.cloud.command in dcs-site.xml)

All services will be started automatically in the background within 5 mins, you can attach to the container using `docker exec -it <container id> bash`, then use `sqcheck` to get the status, or use `docker logs <container id>` to check the start log.

When the instance is up, you can open DB Manager in your web brower, check the port number using `docker ps`, in below example, the random port number is `32816`, so the DB Manager URL is `http://<host-ip>:32816`:

```
# docker ps
CONTAINER ID        IMAGE                                       COMMAND                  CREATED             STATUS              PORTS                                                                                                                                                                                 NAMES
3bd5520347e7        esgyndb:2.7                                 "/opt/start.sh"          17 hours ago        Up 17 hours         0.0.0.0:32816->4205/tcp
```

##### 3. Export the single node docker image:

The image builder can also be used to generate the docker image based on any EsgynDB release. Using below command to save the generated docker image to a tar file:

`docker save <esgyndb_docker_version> > /path/to/docker-image.tar.gz`

For example:
`docker save esgyndb:2.7 > /opt/esgyndb2.7-docker.tar.gz`
