#!/usr/bin/env bash

pipe_read_username=$(printf "%q" ${PIPE_READ_USERNAME:-$1})
runscope_pipe_read_username=$(printf "%q" ${RUNSCOPE_PIPE_READ_USERNAME:-$2})
support_username=$(printf "%q" ${SUPPORT_USERNAME:-${3}})

sed -i "s/{PIPE_READ_USERNAME}/$pipe_read_username/" /etc/aqueduct/pipe/application.yml
sed -i "s/{RUNSCOPE_PIPE_READ_USERNAME}/$runscope_pipe_read_username/" /etc/aqueduct/pipe/application.yml
sed -i "s/{SUPPORT_USERNAME}/$support_username/" /etc/aqueduct/pipe/application.yml

exec java \
    -Dmicronaut.config.files=/etc/aqueduct/pipe/application.yml \
    -jar $(ls -1 /opt/aqueduct/pipe/pipe-http-server-cloud-*all.jar | tail -n 1)
