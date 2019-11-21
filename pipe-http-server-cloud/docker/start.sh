#!/usr/bin/env bash

pipe_read_username=$(printf "%q" ${PIPE_READ_USERNAME:-$1})
pipe_read_password=$(printf "%q" ${PIPE_READ_PASSWORD:-$2})
runscope_pipe_read_username=$(printf "%q" ${RUNSCOPE_PIPE_READ_USERNAME:-$3})
runscope_pipe_read_password=$(printf "%q" ${RUNSCOPE_PIPE_READ_PASSWORD:-${4}})
support_username=$(printf "%q" ${SUPPORT_USERNAME:-${5}})
support_password=$(printf "%q" ${SUPPORT_PASSWORD:-${6}})

sed -i "s/{PIPE_READ_USERNAME}/$pipe_read_username/" /etc/aqueduct/pipe/application.yml
sed -i "s/{PIPE_READ_PASSWORD}/$pipe_read_password/" /etc/aqueduct/pipe/application.yml
sed -i "s/{RUNSCOPE_PIPE_READ_USERNAME}/$runscope_pipe_read_username/" /etc/aqueduct/pipe/application.yml
sed -i "s/{RUNSCOPE_PIPE_READ_PASSWORD}/$runscope_pipe_read_password/" /etc/aqueduct/pipe/application.yml
sed -i "s/{SUPPORT_USERNAME}/$support_username/" /etc/aqueduct/pipe/application.yml
sed -i "s/{SUPPORT_PASSWORD}/$support_password/" /etc/aqueduct/pipe/application.yml

exec java \
    -Dmicronaut.config.files=/etc/aqueduct/pipe/application.yml \
    -jar $(ls -1 /opt/aqueduct/pipe/pipe-http-server-cloud-*all.jar | tail -n 1)
