#!/usr/bin/env bash

POD_ID=$(docker ps -aq -f "name=registry"  --no-trunc)

if [[ ${POD_ID} == "" ]]
then
  echo "Registry container not running"
 else
  echo "Deleting Registry service image ${POD_ID}"
  docker kill ${POD_ID} > /dev/null 2>&1
  docker rm ${POD_ID} > /dev/null 2>&1
fi

