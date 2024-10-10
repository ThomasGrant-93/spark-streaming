#!/bin/bash

kafka-topics.sh --bootstrap-server localhost:9092 --create --topic weather --partitions 2 --replication-factor 1
