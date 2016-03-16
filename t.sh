#!/usr/bin/env bash

set -e

while :
do
	redis-cli flushall
#	activator 'project s2core' 'test-only com.kakao.s2graph.core.Integrate.StrongLabelDeleteTest' &> out
	activator 'project s2core' 'redis:test' &> out
	sleep 1
done

