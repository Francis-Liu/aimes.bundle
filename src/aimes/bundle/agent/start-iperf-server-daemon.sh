#!/bin/bash

if [ $# -lt 2 ]
then
    echo "Usage : $0 <port>"
    exit
fi

iperf_port=$1

if [ ! -f ./iperf-3.0.11/src/iperf3 ]
then
    tar -xvzf iperf-3.0.11-source.tar.gz
    cd iperf-3.0.11
    ./configure
    make
    cd ..
fi

./iperf-3.0.11/src/iperf3 -s -D -p $iperf_port
