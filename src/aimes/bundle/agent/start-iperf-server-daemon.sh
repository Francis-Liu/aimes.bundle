#!/bin/bash

if [ $# -lt 1 ]
then
    echo "Usage : $0 <port>"
    exit
fi

port=$1

if [ ! -f iperf-3.0.11-source.tar.gz ]
then
    curl --insecure -Os https://iperf.fr/download/iperf_3.0/iperf-3.0.11-source.tar.gz
fi

if [ ! -f ./iperf-3.0.11/src/iperf3 ]
then
    tar -xvzf iperf-3.0.11-source.tar.gz
    cd iperf-3.0.11
    ./configure
    make
    cd ..
fi

netstat -anp | grep $port
found=$?
while [ $found -eq 0 ]
do
    ((port++))
    netstat -anp | grep $port
    found=$?
done

echo "`hostname --fqdn` $port" > PORT
./iperf-3.0.11/src/iperf3 -s -D -1 -p $port
