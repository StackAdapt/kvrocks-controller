#!/bin/bash

env GOOS=linux GOARCH=arm64 go build
scp -S'/usr/local/bin/sdm' -osdmSCP reader kvrocks-byron-load-test-node-8.us-east:~
scp -S'/usr/local/bin/sdm' -osdmSCP reader kvrocks-byron-load-test-node-7.us-east:~
scp -S'/usr/local/bin/sdm' -osdmSCP reader kvrocks-byron-load-test-node-6.us-east:~
scp -S'/usr/local/bin/sdm' -osdmSCP reader kvrocks-byron-load-test-node-5.us-east:~
scp -S'/usr/local/bin/sdm' -osdmSCP reader kvrocks-byron-load-test-node-4.us-east:~
scp -S'/usr/local/bin/sdm' -osdmSCP reader kvrocks-byron-load-test-node-3.us-east:~
scp -S'/usr/local/bin/sdm' -osdmSCP reader kvrocks-byron-load-test-node-2.us-east:~
scp -S'/usr/local/bin/sdm' -osdmSCP reader kvrocks-byron-load-test-node-1.us-east:~
