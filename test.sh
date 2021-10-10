#!/bin/bash

cd tester/test

go build

rm -rf logs

cp -r ../../logs .

./test logs 1

cd ../Knossos_testing

./test.sh
