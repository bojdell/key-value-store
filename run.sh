#!/bin/bash

. newtab.sh

newtab -G python ./server-client.py ./conf.txt A
newtab -G python ./server-client.py ./conf.txt B
newtab -G python ./server-client.py ./conf.txt C
newtab -G python ./server-client.py ./conf.txt D
newtab -G python ./central-server.py ./conf.txt CENTRAL

echo "Done"