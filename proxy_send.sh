#!/bin/bash

echo -ne "$1\r\n" | ncat localhost 8787
