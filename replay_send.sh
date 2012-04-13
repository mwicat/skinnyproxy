#!/bin/bash

echo "$1" | ncat --crlf --send-only localhost 8989
