#!/bin/bash
set -o nounset

./proxy_send.sh "proxy.detach('$1')"
