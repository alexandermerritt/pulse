#! /usr/bin/env bash
set -e
set -u

rm -fv /tmp/*bolt*
rm -fv /tmp/*spout*
rm -fv /tmp/preamble-*.pch

num=$(ls /tmp/ | egrep '^[0-9a-f]{8}-' | wc -l)
if [[ $num -gt 0 ]]; then
    ls /tmp/ \
        | egrep '^[0-9a-f]{8}-' \
        | paste -d '' <(yes /tmp/ | head -n $num) - \
        | xargs rm -vrf
fi

