#!/bin/bash
chmod 644 $(git log -p HEAD | grep -B1 "old mode" | grep diff | grep -v /bin | awk -F "b/" '{print $2}')
