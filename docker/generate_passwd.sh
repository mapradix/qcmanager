#!/bin/sh

echo "`id -un`::`id -u`::::" > docker/passwd

exit 0
