#!/bin/bash -e

podman build --network host -f Dockerfile.ubuntu1804 -t pyinstaller-builder-ubuntu-1804 .
