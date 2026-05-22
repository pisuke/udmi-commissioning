#!/bin/bash -e

podman build --network host -f Dockerfile.ubuntu1604 -t pyinstaller-builder-ubuntu-1604 .
