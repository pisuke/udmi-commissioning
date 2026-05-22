#!/bin/bash -e

podman build --network host -f Dockerfile.ubuntu2004 -t pyinstaller-builder-ubuntu-2004 .
