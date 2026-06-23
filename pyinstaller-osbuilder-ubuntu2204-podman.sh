#!/bin/bash -e

podman build --network host -f Dockerfile.ubuntu2204 -t pyinstaller-builder-ubuntu-2204 .
