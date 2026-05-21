#!/bin/bash -e

podman build --network host -f Dockerfile.debian11 -t pyinstaller-builder-debian-11 .
