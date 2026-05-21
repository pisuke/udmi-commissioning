#!/bin/bash -e

podman build --network host -f Dockerfile.debian12 -t pyinstaller-builder-debian-12 .
