#!/bin/bash -e

podman build --network host -f Dockerfile.debian13 -t pyinstaller-builder-debian-13 .
