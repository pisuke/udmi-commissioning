#!/bin/bash -e

podman build --network host -f Dockerfile.rocky810 -t pyinstaller-builder-rocky-810 .
