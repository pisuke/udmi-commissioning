#!/bin/bash -e
set -x
ROOT_DIR=$(realpath $(dirname $0)/)

TMP_DIR=$(mktemp -d)
OUT_DIR=$ROOT_DIR/dist
OUT_FILE=$OUT_DIR/bacnet-scan-ubuntu1604

echo Building binary to 
cat >$TMP_DIR/build.sh <<-EOF
            set -x
            rm -rf /tmp/bacnet-scan
            rm -rf /dist/bacnet-scan
            mkdir /build
            cp -r /src/bacnet-scan.py /build
            cp -r /src/bacnet-scan.spec /build
            cp -r /src/requirements.txt /build
            cd /build
            python3 -m pip install --upgrade pip
            python3 -m pip install -r requirements.txt
            ls -la /usr/local/lib/python3.12/site-packages
            ls -la /usr/local/lib/python3.12/site-packages/packaging
            ls -la /usr/local/lib/python3.12/site-packages/pkg_resources
            ls -la /usr/local/lib/python3.12/lib-dynload
            python3 --version
            python3 -m pandas
            pyinstaller --onefile --hidden-import=pandas bacnet-scan.py
            ls -la dist/*
            mv dist/bacnet-scan /tmp/bacnet-scan
EOF

docker run --rm --volume $ROOT_DIR/:/src --volume $TMP_DIR:/tmp pyinstaller-builder-ubuntu-1604:latest /bin/bash /tmp/build.sh
mkdir -p $OUT_DIR
mv $TMP_DIR/bacnet-scan $OUT_FILE
chmod 777 $OUT_FILE
