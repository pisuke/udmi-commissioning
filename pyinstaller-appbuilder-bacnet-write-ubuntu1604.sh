#!/bin/bash -e
set -x
ROOT_DIR=$(realpath $(dirname $0)/)

TMP_DIR=$(mktemp -d)
OUT_DIR=$ROOT_DIR/dist
OUT_FILE=$OUT_DIR/bacnet-write-ubuntu1604

echo Building binary to 
cat >$TMP_DIR/build.sh <<-EOF
            set -x
            rm -rf /tmp/bacnet-write
            rm -rf /dist/bacnet-write
            mkdir /build
            cp -r /src/bacnet-write.py /build
            # cp -r /src/bacnet-write.spec /build
            cp -r /src/requirements.txt /build
            cd /build
            python3 -m pip install --upgrade pip
            python3 -m pip install -r requirements.txt
            wget https://github.com/pisuke/BAC0/archive/refs/heads/unknown-units-2023.zip
            unzip unknown-units-2023.zip
            python3 -m pip install ./BAC0-unknown-units-2023/
            ls -la /usr/local/lib/python3.12/site-packages
            ls -la /usr/local/lib/python3.12/site-packages/packaging
            ls -la /usr/local/lib/python3.12/site-packages/pkg_resources
            ls -la /usr/local/lib/python3.12/lib-dynload
            python3 --version
            python3 -m pandas
            pyinstaller --onefile --hidden-import=pandas bacnet-write.py
            ls -la dist/*
            mv dist/bacnet-write /tmp/bacnet-write
EOF

docker run --rm --volume $ROOT_DIR/:/src --volume $TMP_DIR:/tmp pyinstaller-builder-ubuntu-1604:latest /bin/bash /tmp/build.sh
mkdir -p $OUT_DIR
mv $TMP_DIR/bacnet-write $OUT_FILE
chmod 777 $OUT_FILE
