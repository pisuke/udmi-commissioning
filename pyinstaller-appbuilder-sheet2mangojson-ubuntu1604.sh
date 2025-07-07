#!/bin/bash -e
set -x
ROOT_DIR=$(realpath $(dirname $0)/)

TMP_DIR=$(mktemp -d)
OUT_DIR=$ROOT_DIR/dist
OUT_FILE=$OUT_DIR/sheet2mangojson-ubuntu1604

echo Building binary to 
cat >$TMP_DIR/build.sh <<-EOF
            set -x
            rm -rf /tmp/sheet2mangojson
            rm -rf /dist/sheet2mangojson
            mkdir /build
            cp -r /src/sheet2mangojson.py /build
            cp -r /src/requirements.txt /build
            cd /build
            python3 -m pip install --upgrade pip
            python3 -m pip install -r requirements.txt
            pyinstaller --onefile --hidden-import=pandas sheet2mangojson.py
            ls -la dist/*
            mv dist/sheet2mangojson /tmp/sheet2mangojson
EOF

docker run --rm --volume $ROOT_DIR/:/src --volume $TMP_DIR:/tmp pyinstaller-builder-ubuntu-1604:latest /bin/bash /tmp/build.sh
mkdir -p $OUT_DIR
mv $TMP_DIR/sheet2mangojson $OUT_FILE
chmod 777 $OUT_FILE
