#!/usr/bin/env bash
set -e

if [[ -z "${DRUST_HOME}" ]]; then
  DRUST_HOME=$HOME/DRust/
fi

cd $DRUST_HOME/comm-lib
make clean
make -j lib
cp libmyrdma.a $DRUST_HOME/drust/

cd $DRUST_HOME/drust
cargo clean --release -p drust
cargo build --release
