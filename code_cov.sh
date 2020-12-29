#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

INCLUDE="/src"
EXCLUDE="/.cargo,/examples"
TARGET="target/cov"

KCOV_ARGS="--include-pattern=$INCLUDE --exclude-pattern=$EXCLUDE --verify $TARGET"

TSXLIB_UNIT_TESTS="target/debug/tsxlib-"

export RUSTFLAGS="-C link-dead-code"

echo -e "${GREEN}*** Clean previous coverage results and executables ***${NC}"
rm -rf "$TARGET"
rm -f "$TSXLIB_UNIT_TESTS"*


echo -e "${GREEN}*** Rebuilding tests ***${NC}"
cargo test --no-run

echo -e "${GREEN}*** Run coverage on tsxlib unit tests ***${NC}"
for test_file in `ls "$TSXLIB_UNIT_TESTS"*`
do
    if [[ ! -x "$test_file" ]]; then
        continue
    fi
    kcov $KCOV_ARGS "$test_file"
    if [ "$?" != "0" ]; then
        echo -e "${RED}*** Failure during unit test converage ***${NC}"
        exit 1
    fi
done

echo -e "${GREEN}*** Coverage completed successfully ***${NC}"