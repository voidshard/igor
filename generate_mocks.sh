#! /bin/sh

# Generate mocks for all interfaces in the project
#
# Essentially we look for interface.go files and then generate a _mock package
# under internal/mocks following the same directory structure as the interface itself.
#
# Requires mockgen to be installed.
# > go install go.uber.org/mock/mockgen@latest
# For details: https://github.com/uber-go/mock
#

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR

MOCK_DIR=internal/mocks
mkdir -p $MOCK_DIR

set -ex

for i in $(find ./ -type f -name "interface.go"); do
    package=$(head -n 1 $i | cut -d " " -f 2)_mock
    idir=$(dirname $i)_mock
    mockgen -source=$i -destination=$MOCK_DIR/$idir/autogenerated.go -package=$package
done

cd -