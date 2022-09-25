#!/bin/bash
TMPFILE=$(mktemp /tmp/PyInstallInfo.XXX)
hdrFile="./python-installer/PyInstallerVer"
verinfo=

if [ $# -gt 0 ]; then
     verinfo="${1}"
fi

[ -z "$verinfo" ] && verinfo=NoInfo

cat > $TMPFILE <<EOF
$verinfo
EOF

diff --brief --new-file $TMPFILE $hdrFile 2>&1 >/dev/null
dh=$?
if [ $dh -ne 0 ]; then
    echo "Creating file $hdrFile"
    cp -f $TMPFILE $hdrFile
fi

rm -f $TMPFILE
