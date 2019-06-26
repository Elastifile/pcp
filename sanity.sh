#!/bin/bash
# 
# File:   sanity.sh
# Author: shaharf
#
# Created on Sep 6, 2018, 3:22:09 PM
#

function panic() {
	echo "PANIC: $*" > /dev/stderr
	exit 1
}

function load_to_cache() {
	cat $f > /dev/null
}

function echodo() {
	echo "$*"
	eval "$*"
}

function dotest() {
    local msg=$1
    shift 1
    local cmd=$*

    echo "$msg"
    load_to_cache
    d=$t.$RANDOM
    time echodo "$cmd"
    cmp $f $m$d || panic "cmp $f to $m$d failed!!!"
    rm -f $m$d
}

LOG="-L pcp.log"
f=$(mktemp)
echo "# Generating 1GB random data file"
dd bs=1M count=1024 if=/dev/urandom of=$f

t=`basename $f`
PCP=./pcp
m="./"

[ -r $f ] || panic "no src file '$f'" 

dotest "# cp local -> local copy " '(cp $f $d; sync)'

dotest "# pcp local -> local" '($PCP $LOG -f $f $d; sync)'

dotest "# pcp stream -> local" '(cat $f | $PCP $LOG -f - $d; sync)'

dotest "# pcp local -> stream" '($PCP $LOG $f - | cat > $d; sync)'

dotest "# pcp local -> local (SYNC)" '($PCP $LOG -s -f $f $d; sync)'

dotest "# pcp stream -> local (SYNC)" '(cat $f | $PCP $LOG -s -f - $d; sync)'

echo "# mkdir a new dir and 3 symlink"
m1=dir1-$RANDOM
m2=dir2-$RANDOM
m3=dir3-$RANDOM
real=dir-$RANDOM
mkdir -p $real
ln -s $real $m1
ln -s $real $m2
ln -s $real $m3

m="$real/"    # for cmp

dotest "# pcp local -> local (multipath)" '($PCP -f $f -- $m1/$d $m2/$d $m3/$d; sync)'

echo "# rm data file $f"
rm -f $f
