#/bin/bash


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

LOG="-L pcp.log"
f=$(mktemp)
dd bs=1M count=1024 if=/dev/urandom of=$f

t=`basename $f`
PCP=./pcp

m0=/root/work/CitiProject/samples/fuse_mirror/mountpoint0
m1=/root/work/CitiProject/samples/fuse_mirror/mountpoint1
m2=/root/work/CitiProject/samples/fuse_mirror/mountpoint2

[ -r $f ] || panic "no src file '$f'"

echo "# cp local -> local copy "
load_to_cache
time (echodo cp $f $t.$RANDOM; sync)

echo "# cp local -> elfs"
load_to_cache
time (echodo cp $f $m0/$t.$RANDOM; sync)

echo "# pcp local -> local copy "
load_to_cache
time (echodo $PCP $LOG $f $m0/$t.$RANDOM; sync)

echo "# pcp local -> elfs"
load_to_cache
time (echodo $PCP $LOG $f $m0/$t.$RANDOM; sync)

echo "# pcp local -> 3 mounts "
load_to_cache
time (echodo $PCP $LOG -M $m0 -M $m1 -M $m2 $f $t.$RANDOM; sync)

echo "# pcp local -> elfs (128K)"
load_to_cache
time (echodo $PCP $LOG -b 128k $f $m0/$t.$RANDOM; sync)

echo "# pcp local -> 3 mounts (128k)"
load_to_cache
time (echodo $PCP $LOG -b 128k -M $m0 -M $m1 -M $m2 $f $t.$RANDOM; sync)

