#!/bin/bash

if ! test -d $MAUM_ROOT 
then
    echo "MAUM_ROOT not configured"
    exit 1
fi

SRC_ROOT=../proto
SUB_DIRS=("maum/biz" "maum/biz/collector" "maum/biz/common" "maum/biz/proc")
TGT_ROOT=$MAUM_ROOT/lib/python

case "$1" in
install)
         for dir in ${SUB_DIRS[@]}; do
	     mkdir -p $TGT_ROOT/$dir
             echo "make dir" $TGT_ROOT/$dir
             protoc --proto_path=$SRC_ROOT --python_out=$SRC_ROOT $SRC_ROOT/$dir/*.proto 2> /dev/null
             mv $SRC_ROOT/$dir/*.py $TGT_ROOT/$dir/ 2> /dev/null
             touch $TGT_ROOT/$dir/__init__.py
         done
         echo "done."
         ;;
remove)
         for dir in ${SUB_DIRS[@]}; do
	     rm -rf $TGT_ROOT/$dir/*.py*
             echo "delete files in:" $TGT_ROOT/$dir
         done
         echo "done."
         ;;
*)       
         echo "Usage: {install|remove}"
        exit 2
         ;;

esac
exit 0
