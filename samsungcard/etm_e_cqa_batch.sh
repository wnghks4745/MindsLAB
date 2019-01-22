INPUT_1=$1
INPUT_2=$2
export MAUM_ROOT=/data1/maum
export PATH=$PATH:/usr/local/cuda/bin:$MAUM_ROOT/bin
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$MAUM_ROOT/lib
export NGINX_BIN=/usr/sbin/nginx

touch /logs/maum/batch/system.log
python $MAUM_ROOT/code/etm_e_cqa_batch.py -f $INPUT_1 -d $INPUT_2 >> /logs/maum/batch/system.log
