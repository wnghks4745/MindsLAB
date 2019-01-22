export MAUM_ROOT=/data1/maum
export PATH=$PATH:/usr/local/cuda/bin:$MAUM_ROOT/bin
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$MAUM_ROOT/lib
export NGINX_BIN=/usr/sbin/nginx

touch /logs/maum/batch/system.log
python $MAUM_ROOT/code/summary_batch.py -t D >> /logs/maum/batch/system.log
