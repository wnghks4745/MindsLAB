export LANG=ko_KR.UTF-8
export NLS_LANG=Korean_Korea.KO16KSC5601
export DAMO=/app/prd/MindsVOC/damo/cfg/64
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${DAMO}
/usr/bin/flock -w 1 /tmp/encrypt.lockfile /usr/bin/python /app/prd/MindsVOC/get_view_data/get_view_data.py >> /dev/null
