카드사 녹취 파일에서 메타정보를 추출해서 CALL_META TABLE에 저장하는 스크립트

crontab로 등록되어있음.
* * * * * /usr/bin/flock -w 1 /tmp/insert.lockfile python /스크립트 절대경로/insert_card_rec_meta.py -ct dev >> /dev/null