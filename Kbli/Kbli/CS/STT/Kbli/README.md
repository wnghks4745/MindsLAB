CS_daemon.py
 - usage: CS_daemon.py [-h] -ct {dev,uat,prd} -p {stop,start,restart}
 - optional arguments:
    -h, --help                show this help message and exit
    -ct {dev,uat,prd}         dev or uat or prd
    -p {stop,start,restart}   stop or start or restart

daemon 시작
 - python CS_daemon.py -ct dev -p start
daemon 종료
 - python CS_daemon.py -ct dev -p stop
daemon 재시작
 - python CS_daemon.py -ct dev -p restart


==================================================================================================
monitoring.py
 - usage: monitoring.py [-h] -ct {dev,uat,prd} [-t {CS,TM,CD}] [-date DATE] [-detail {Y,N}]
 - optional arguments:
    -h, --help         show this help message and exit
    -ct {dev,uat,prd}  dev or uat or prd
    -t {CS,TM,CD}      CS or TM or CD
    -date DATE         YYYY-MM-DD
    -detail {Y,N}      Y or N

CS STT 전체 날짜 상태 조회
 - python monitoring.py -ct dev -t CS
CS STT 지정 날짜 상태 조회
 - python monitoring.py -ct dev -t CS -date 2018-01-01
CS STT 지정 날짜 상태 디테일 조회 (에러 데이터의 call_meta 정보 조회 (document_id, rec_id))
 - python monitoring.py -ct dev -t CS -date 2018-01-01 -detail Y