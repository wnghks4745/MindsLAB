TM_QA_TA_Daemon.py
 - usage: TM_QA_TA_Daemon.py [-h] -ct {dev,uat,prd} -p {stop,start,restart}
 - optional arguments:
  -h, --help                show this help message and exit
  -ct {dev,uat,prd}         dev or uat or prd
  -p {stop,start,restart}   stop or start or restart

daemon 시작
 - python TM_QA_TA_Daemon.py -ct dev -p start
daemon 종료
 - python TM_QA_TA_Daemon.py -ct dev -p stop
daemon 재시작
 - python TM_QA_TA_Daemon.py -ct dev -p restart


==================================================================================================
monitoring.py
 - usage: monitoring.py [-h] -ct {dev,uat,prd} [-date DATE] [-detail {Y,N}]
 - optional arguments:
  -h, --help         show this help message and exit
  -ct {dev,uat,prd}  dev or uat or prd
  -date DATE         YYYYMMDD
  -detail {Y,N}      Y or N


TA 전체 날짜 상태 조회
 - python monitoring.py -ct dev
TA 지정 날짜 상태 조회
 - python monitoring.py -ct dev -date 20180101
TA 지정 날짜 상태 디테일 조회 (에러 데이터 정보 조회)
 - python monitoring.py -ct dev -date 20180101 -detail Y