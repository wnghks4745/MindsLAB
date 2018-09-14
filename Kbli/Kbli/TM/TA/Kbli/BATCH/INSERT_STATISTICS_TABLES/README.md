TA 통계 저장 스크립트

crontab로 등록되어있음.
0 1 * * * python /app/prd/MindsVOC/TM/TA/Kbli/BATCH/INSERT_STATISTICS_TABLES/Insert_statistics_tables.py -ct prd

실행 arguments
-ct : dev or uat or prd
    dev : 개발 / uat : UAT / prd : 운영

ex)
python Insert_statistics_tables.py -ct prd