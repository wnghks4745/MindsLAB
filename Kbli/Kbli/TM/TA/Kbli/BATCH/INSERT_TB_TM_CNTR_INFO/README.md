TA 현업 운영 데이터 -> ZSTT DB로 저장

crontab로 등록되어있음.
0 1 * * * python /app/prd/MindsVOC/TM/TA/Kbli/BATCH/INSERT_TB_TM_CNTR_INFO/Insert_tb_tm_cntr_info.py -ct prd

실행 arguments
-ct : dev or uat or prd
    dev : 개발 / uat : UAT / prd : 운영

ex)
python Insert_tb_tm_cntr_info.py -ct prd
