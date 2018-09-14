/app/prd/MindsVOC/TM/TA/Kbli/DB_SCRIPT/TB_SCRT_DB_Script

실행 arguments
-ct : dev or uat or prd
    dev : 개발 / uat : UAT / prd : 운영

-ct dev (개발) , -ct uat (UAT), -ct prd (운영에 반영할때는 TA배치(TM_QA_TA_Daemon.py) 정지 후 반영)

python Insert_script.py -ct dev -insert TB_SCRT_SEC_INFO -f TB_SCRT_SEC_INFO.txt

python Insert_script.py -ct dev -delete TB_SCRT_SEC_INFO
