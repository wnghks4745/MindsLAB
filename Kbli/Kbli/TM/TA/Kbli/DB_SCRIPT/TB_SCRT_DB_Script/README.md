/app/prd/MindsVOC/TM/TA/Kbli/DB_SCRIPT/TB_SCRT_DB_Script

���� arguments
-ct : dev or uat or prd
    dev : ���� / uat : UAT / prd : �

-ct dev (����) , -ct uat (UAT), -ct prd (��� �ݿ��Ҷ��� TA��ġ(TM_QA_TA_Daemon.py) ���� �� �ݿ�)

python Insert_script.py -ct dev -insert TB_SCRT_SEC_INFO -f TB_SCRT_SEC_INFO.txt

python Insert_script.py -ct dev -delete TB_SCRT_SEC_INFO
