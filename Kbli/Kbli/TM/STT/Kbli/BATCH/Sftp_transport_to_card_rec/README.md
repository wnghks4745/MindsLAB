# ī��� ���� ����
 - wav ���� ��δ� /home/ftpuser06/WAVE_LIST
# ī��� ��������
 - 20160511163357_20160511164024_1052603_Ȳ����_¯ġ����Ÿ���_26543391_IN07_MD05727_20160511174208.wav �̷� ������ ���Ϸ� ����
 - ��ȭ���۽ð�_��ȭ����ð�_�������_����_����_�����̵�_�����ڵ�_���������� ���� ���(?)_��¥ ��¥�� � ��¥���� ��Ȯ�� ��.


1. ī��� �������� ������ �����´�.

2. 1���� ������ ���Ͽ� ���ϸ� ��Ÿ ������ �ִµ� CALL_META ���̺� ������ �ִ´�.
    - �̹� ������ ������ �ִ� ���� ������ ����
    - PROJECT_CD : 'CD' ī���� 'CD'�� ����
    - DOCUMENT_DT : ��ȭ���۽ð�
    - DOCUMENT_ID : 3���� ������ ���ϸ�  -  VARCHAR2(40 BYTE) �̴� ����
    - CALL_TYPE : 1                      - 0 �ιٿ�� / 1 �ƿ��ٿ��
    - AGENT_ID : ���
    - BRANCH_CD : �����ڵ�
    - CALL_DT : ��ȭ���۽ð��� ��¥
    - START_DTM : ��ȭ���۽ð�
    - END_DTM : ��ȭ����ð�
    - DURATION : ��ȭ����ð�             - ��ȭ���۽ð�
    - CHN_TP : M                          - ���
    - REC_ID : sysdate �и�����_CARD ���·� ���� ex) 20180517115635139927_CARD
    - CUSTOMER_NM : ����
    - AGENT_NM : ������

3. ���ϸ��� �����Ѵ�. (�ѱ۸��� �־� ������ �ʿ���)
    - CALL_META ���̺��� document_id �� ���� ���·� ���� �ؾ���.
    - ������ ���ϸ��� CALL_META ���̺��� document_id ���� �����ؾ��Ѵ�.

4. ������ ��ȭ���۽ð� �������� ��/��/�� ���������� ����
    - /app/rec_server/prd/cardTM/2010/01/01


���� arguments
-ct : dev or uat or prd
    dev : ���� / uat : UAT / prd : �

BATCH JOB - crontab
* * * * * /usr/bin/flock -w 1 /tmp/sftp_transport.lockfile python /��ũ��Ʈ ������/sftp_transport.py -ct dev >> /dev/null