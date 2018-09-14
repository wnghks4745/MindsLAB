# 카드사 서버 정보
 - wav 파일 경로는 /home/ftpuser06/WAVE_LIST
# 카드사 파일정보
 - 20160511163357_20160511164024_1052603_황지연_짱치브덜거르마_26543391_IN07_MD05727_20160511174208.wav 이런 형태의 파일로 존재
 - 통화시작시간_통화종료시간_상담원사번_상담원_고객명_고객아이디_지점코드_내부적으로 쓰는 사번(?)_날짜 날짜는 어떤 날짜인지 정확히 모름.


1. 카드사 서버에서 파일을 가져온다.

2. 1에서 가져온 파일에 파일명에 메타 정보가 있는데 CALL_META 테이블에 정보를 넣는다.
    - 이미 가져온 정보가 있는 동일 파일은 무시
    - PROJECT_CD : 'CD' 카드사는 'CD'로 통일
    - DOCUMENT_DT : 통화시작시간
    - DOCUMENT_ID : 3에서 변경한 파일명  -  VARCHAR2(40 BYTE) 이니 참고
    - CALL_TYPE : 1                      - 0 인바운드 / 1 아웃바운드
    - AGENT_ID : 사번
    - BRANCH_CD : 지점코드
    - CALL_DT : 통화시작시간의 날짜
    - START_DTM : 통화시작시간
    - END_DTM : 통화종료시간
    - DURATION : 통화종료시간             - 통화시작시간
    - CHN_TP : M                          - 모노
    - REC_ID : sysdate 밀리세컨_CARD 형태로 생성 ex) 20180517115635139927_CARD
    - CUSTOMER_NM : 고객명
    - AGENT_NM : 상담원명

3. 파일명을 변경한다. (한글명이 있어 변경이 필요함)
    - CALL_META 테이블의 document_id 와 같은 형태로 변경 해야함.
    - 변경한 파일명을 CALL_META 테이블의 document_id 에도 저장해야한다.

4. 파일은 통화시작시간 기준으로 년/월/일 폴더구조로 저장
    - /app/rec_server/prd/cardTM/2010/01/01


실행 arguments
-ct : dev or uat or prd
    dev : 개발 / uat : UAT / prd : 운영

BATCH JOB - crontab
* * * * * /usr/bin/flock -w 1 /tmp/sftp_transport.lockfile python /스크립트 절대경로/sftp_transport.py -ct dev >> /dev/null