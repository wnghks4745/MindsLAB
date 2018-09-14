# 환경설정
vi ~/.bashrc
export DAMO=/app/prd/MindsVOC/damo/cfg/64:/home/minds/diamo/64
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${DAMO}
source ~/.bashrc

# 실행방법
- 문자열 암호화
python damo.py -type str -p enc -data test
- 문자열 복호화
python damo.py -type str -p dec -data 7B57CA6DA7912541239153156CBCF037

- 파일 암호화
python damo.py -type file -p enc -data /대상파일경로 /암호화파일경로
- 파일 복호화
python damo.py -type file -p dec -data /암호화파일경로 /복호화파일경로
