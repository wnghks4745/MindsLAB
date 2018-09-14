#!/bin/sh

LOG_SENDER_HOME=/damo

LOG_SENDER_BIN_DIR=${LOG_SENDER_HOME}
LOG_SENDER_CONF_DIR=${LOG_SENDER_HOME}
LOG_SENDER_LOG_DIR=${LOG_SENDER_HOME}

LOG_SENDER_BIN=${LOG_SENDER_BIN_DIR}/logSender
LOG_SENDER_CONF=${LOG_SENDER_CONF_DIR}/damo_lms.conf
LOG_SENDER_LOG=${LOG_SENDER_LOG_DIR}/LogSender.log
LOG_SENDER_ERR=${LOG_SENDER_LOG_DIR}/LogSender.err
LOG_SENDER_PID=${LOG_SENDER_HOME}/.LogSender.pid

if [ -r "${LOG_SENDER_PID}" ] 
then
	echo "Stopping LogSender."
	sender_pid=`cat ${LOG_SENDER_PID}`
	kill $sender_pid 2>/dev/null
fi 

nohup ${LOG_SENDER_BIN} -i ${LOG_SENDER_CONF} > ${LOG_SENDER_LOG} 2> ${LOG_SENDER_ERR} < /dev/null &

sender_pid=$!

sleep 1

ps -p $sender_pid > /dev/null 2> /dev/null
if [ $? = 0 ];
then
	echo "Starting LogSender."
	echo $sender_pid > ${LOG_SENDER_PID}
else
	echo "Failed to start LogSender."
	cat ${LOG_SENDER_LOG}
	rm ${LOG_SENDER_PID} > /dev/null 2> /dev/null
fi

