#!/bin/sh
APP_NAME=algorithm-replenishment
JAR=$APP_NAME-start.jar

APP_DIR=/data/server/$APP_NAME
MAIN_JAR=$APP_DIR/boot/$JAR
LOGS=$APP_DIR/logs
JAVA_OPTS=""

LOCAL_IP=`ifconfig eth0 | grep "inet" | awk '{ print $2}'`
echo "Current ip: ${LOCAL_IP}"

if [[ $LOCAL_IP =~ ^172.21.* ]]; then
  JAVA_OPTS="-Xms256m -Xmx512m -XX:+UseG1GC"
else
  JAVA_OPTS="-Xms2048m -Xmx2048m -XX:+UseG1GC"
fi

PIDFILE=$APP_DIR/service.pid

OPER=$1

start_app(){ 
	cd $APP_DIR
  if [ ! -d "$LOGS" ]; then
   		mkdir $LOGS
	fi
	chmod 775 -R $APP_DIR
	if [ -f "$PIDFILE" ]; then
	    echo "Service is already start ..."
	else
	    echo "Service  start ..."
	    nohup java $JAVA_OPTS -jar $MAIN_JAR 1> $LOGS/console.out 2>&1  &  printf '%d' $! > $PIDFILE
	    echo "Service  start SUCCESS "
	fi
}
stop_app(){
	cd $APP_DIR
  if [ -f "$PIDFILE" ]; then
    	kill -9 `cat $PIDFILE`
    	rm -rf $PIDFILE
    	echo "Service is stop SUCCESS!"
	else
	    echo "Service is already stop ..."
	fi
}
case $OPER in
  start)
        start_app
        ;;
  stop)
        stop_app
        ;;
  restart) 
        stop_app
        sleep 5
        start_app
        ;;
esac
