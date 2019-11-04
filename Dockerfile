FROM openjdk:8-jre-alpine

ENV JAVA_OPTS="-Xmx8096m -Djava.security.egd=file:/dev/./urandom"
ENV JAVA_APP=/app.jar

RUN echo $'#!/bin/sh \n\
exec java $JAVA_OPTS -jar $JAVA_APP' > /start.sh && chmod +x /start.sh

VOLUME /tmp
ADD build/libs/service-cleaner.jar $JAVA_APP

RUN sh -c 'touch $JAVA_APP'

EXPOSE 2020

ENTRYPOINT ["/start.sh"]