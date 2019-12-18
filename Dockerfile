FROM openjdk:8-jre-alpine
ENV JAVA_APP=/app.jar
ADD build/libs/service-cleaner.jar $JAVA_APP
RUN sh -c 'touch $JAVA_APP'
ENTRYPOINT exec java $JAVA_OPTS -jar $JAVA_APP
EXPOSE 2020