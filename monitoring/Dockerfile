FROM openjdk:15

WORKDIR /app

COPY ./target/monitoring-1.0-SNAPSHOT-jar-with-dependencies.jar .

EXPOSE 6660

ENTRYPOINT ["java", "-jar", "monitoring-1.0-SNAPSHOT-jar-with-dependencies.jar"]
