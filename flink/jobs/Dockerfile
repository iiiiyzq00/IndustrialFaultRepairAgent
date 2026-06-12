FROM maven:3.9-eclipse-temurin-11 AS builder
WORKDIR /app
COPY flink/jobs/pom.xml .
RUN mvn dependency:go-offline -B
COPY flink/jobs/src/ src/
RUN mvn package -DskipTests -B
FROM eclipse-temurin:11-jre
WORKDIR /app
COPY --from=builder /app/target/industrial-fault-detection-1.0.0.jar /app/job.jar
CMD ["echo", "JAR built at /app/job.jar"]
