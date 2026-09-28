# Build stage
FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /app
COPY . .
RUN mvn clean package -DskipTests

# Run stage with Java and ZooKeeper
FROM eclipse-temurin:17-jre
RUN apt-get update && apt-get install -y zookeeper zookeeperd && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app /app
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

EXPOSE 8080 2181

CMD ["/app/start.sh"]
