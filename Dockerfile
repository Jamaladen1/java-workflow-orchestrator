# ---- 1. build the React Flow UI ----
FROM node:22-alpine AS ui
WORKDIR /ui
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

# ---- 2. build the Spring Boot jar (UI baked into static resources) ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml ./
RUN mvn -q -B dependency:go-offline
COPY src ./src
COPY --from=ui /ui/dist ./src/main/resources/static
RUN mvn -q -B -DskipTests package

# ---- 3. runtime ----
FROM eclipse-temurin:21-jre
RUN groupadd -r app && useradd -r -g app -u 10001 app
WORKDIR /app
COPY --from=build /app/target/app.jar app.jar
USER app
EXPOSE 8090
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
