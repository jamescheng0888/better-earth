FROM eclipse-temurin:17-jdk

WORKDIR /app
RUN javac --add-modules jdk.httpserver OrbitServer.java

ENV HOST=0.0.0.0
EXPOSE 10000

CMD ["java", "--add-modules", "jdk.httpserver", "OrbitServer"]
