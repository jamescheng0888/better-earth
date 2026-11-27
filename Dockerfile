FROM eclipse-temurin:17-jdk

WORKDIR /app
COPY . /tmp/source
RUN SRC=$(find /tmp/source -name OrbitServer.java | head -n 1) \
 && cp "$SRC" /app/OrbitServer.java \
 && cp /tmp/source/index.html /app/index.html \
 && javac --add-modules jdk.httpserver OrbitServer.java

ENV HOST=0.0.0.0
EXPOSE 10000

CMD ["java", "--add-modules", "jdk.httpserver", "OrbitServer"]
