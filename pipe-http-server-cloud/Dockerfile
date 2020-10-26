FROM library/openjdk:8-jre

MAINTAINER Technology_Retail_PriceService_UK_5 <Technology_Retail_PriceService_UK_5@tesco.com>

RUN mkdir -p /opt/aqueduct/pipe
RUN mkdir -p /etc/aqueduct/pipe

# Copy config and binaries
COPY build/libs/pipe-http-server-cloud*-all.jar /opt/aqueduct/pipe/

# Creating Aqueduct Pipe user
RUN useradd --create-home -s /bin/bash aqueductpipe

WORKDIR /home/aqueduct/pipe

# Change ownership of aqueduct pipe
RUN chown -R aqueductpipe /opt/aqueduct/pipe
RUN chown -R aqueductpipe /etc/aqueduct/pipe
RUN chown -R aqueductpipe /var/log

# Switch to Aqueduct Pipe user
USER aqueductpipe

ENV BIN_ROOT /opt/aqueduct/pipe

# Run Aqueduct Pipe
ENTRYPOINT ["/bin/bash", "-c", "java -Xmx10000m -Xms10000m -XX:+UseG1GC -jar /opt/aqueduct/pipe/pipe-http-server-cloud-*all.jar"]

