FROM openjdk:8

# Install sendmail utils
RUN apt-get update && apt-get install -y mailutils
RUN apt-get update && apt-get install -y sendmail

# Add the build artifact under /opt, can be overridden by docker build
ARG ARTIFACT_PATH=target/doctorkafka-0.2.4.8-bin.tar.gz
ADD $ARTIFACT_PATH /opt/doctorkafka/
# default cmd
CMD /opt/doctorkafka/scripts/run_in_container.sh
