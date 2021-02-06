FROM openjdk:8

# Install sendmail utils
RUN apt-get update && apt-get install -y mailutils
RUN apt-get update && apt-get install -y sendmail

# Add the build artifact under /opt, can be overridden by docker build
ARG ARTIFACT_PATH=target/doctork-0.3.0-rc.3-bin.tar.gz
ADD $ARTIFACT_PATH /opt/doctork/
# default cmd
CMD /opt/doctork/scripts/run_in_container.sh
