# Pip installing
FROM python:3.8

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

COPY ./ .
RUN pip install --upgrade pip && pip install -r ./src/requirements.txt
WORKDIR ./src

ENV PYTHONPATH "${PYTHONPATH}:./src"
CMD ["/bin/bash"]


