# Use an official Ubuntu as a parent image
FROM ubuntu:20.04

# Set environment variables for non-interactive installation of software
ENV DEBIAN_FRONTEND=noninteractive

# Update package list and install essential packages
RUN apt-get update && apt-get install -y \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/* \

# Add the deadsnakes PPA to install Python 3.8
RUN add-apt-repository ppa:deadsnakes/ppa

# Update package list again and install Python 3.8
RUN apt-get update && apt-get install -y \
    python3.8 \
    python3.8-dev \
    python3.8-distutils \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install the python requirements
COPY ./ .
RUN pip install --upgrade pip && pip install -r ./src/requirements.txt
WORKDIR ./src

# Install OpenJDK 11
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk

# Set Python 3.8 as the default Python version
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin
ENV PYTHONPATH "${PYTHONPATH}:./src"

# Cleanup unnecessary packages and cache
RUN apt-get autoremove -y && apt-get clean && rm -rf /var/lib/apt/lists/*

# Default command when the container runs
CMD ["bash"]
