ARG BASE_IMAGE=rockylinux:9
FROM $BASE_IMAGE

# Install required packages for testing
RUN dnf install -y --allowerasing \
    gcc \
    gcc-c++ \
    make \
    openssl \
    openssl-devel \
    libffi-devel \
    redhat-rpm-config \
    java-11-openjdk \
    git \
    curl \
    wget \
    tar \
    gzip \
    && dnf clean all

# Install .NET SDK
# Add Microsoft repository for .NET packages
RUN rpm -Uvh https://packages.microsoft.com/config/rhel/9/packages-microsoft-prod.rpm

# Install .NET SDK (includes multiple target frameworks)
RUN dnf install -y dotnet-sdk-6.0 dotnet-sdk-7.0 dotnet-sdk-8.0 dotnet-sdk-9.0 && dnf clean all

# Verify .NET installation
RUN dotnet --version && \
    dotnet --list-sdks && \
    dotnet --list-runtimes

# This is to solve permission issue, read https://denibertovic.com/posts/handling-permissions-with-docker-volumes/
ARG GOSU_URL=https://github.com/tianon/gosu/releases/download/1.14/gosu-amd64
ENV GOSU_PATH=${GOSU_URL}
RUN curl -o /usr/local/bin/gosu -SL $GOSU_PATH
RUN chmod +x /usr/local/bin/gosu

COPY ../../../image/scripts/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

WORKDIR /home/user
RUN chmod 777 /home/user

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

