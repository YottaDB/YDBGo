#################################################################
#								#
# Copyright (c) 2019-2023 YottaDB LLC and/or its subsidiaries.	#
# All rights reserved.						#
#								#
#   This source code contains the intellectual property		#
#   of its copyright holder(s), and is made available		#
#   under a license.  If you do not know the terms of		#
#   the license, please stop and do not read further.		#
#								#
#################################################################

FROM yottadb/yottadb-base:latest-master

WORKDIR /data

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
                    wget \
                    ca-certificates \
                    git \
                    g++ \
                    gcc \
                    libc6-dev \
                    make \
                    pkg-config \
                    && \
    rm -rf /var/lib/apt/lists/*

# Install go
ENV GOPATH /go
RUN mkdir /go
ENV GOLANG_VERSION 1.13.8
ENV PATH=$GOPATH/bin:/usr/local/go/bin:$PATH
RUN wget -O go.tgz -q https://golang.org/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go.tgz && \
    rm go.tgz

# Setup YottaDB
ENV ydb_dir /data
ENV ydb_dist /opt/yottadb/current
