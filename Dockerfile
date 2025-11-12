#################################################################
#								#
# Copyright (c) 2019-2025 YottaDB LLC and/or its subsidiaries.	#
# All rights reserved.						#
#								#
#   This source code contains the intellectual property		#
#   of its copyright holder(s), and is made available		#
#   under a license.  If you do not know the terms of		#
#   the license, please stop and do not read further.		#
#								#
#################################################################
# Build this Dockerfile with:
# docker build --progress=plain -f Dockerfile -t ydbgo .

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
		    vim \
                    && \
    rm -rf /var/lib/apt/lists/*

# Install go
ENV GOPROXY=https://proxy.golang.org/cached-only
ENV GOPATH=/go
RUN mkdir /go
ENV GOLANG_VERSION=1.25.4
ENV PATH=$GOPATH/bin:/usr/local/go/bin:$PATH
RUN rm -rf /usr/local/go && wget -O - https://go.dev/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz | tar -C /usr/local -xz
RUN go version

# Setup YottaDB
ENV ydb_dir=/data
ENV ydb_dist=/opt/yottadb/current
ENV ydb_chset=UTF-8
