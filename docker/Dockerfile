FROM ubuntu:18.04 as builder

RUN apt-get update \
    && apt-get install -y libpar-packer-perl \
    && apt-get install -y libjson-pp-perl \
    && apt-get install -y libdate-manip-perl \
    && apt-get install -y libdatetime-format-dateparse-perl \
    && apt-get install -y libcrypt-cbc-perl \
    && apt-get install -y libcrypt-blowfish-perl \
    && apt-get install -y libtext-csv-perl \
    && apt-get install -y libtry-tiny-perl \
    && apt-get install -y libparse-http-useragent-perl \
    && apt-get install -y libnet-ssleay-perl \
    && apt-get install -y libio-socket-ssl-perl \
    && apt-get install -y libfilter-perl \
    && apt-get install -y libterm-readkey-perl \
    && apt-get install -y cpanminus \
    && apt-get install -y make \
    && apt-get install -y gcc \
    && apt-get install -y libssl-dev \
    && apt-get install -y libwww-perl \
    && cpanm DateTime::Event::Cron::Quartz \
    && cpanm Log::Syslog::Fast@0.65 \
    && cpanm Filter::Crypto::Decrypt 

ARG INPUT_ENCKEY
ENV INSIDE_INPUT_ENCKEY=$INPUT_ENCKEY
RUN apt-get install -y git
RUN mkdir -p /github
RUN mkdir -p /dxtoolkit
RUN cd /github; git clone https://github.com/delphix/dxtoolkit.git
RUN cd /github/dxtoolkit/lib \
    && mv dbutils.pm dbutils.orig.pm \
    && cat dbutils.orig.pm | sed -e "s/put your encryption key here/$INSIDE_INPUT_ENCKEY/" > dbutils.pm

RUN cd /github/dxtoolkit/bin \
    && pp -u -I /github/dxtoolkit/lib -l /usr/lib/x86_64-linux-gnu/libcrypto.so.1.1 -l /usr/lib/x86_64-linux-gnu/libssl.so.1.1 -M Text::CSV_PP -M List::MoreUtils::PP -M Crypt::Blowfish  \
      -F Crypto=dbutils\.pm$ -M Filter::Crypto::Decrypt -o /dxtoolkit/runner `ls dx_*.pl | xargs`


RUN cd /dxtoolkit && \
    for i in /github/dxtoolkit/bin/dx_*.pl ; do \
      name=`basename -s .pl $i`; \
      ln -sf runner $name; \
    done 

# ENV PATH=$PATH:/dxtoolkit
# WORKDIR /dxtoolkit
# ENV DXTOOLKIT_CONF=/config/dxtools.conf
# CMD tail -f /dev/null

FROM ubuntu:18.04

RUN mkdir /dxtoolkit
COPY --from=builder /dxtoolkit /dxtoolkit
WORKDIR /dxtoolkit

ENV PATH=$PATH:/dxtoolkit
WORKDIR /dxtoolkit
ENV DXTOOLKIT_CONF=/config/dxtools.conf
CMD tail -f /dev/null




