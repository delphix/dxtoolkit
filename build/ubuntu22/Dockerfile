FROM ubuntu:22.04

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

RUN mkdir -p /github/workspace
RUN mkdir -p /github/home
RUN mkdir -p /github/workflow
COPY entrypoint.sh /entrypoint.sh
WORKDIR /github/workspace
RUN chmod +x /entrypoint.sh


ENTRYPOINT ["/entrypoint.sh"]



