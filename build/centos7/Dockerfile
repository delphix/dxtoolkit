FROM centos:7

RUN yum install -y centos-release-scl \
    && yum install -y rh-perl526 \
    && yum install -y rh-perl526-perl-JSON-PP.noarch \
    && yum install -y rh-perl526-perl-App-cpanminus.noarch \
    && yum install -y rh-perl526-perl-Try-Tiny.noarch \
    && yum install -y rh-perl526-perl-DateTime.x86_64 \
    && yum install -y openssl-devel.x86_64 \
    && yum install -y openssl \
    && yum install -y gcc \
    && source scl_source enable rh-perl526 \
    && cpanm JSON@4.02 \
    && cpanm JSON::XS@4.02 \
    && cpanm Date::Manip \
    && cpanm DateTime::Event::Cron::Quartz \
    && cpanm DateTime::Format::DateParse \
    && cpanm Crypt::CBC \
    && cpanm Crypt::Blowfish \
    && cpanm Text::CSV \
    && cpanm LWP::UserAgent \
    && cpanm Net::SSLeay \
    && cpanm IO::Socket::SSL \
    && cpanm LWP::Protocol::https \
    && cpanm Term::ReadKey \
    && cpanm Log::Syslog::Fast \
    && cpanm Filter::Crypto::Decrypt \
    && cpanm PAR::Packer \
    && cpanm List::MoreUtils::PP \
    && cpanm Test::More \
    && cpanm Test::Script \
    && cpanm Test::Files \
    && cpanm HTTP::Server::Simple::CGI \
    && cpanm Log::Log4perl

RUN mkdir -p /github/workspace
RUN mkdir -p /github/home
RUN mkdir -p /github/workflow
COPY entrypoint.sh /entrypoint.sh
WORKDIR /github/workspace

ENTRYPOINT ["/entrypoint.sh"]
