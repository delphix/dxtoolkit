FROM centos:6

RUN yum install -y centos-release-scl \
    && yum install -y rh-perl524 \
    && yum install -y rh-perl524-perl-JSON-PP.noarch \
    && yum install -y rh-perl524-perl-App-cpanminus.noarch \
    && yum install -y rh-perl524-perl-Try-Tiny.noarch \
    && yum install -y rh-perl524-perl-DateTime.x86_64 \
    && yum install -y rh-perl524-perl-Storable.x86_64 \
    && yum install -y rh-perl524-perl-Test.noarch \
    && yum install -y rh-perl524-perl-Test-Simple.noarch \
    && yum install -y rh-perl524-perl-ExtUtils-Embed.noarch \
    && yum install -y rh-perl524-perl-open.noarch \
    && yum install -y openssl-devel.x86_64 \
    && yum install -y openssl \
    && yum install -y gcc 

RUN source scl_source enable rh-perl524 \
    && cpanm JSON@4.02 \
    && cpanm JSON::PP@4.04 \
    && cpanm Date::Manip@6.79 \
    # force requires after 1/1/2020 ?
    && cpanm -f Date::Parse@2.30 \
    && cpanm DateTime::Event::Cron::Quartz@0.05 \
    && cpanm DateTime::Format::DateParse@0.05 \
    && cpanm Crypt::CBC@2.33 \
    && cpanm Crypt::Blowfish@2.14 \
    && cpanm Text::CSV@2.00 \
    && cpanm LWP::UserAgent@6.43 \
    && cpanm Net::SSLeay@1.88 \
    && cpanm IO::Socket::SSL@2.066 \
    && cpanm LWP::Protocol::https@6.07 \
    && cpanm Term::ReadKey@2.38 \
    && cpanm Log::Syslog::Fast@0.67 \
    && cpanm PAR::Packer@1.049 \
    && cpanm Filter::Crypto::Decrypt@2.08 \
    && cpanm List::MoreUtils::PP@0.428 

RUN mkdir -p /github/workspace
RUN mkdir -p /github/home
RUN mkdir -p /github/workflow
COPY entrypoint.sh /entrypoint.sh
WORKDIR /github/workspace

ENTRYPOINT ["/entrypoint.sh"]
