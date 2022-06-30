FROM oraclelinux:8

RUN dnf config-manager --set-enabled ol8_codeready_builder
RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm

RUN yum install -y perl \
                  tar \
                  perl-JSON-PP.noarch \
                  perl-Date-Manip.noarch \
                  perl-Net-SSLeay.x86_64 \
                  git \
                  perl-JSON-PP.noarch \
                  perl-JSON.noarch \
                  perl-LWP-Protocol-https.noarch \
                  perl-Crypt-CBC.noarch \
                  perl-Crypt-Blowfish.x86_64 \
                  perl-App-cpanminus.noarch \
                  perl-DateTime.x86_64 \
                  perl-DateTime-Locale.noarch \
                  perl-App-cpanminus.noarch \
                  perl-ExtUtils-Config.noarch \
                  perl-ExtUtils-InstallPaths.noarch \
                  perl-ExtUtils-Helpers.noarch \
                  perl-Module-Build-Tiny.noarch \
                  perl-DateTime-Format-DateParse.noarch \
                  perl-Test-Script.noarch \
                  perl-HTTP-Server-Simple.noarch \
                  perl-Log-Log4perl.noarch \
                  perl-Text-CSV.noarch \
                  perl-Text-CSV_XS.x86_64 \
                  perl-PAR-Packer.x86_64 \
                  perl-List-MoreUtils.noarch \
                  perl-List-MoreUtils-XS.x86_64 \
                  openssl-devel


RUN cpanm -f DateTime::Event::Cron::Quartz

RUN cpanm Test::Files
RUN cpanm Filter::Crypto::Decrypt


RUN mkdir -p /github/workspace
RUN mkdir -p /github/home
RUN mkdir -p /github/workflow
COPY entrypoint.sh /entrypoint.sh
WORKDIR /github/workspace

ENTRYPOINT ["/entrypoint.sh"]
