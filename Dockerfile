FROM perl:5.28 as builder

RUN  apt-get update \
     && apt-get install -y libnet-ssleay-perl libcrypt-ssleay-perl libwww-perl \
     && cpanm JSON   Date::Manip   DateTime::Event::Cron::Quartz   DateTime::Format::DateParse \
        Crypt::CBC   Crypt::Blowfish   Text::CSV   Try::Tiny   LWP::UserAgent   Net::SSLeay   \
        IO::Socket::SSL   LWP::Protocol::https     Filter::Crypto::Decrypt   PAR::Packer   \
        Term::ReadKey   Log::Syslog::Fast \
     && mkdir /app

ADD lib /app/lib
ADD bin /app/bin

WORKDIR /app/bin

RUN for script in $(ls dx*.pl); do \
     pp  -u -l /usr/lib/x86_64-linux-gnu/libcrypto.so.1.1 -l /usr/lib/x86_64-linux-gnu/libssl.so.1.1 -I ../lib/  -M Crypt::Blowfish  -F Crypto=dbutils.pm -M Filter::Crypto::Decrypt -o $(echo $script | cut -d\. -f1)  $script; \
     done

RUN rm -f /app/bin/*.pl
 


FROM ubuntu:16.04

RUN mkdir -p /app/bin

COPY --from=builder /app/bin /app/bin

ENV PATH=$PATH:/app/bin/

WORKDIR /app/bin/

CMD ["/bin/bash"]
