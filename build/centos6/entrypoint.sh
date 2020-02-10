#!/bin/bash

export DELPHIX_OUTPUT=/github/workspace/dxtoolkit2
mkdir $DELPHIX_OUTPUT

source scl_source enable rh-perl524

cd /github/workspace/lib
mv dbutils.pm dbutils.orig.pm
cat dbutils.orig.pm | sed -e "s/put your encryption key here/${INPUT_ENCKEY}/" > dbutils.pm
ls -l dbutils*

cd /github/workspace/bin
pp -u -I /github/workspace/lib -l /usr/lib64/libcrypto.so -l /usr/lib64/libssl.so -M Text::CSV_PP -M List::MoreUtils::PP -M Crypt::Blowfish  \
      -F Crypto=dbutils\.pm$ -M Filter::Crypto::Decrypt -o $DELPHIX_OUTPUT/runner `ls dx_*.pl | xargs`

cd $DELPHIX_OUTPUT
#for i in /github/workspace/bin/dx_*.pl ; do name=`basename $i .pl`; ln -s runner $name; done

echo #!/bin/bash > install.sh
echo LIST_OF_SCRIPTS=\( >> install.sh

for i in /github/workspace/bin/dx_*.pl ; do
    name=`basename $i .pl`;
    echo $name >> install.sh
done

echo \) >> install.sh
echo >> install.sh
echo >> install.sh
echo for i in \"\$\{LIST_OF_SCRIPTS\[\@\]\}\" >> install.sh
echo do >> install.sh
echo   echo \$i >> install.sh
echo   ln -sf runner \$i >> install.sh
echo done >> install.sh

cd /github/workspace
tar czvf /github/workspace/dxtoolkit.tar.gz dxtoolkit2/

echo ${HOME}

cp /github/workspace/dxtoolkit.tar.gz ${HOME}

ls -l ${HOME}
