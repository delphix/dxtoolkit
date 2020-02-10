#!/bin/bash

# TODO: replace encryption key 

export DELPHIX_OUTPUT=/github/workspace/dxtoolkit2
export DXTOOLKIT_CONF=/github/workspace/test/dxtools.conf
mkdir $DELPHIX_OUTPUT

source scl_source enable rh-perl526 

echo "run tests"

cd /github/workspace/test
chmod +x ./runtest.sh
./runtest.sh

if [[ $? -ne 0 ]]; then
    exit 1;
fi

cd /github/workspace/lib
mv dbutils.pm dbutils.orig.pm
cat dbutils.orig.pm | sed -e "s/put your encryption key here/${INPUT_ENCKEY}/" > dbutils.pm

cd /github/workspace/bin
pp -u -I /github/workspace/lib -l /usr/lib64/libcrypto.so -l /usr/lib64/libssl.so -M Text::CSV_PP -M List::MoreUtils::PP -M Crypt::Blowfish  \
      -F Crypto=dbutils\.pm$ -M Filter::Crypto::Decrypt -o $DELPHIX_OUTPUT/runner `ls dx_*.pl | xargs`

cd $DELPHIX_OUTPUT
#for i in /github/workspace/bin/dx_*.pl ; do name=`basename -s .pl $i`; ln -s runner $name; done


echo #!/bin/bash > install.sh
echo LIST_OF_SCRIPTS=\( >> install.sh

for i in /github/workspace/bin/dx_*.pl ; do
    name=`basename -s .pl $i`;
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
