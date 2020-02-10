@echo off


set PATH=d:\a\perl\perl\site\bin;d:\a\perl\perl\bin;d:\a\perl\c\bin;%PATH%
set TERM=
set PERL_JSON_BACKEND=
set PERL_YAML_BACKEND=
set PERL5LIB=
set PERL5OPT=
set PERL_MM_OPT=
set PERL_MB_OPT=

echo ----------------------------------------------
echo  Welcome to Strawberry Perl Portable Edition!
echo  * URL - http://www.strawberryperl.com/
echo  * see README.TXT for more info
echo ----------------------------------------------
perl -MConfig -e "printf("""Perl executable: %%s\nPerl version   : %%vd / $Config{archname}\n\n""", $^X, $^V)" 2>nul
if ERRORLEVEL==1 echo FATAL ERROR: 'perl' does not work; check if your strawberry pack is complete!



call cpanm PAR::Packer
call cpanm Date::Manip 
call cpanm DateTime::Event::Cron::Quartz
call cpanm Filter::Crypto::Decrypt



setlocal EnableDelayedExpansion

set "FILES="

for %%i in (bin\dx_*.pl) do (
	set FILES=!FILES! %%i
)

echo %FILES%

mkdir compiled
del compiled\*.exe /q


call pp -u -I .\lib -l d:\a\perl\c\bin\libcrypto-1_1-x64__.dll -l d:\a\perl\c\bin\zlib1__.dll -l d:\a\perl\c\bin\libssl-1_1-x64__.dll -M Crypt::Blowfish  -F Crypto=dbutils\.pm$ -M Filter::Crypto::Decrypt -M Text::CSV_PP -M List::MoreUtils::PP -o compiled\runner.exe %FILES%

echo @echo off > compiled\install.cmd
echo set "LISTOFFILES=%FILES%" >> compiled\install.cmd
echo for %%%%d in (%%LISTOFFILES%%) do ( >> compiled\install.cmd
echo    echo %%%%~nd.exe  >> compiled\install.cmd
echo    mklink /H %%%%~nd.exe runner.exe  >> compiled\install.cmd
echo )  >> compiled\install.cmd

dir compiled

rename compiled dxtoolkit2