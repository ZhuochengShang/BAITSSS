@echo off
setlocal enabledelayedexpansion

:: Define version for beast-spark package.
set beast_version=0.10.1-RC2

:: Initialize jars and packages list
set jars=
set packages=edu.ucr.cs.bdlab:beast-spark:%beast_version%,org.mortbay.jetty:jetty:6.1.26,org.eclipse.jetty:jetty-servlet:9.4.48.v20220622,org.eclipse.jetty:jetty-server:9.4.48.v20220622,org.geotools:gt-epsg-hsql:26.1,org.geotools:gt-coverage:26.1,org.locationtech.jts:jts-core:1.18.2,com.google.protobuf:protobuf-java:3.21.12
set excluded_packages=org.apache.hadoop:hadoop-common,org.apache.hadoop:hadoop-hdfs,javax.media:jai_core
set repositories=https://repo.osgeo.org/repository/release/

:: Initialize path to lib dir and check for existing beast-spark JAR
set "lib_dir=%~dp0..\lib"

:: Create the lib directory if it does not exist
if not exist "%lib_dir%" mkdir "%lib_dir%"

:: Download javax activation and jai_core if they do not exist locally
if not exist "%lib_dir%\jai_core-1.1.3.jar" curl -o "%lib_dir%\jai_core-1.1.3.jar" "https://repo1.maven.org/maven2/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar"

:: Populate jars with all jar files under lib_dir
for %%i in ("%lib_dir%\*.jar") do (
    if "!jars!" == "" (
        set "jars=%%i"
    ) else (
        set "jars=!jars!,%%i"
    )
)

:: Handle command line arguments
set additional_spark_args=
set program=
:parse_args
if "%~1"=="" goto end_parse_args
if "%~1"=="--jars" (
    set jars=!jars!,%~2,
    shift
) else if "%~1"=="--packages" (
    set packages=!packages!,%~2
    shift
) else if "%~1"=="--repositories" (
    set repositories=!repositories!,%~2
    shift
) else if "%~1"=="--exclude-package" (
    set excluded_packages=!excluded_packages!,%~2
    shift
) else if "%~1"=="--*" (
    set additional_spark_args=!additional_spark_args! %1 %2
    shift
) else (
    set program=%1
    goto end_parse_args
)
shift
goto parse_args
:end_parse_args

:: Generate Spark arguments
set spark_args=--jars %jars% --packages %packages% --repositories %repositories% --exclude-packages %excluded_packages%
set spark_args=%spark_args% %additional_spark_args%

if not "%program%"=="" (
    if exist "%program%" (
        set spark_args=%spark_args% %program%
    ) else (
        goto run_else_part
    )
) else (
    :run_else_part
    :: This part runs if the program is not set or it is not an existent file
    set spark_args=%spark_args% --class edu.ucr.cs.bdlab.beast.operations.Main . %program%
)

:: Loop through each remaining command-line argument
:loop_args
if "%~1"=="" goto end_loop_args
set spark_args=%spark_args% %1
shift
goto loop_args
:end_loop_args

:: Execute spark-submit with the generated arguments
echo Running spark-submit with the following arguments:
echo %spark_args%
:: Uncomment the line below to actually run the command
spark-submit %spark_args%

endlocal
