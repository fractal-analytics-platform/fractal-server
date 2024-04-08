HERE=`pwd`

for FOLDER in invalid_manifest missing_executable missing_manifest; do
    cd $FOLDER
    echo $FOLDER
    ./update_package_manifest_and_wheel.sh
    cd $HERE
done
