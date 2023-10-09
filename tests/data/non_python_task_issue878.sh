# This script accepts two optional arguments, `--json`` and `--metadata-out`.
# If both arguments are provided, the script copies the file specified
# by the `--json` argument to the location specified by the `--metadata-out`
# argument. If any other arguments are provided, the script raises an error.

# used in `tests/test_full_workflow.py::test_non_python_task`

JSON_FILE=
METADATA_OUT=

while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
        -j)
        JSON_FILE="$2"
        shift # past argument
        shift # past value
        ;;
        --metadata-out)
        METADATA_OUT="$2"
        shift # past argument
        shift # past value
        ;;
        *)
        echo "Error: unknown argument $key"
        exit 1
        ;;
    esac
done

sleep 1

if [[ -n $JSON_FILE && -n $METADATA_OUT ]]
then
    echo "null" > $METADATA_OUT
fi
