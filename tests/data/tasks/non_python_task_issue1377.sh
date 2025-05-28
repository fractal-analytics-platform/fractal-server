# This script accepts two optional arguments, `--args-json` and `--out-json`.
# If both arguments are provided, the script terminates successfully.
# If any other arguments are provided, the script raises an error.

# used in `tests/v2/08_full_workflow/test_full_workflow_*_v2.py`

JSON_FILE=
METADATA_OUT=

while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
        --args-json)
        JSON_FILE="$2"
        shift # past argument
        shift # past value
        ;;
        --out-json)
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
    echo "This goes to standard error" >&2
    echo "This goes to standard output"
fi
