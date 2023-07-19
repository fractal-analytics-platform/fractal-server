# This script is a copy of `non_python_task_issue189.sh`, but it is used as a
# non-executable task command (that should trigger a specific error).

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

if [[ -n $JSON_FILE && -n $METADATA_OUT ]]
then
    cp "$JSON_FILE" "$METADATA_OUT"
    echo "This goes to standard error" >&2
    echo "This goes to standard output"
fi
