#!/bin/bash

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --json)
            shift
            ;;
        --metadata-out)
            shift
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
    shift
done

echo "Hello, Fractal!"
