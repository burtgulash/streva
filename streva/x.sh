#!/bin/bash

COMMANDS="try test"

command=${1}

case ${command} in
"test")
    python3 -m pytest tests ;;

"try")
    old_pythonpath=${PYTHONPATH}
    export PYTHONPATH=${PYTHONPATH}:$(pwd)
    python3 examples/try.py
    export PYTHONPATH=${old_pythonpath}
    ;;


*)
    echo "No command given! Use one of following:"
    for c in $(echo ${COMMANDS} | tr ' ' "\n" | sort)
    do
        echo "  $c"
    done

    exit 1
    ;;
esac
