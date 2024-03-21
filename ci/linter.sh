#!/bin/bash

# Ensure the script stops if there's an error
set -e

# Fetch latest commits and branches from origin to ensure the comparison is up-to-date
git fetch origin master

# List files that have been modified in the current branch compared to master
# This filters for files ending in .cs (C# files)
modified_files=$(git diff --name-only origin/master...HEAD | grep '\.cs$')

# Check if modified_files is empty
if [ -z "$modified_files" ]; then
    echo "No C# files have been modified."
else
    echo "Modified C# files:"
    echo "$modified_files"

    # Run dotnet format on each modified file
    for file in $modified_files; do
        echo "Formatting $file"
        dotnet format "$file" --check --verbosity diagnostic
    done

    echo "Formatting complete."
fi
