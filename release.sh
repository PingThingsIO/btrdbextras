#!/bin/bash

set -e

# helper function used to promot the user for how they would like to proceed
function checkYN {
  while true
  do
    read -p "$1 (Y/N): " action
    case $action in
      n|N|no)
        return 1
        ;;
      y|Y|yes)
        return 0
        ;;
      *)
        echo "type Y or N"
        ;;
      esac
  done
}

if [[ "$1" == "" ]]
then
  echo "Missing version e.g 5 11 2"
  echo -e "\nUsage:\n ./release.sh 5 11 2 \n"
  exit 1
fi

if ! checkYN "Add new release 'v$1.$2.$3'?"; then
    echo exiting
    exit 1
fi

echo "Setting version to v$1.$2.$3"

VERION_CODE="__version__ = 'v$1.$2.$3'"
sed -i "s/^__version__.*$/${VERION_CODE}/g" btrdbextras/__init__.py

git add btrdbextras/__init__.py
git commit -m "Release v$1.$2.$3"
git tag v$1.$2.$3
git push origin v$1.$2.$3

sleep 10
git push