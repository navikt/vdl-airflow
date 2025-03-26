#!/bin/bash
set -e

git switch main
git pull
read -p "Enter branch name: " branch
git switch -c $branch
git push -u origin $branch
echo "Created branch $branch and pushed to origin"
