#!/bin/bash

current_branch=$(git symbolic-ref -q HEAD)
current_branch=${current_branch##refs/heads/}
current_branch=${current_branch:-HEAD}

if [ "$current_branch" == "HEAD" ]; then
    echo "You don't have a branch checked out. Please switch to your development branch."
    exit 1
fi

if [ "$current_branch" == "main" ]; then
    echo "You are on the main branch. Please switch to your development branch."
    exit 1
fi

if [ "$current_branch" == "dev" ]; then
    echo "You are on the dev branch. Please switch to your development branch."
    exit 1
fi

set -e

uncommitted_files=$(git diff-index --quiet HEAD -- || echo "untracked")

continue="y"
if [ $uncommitted_files ]; then
    git status
    echo "There are uncommitted files. Do you want to continue? (Y/n)"
    read continue
    echo ""
    continue=${continue:-y}
fi

if [ "$continue" != "y" ]; then
    exit 0
fi

git pull
git push

echo "Current branch: $current_branch"
echo "What branch would you like to deploy to?"
select branch in main dev; do
    break;
done

if [ "$branch" == "dev" ]; then
    echo "Do you want to reset the branch 'dev' to 'main'? (Y/n)"
    read reset
    reset=${reset:-y}
    if [ "$reset" == "y" ]; then
        git switch dev
        git reset --hard main
        git push -f
    fi
fi

echo "Deploying kode from $current_branch to $branch"
git switch $branch
git pull
git merge $current_branch
git push
git switch $current_branch
echo "Deployed kode from $current_branch to $branch"
