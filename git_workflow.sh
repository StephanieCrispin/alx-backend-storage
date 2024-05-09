#!/usr/bin/env sh
git status 

git add .

echo "git status after adding changes"

git status

echo "Changes have been added to commit"

read -p "Enter your commit message: " commit_message

git commit -m "$commit_message"

git push

echo "Git workflow completed successfully"