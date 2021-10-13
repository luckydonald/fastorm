#!/usr/bin/env bash
# $ git clone ../DockerTgBot/.git .got.git
# $ ./copy_commit.sh <COMMIT_HASH>
# or
# $ alias c='./copy_commit.sh'
# $ c <COMMIT_HASH>

COMMIT=${1}

cd .got.git/ || exit 2
git checkout $COMMIT

COMMIT_MESSAGE=$(git log -1 --pretty=%B)
COMMIT_MESSAGE=$(echo $COMMIT_MESSAGE | python -c 'import sys; txt = sys.stdin.read(); txt = txt.removeprefix("[auction_helper]").strip(); print(txt);')

COMMIT_HASH=$(git log -1 --pretty=%H)
COMMIT_DATE=$(git log -1 --pretty='%cd')
# COMMIT_AUTHOR_DATE=$(git log -1 --pretty='%ad')

echo "Commit message: <$COMMIT_MESSAGE>"
echo "Commit date: <$COMMIT_DATE>"
echo "Commit hash: <$COMMIT_HASH>"

git format-patch -1 --stdout -- auction_helper/code/auction_helper/database/cheap_orm.py > ../dot.patch
sed -i 's!auction_helper/code/auction_helper/database/!fastorm/!g' ../dot.patch
cd ..
git apply dot.patch || ( echo -e '\nClose (ctrl + X) when fixed the merge conflict' > /tmp/alert.txt ; nano /tmp/alert.txt )
git add -u
git status
git commit \
  -m "$COMMIT_MESSAGE" \
  -m "" \
  -m "" \
  -m "" \
  -m "(from luckydonald/docker-tg-bot@$COMMIT_HASH)" \
  --date="$COMMIT_DATE"



