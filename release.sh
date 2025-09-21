#!/bin/bash
set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <new_version>"
  exit 1
fi

VERSION=$1

echo "Bumping version to $VERSION..."

# update version in __init__.py
sed -i "s/^__version__ = .*/__version__ = \"$VERSION\"/" freactor/__init__.py

git add freactor/__init__.py
git commit -m "bump version to $VERSION"
git tag -a "v$VERSION" -m "Release $VERSION"
git push origin master --tags

echo "Building..."
rm -rf dist build *.egg-info
python3 -m build

echo "Uploading..."
python3 -m twine upload dist/*

echo "✅ Release $VERSION published!"
