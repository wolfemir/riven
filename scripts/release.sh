#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Docker repository
DOCKER_REPO="machetienew/riven"

# Get the latest docker tag and increment it
LATEST_TAG=$(docker images ${DOCKER_REPO} --format "{{.Tag}}" | grep -E "^[0-9]+\.[0-9]+\.[0-9]+$" | sort -V | tail -n1 || echo "0.3.5")

# Split version into major, minor, and patch
IFS='.' read -r MAJOR MINOR PATCH <<< "$LATEST_TAG"

# Increment patch version
PATCH=$((PATCH + 1))

# New version
VERSION="${MAJOR}.${MINOR}.${PATCH}"

echo -e "${YELLOW}Starting release process for version ${VERSION}${NC}"

# Ensure we're in the git repository root
cd "$(git rev-parse --show-toplevel)" || exit 1

# Generate changelog
echo -e "${YELLOW}Generating changelog...${NC}"
CHANGELOG=$(git log --pretty=format:"- %s" HEAD~1..HEAD)

# Create commit message with conventional commit format and changelog
COMMIT_MSG="feat(release): version ${VERSION}

This release includes various improvements and bug fixes:

${CHANGELOG}

BREAKING CHANGE: None"

# Stage all changes
git add .

# Commit with changelog
echo -e "${YELLOW}Committing changes...${NC}"
git commit -m "$COMMIT_MSG"

# Push changes to branch
echo -e "${YELLOW}Pushing changes to branch...${NC}"
git push origin HEAD

# Build Docker image
echo -e "${YELLOW}Building Docker image...${NC}"
docker build -t "${DOCKER_REPO}:${VERSION}" -t "${DOCKER_REPO}:latest" .

# Push Docker images
echo -e "${YELLOW}Pushing Docker images...${NC}"
docker push "${DOCKER_REPO}:${VERSION}"
docker push "${DOCKER_REPO}:latest"

echo -e "${GREEN}Release ${VERSION} completed successfully!${NC}"
