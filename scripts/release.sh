#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Docker repository
DOCKER_REPO="machetienew/riven"

# Get the latest tag and increment it
LATEST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.3.5")
LATEST_TAG=${LATEST_TAG#v} # Remove 'v' prefix

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
CHANGELOG=$(git log --pretty=format:"- %s" $(git describe --tags --abbrev=0)..HEAD)

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

# Create and push tag
echo -e "${YELLOW}Creating and pushing tag v${VERSION}...${NC}"
git tag -a "v${VERSION}" -m "Release version ${VERSION}"
git push origin main --tags

# Build Docker image
echo -e "${YELLOW}Building Docker image...${NC}"
docker build -t "${DOCKER_REPO}:${VERSION}" -t "${DOCKER_REPO}:latest" .

# Push Docker images
echo -e "${YELLOW}Pushing Docker images...${NC}"
docker push "${DOCKER_REPO}:${VERSION}"
docker push "${DOCKER_REPO}:latest"

echo -e "${GREEN}Release ${VERSION} completed successfully!${NC}"
