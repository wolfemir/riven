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

# Generate detailed changelog
echo -e "${YELLOW}Generating detailed changelog...${NC}"

# Get all changed files
CHANGED_FILES=$(git diff --name-only HEAD~1..HEAD)

# Initialize changelog sections
FEATURES=""
FIXES=""
REFACTOR=""
DOCS=""
OTHER=""

# Process each commit and categorize changes
while IFS= read -r commit; do
    if [[ $commit == feat* ]]; then
        FEATURES="${FEATURES}- ${commit#feat: }\n"
    elif [[ $commit == fix* ]]; then
        FIXES="${FIXES}- ${commit#fix: }\n"
    elif [[ $commit == refactor* ]]; then
        REFACTOR="${REFACTOR}- ${commit#refactor: }\n"
    elif [[ $commit == docs* ]]; then
        DOCS="${DOCS}- ${commit#docs: }\n"
    else
        OTHER="${OTHER}- ${commit}\n"
    fi
done < <(git log --pretty=format:"%s" HEAD~1..HEAD)

# Build the changelog with sections
CHANGELOG="### 🚀 Features\n"
[[ -n "$FEATURES" ]] && CHANGELOG+="$FEATURES\n" || CHANGELOG+="No new features\n\n"

CHANGELOG+="### 🐛 Bug Fixes\n"
[[ -n "$FIXES" ]] && CHANGELOG+="$FIXES\n" || CHANGELOG+="No bug fixes\n\n"

CHANGELOG+="### ♻️ Refactoring\n"
[[ -n "$REFACTOR" ]] && CHANGELOG+="$REFACTOR\n" || CHANGELOG+="No refactoring changes\n\n"

CHANGELOG+="### 📚 Documentation\n"
[[ -n "$DOCS" ]] && CHANGELOG+="$DOCS\n" || CHANGELOG+="No documentation changes\n\n"

[[ -n "$OTHER" ]] && CHANGELOG+="### 🔄 Other Changes\n$OTHER\n"

# Add file changes section
CHANGELOG+="\n### 📁 Modified Files\n"
while IFS= read -r file; do
    CHANGELOG+="- \`$file\`\n"
done <<< "$CHANGED_FILES"

# Create commit message with conventional commit format and detailed changelog
COMMIT_MSG="feat(release): version ${VERSION}

🎉 Release Notes for version ${VERSION}

${CHANGELOG}

### 💥 Breaking Changes
- None

### 🔍 Additional Notes
- Docker image: ${DOCKER_REPO}:${VERSION}
- Previous version: ${LATEST_TAG}"

# Stage all changes
git add .

# Commit with changelog
echo -e "${YELLOW}Committing changes with detailed changelog...${NC}"
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
