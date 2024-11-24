#!/bin/bash

# Get the next version number from the argument or default to 'latest'
VERSION=${1:-latest}
IMAGE_NAME="machetie/riven:$VERSION"

echo "Building Docker image: $IMAGE_NAME"

# Build the Docker image
docker build -t "$IMAGE_NAME" .

# Check if build was successful
if [ $? -eq 0 ]; then
    echo "Build successful!"
    echo "To run the container:"
    echo "docker run -d \\"
    echo "  --name riven \\"
    echo "  -v /path/to/config:/config \\"
    echo "  -v /path/to/downloads:/downloads \\"
    echo "  -v /path/to/media:/media \\"
    echo "  $IMAGE_NAME"
else
    echo "Build failed!"
    exit 1
fi
