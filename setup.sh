#!/bin/bash

# 0. Update apt to ensure we find the packages
sudo apt-get update

# 1. Install Dependencies (CMake AND Hiredis)
echo "Installing dependencies..."
sudo apt-get install -y cmake libhiredis-dev build-essential

# 2. Clean up previous failed attempts
if [ -d "redis-plus-plus" ]; then
    echo "Removing previous redis-plus-plus directory..."
    rm -rf redis-plus-plus
fi

# 3. Clone the C++ wrapper repository
echo "Cloning redis-plus-plus..."
git clone https://github.com/sewenew/redis-plus-plus.git
cd redis-plus-plus

# 4. Build and Install
echo "Building..."
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make
sudo make install

# 5. Refresh library cache so Linux finds the new files
sudo ldconfig

cd ../.. 
echo "Setup complete!"
