# 1. Install CMake if you don't have it
sudo apt-get install cmake

# 2. Clone the C++ wrapper repository
git clone https://github.com/sewenew/redis-plus-plus.git
cd redis-plus-plus

# 3. Build and Install
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make
sudo make install

# 4. Refresh library cache so Linux finds the new files
sudo ldconfig

cd ../.. # Go back to your project folder
