CXX = g++
CXXFLAGS = -std=c++17 -O2 -I/usr/local/include/mongocxx/v_noabi -I/usr/local/include/bsoncxx/v_noabi
LIBS = -lmongocxx -lbsoncxx

main: main.cpp customcontainers.hpp
	$(CXX) $(CXXFLAGS) -o main main.cpp $(LIBS)

clean:
	rm -f main

.PHONY: clean