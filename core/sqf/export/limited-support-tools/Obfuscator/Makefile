CC=g++
CFLAGS=-I.
CXXFLAGS=-std=c++0x
DEPS = Classifier.h Dictionary.h Obfuscator.h Scanner.h Token.h 
OBJ = Classifier.o Dictionary.o Obfuscator.o ObfuscatorMain.o Scanner.o Token.o

%.o: %.cpp $(DEPS)
	$(CC) -g -c -o $@ $< $(CFLAGS) $(CXXFLAGS)

Obfuscator.exe: $(OBJ)
	g++ -g -o $@ $^ $(CFLAGS)
