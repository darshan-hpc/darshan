#include <iostream>

#include "mycpplib.hpp"

void X::fn1(const char* name, int arg1) { std::cout << "X::fn1(" << "name=" << name << ", arg1=" << arg1 << ")\n"; }
void X::fn2() { std::cout << "X::fn2()\n"; }
void X::fn3() { std::cout << "X::fn3()\n"; }
