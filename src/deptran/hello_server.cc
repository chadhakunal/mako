#include "hello.h"
#include <iostream>

using namespace std;

// Implement the HelloService - simplified version without threading
class HelloServiceImpl : public hello::HelloServiceService {
public:
  void SayHello(const string &name, string *greeting,
                rrr::DeferredReply *defer) override {
    cout << "[Server] Received SayHello request from: " << name << endl;
    *greeting = "Hello, " + name + "!";
    cout << "[Server] Replying with: " << *greeting << endl;
    defer->reply();
  }

  void Echo(const string &message, string *response,
            rrr::DeferredReply *defer) override {
    cout << "[Server] Received Echo: " << message << endl;
    *response = "Echo: " + message;
    cout << "[Server] Echoing back: " << *response << endl;
    defer->reply();
  }

  void AsyncCompute(const rrr::i32 &value, rrr::i32 *result,
                    rrr::DeferredReply *defer) override {
    cout << "[Server] Received AsyncCompute: " << value << endl;
    *result = value * 2;
    cout << "[Server] Computed result: " << *result << endl;
    defer->reply();
  }
};

int main(int argc, char **argv) {
  const char *bind_addr = "0.0.0.0:8000";

  if (argc > 1) {
    bind_addr = argv[1];
  }

  cout << "Starting Hello World RRR Server on " << bind_addr << endl;

  // Create server
  rrr::Server server;

  // Create and register service
  HelloServiceImpl hello_service;
  hello_service.__reg_to__(&server);

  // Start server
  if (server.start(bind_addr) != 0) {
    cerr << "Failed to start server on " << bind_addr << endl;
    return 1;
  }

  cout << "Server running. Press Ctrl+C to stop." << endl;

  // Keep running
  while (true) {
    sleep(1);
  }

  return 0;
}
