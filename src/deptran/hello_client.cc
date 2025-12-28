#include "hello.h"
#include <chrono>
#include <iostream>
#include <thread>

using namespace std;

int main(int argc, char **argv) {
  const char *server_addr = "127.0.0.1:8000";

  if (argc > 1) {
    server_addr = argv[1];
  }

  cout << "Hello World RRR Client connecting to " << server_addr << endl;

  // Create poll thread
  auto poll_thread = rrr::PollThread::create();

  // Create client
  auto client = rrr::Client::create(poll_thread.clone());

  // Connect to server
  if (client->connect(server_addr) != 0) {
    cerr << "Failed to connect to " << server_addr << endl;
    return 1;
  }

  cout << "Connected successfully!" << endl;

  // Create proxy - Arc::get() returns const pointer, but proxy needs mutable
  // This is safe because Client uses interior mutability (methods are const)
  hello::HelloServiceProxy proxy(const_cast<rrr::Client *>(client.get()));

  // Test 1: Synchronous call
  cout << "\n=== Test 1: Synchronous SayHello ===" << endl;
  string greeting;
  auto ret = proxy.SayHello("Alice", &greeting);
  cout << "SayHello returned: " << ret << ", greeting: " << greeting << endl;

  // Test 2: Asynchronous call with callback
  cout << "\n=== Test 2: Async Echo with callback ===" << endl;
  rrr::FutureAttr attr;
  attr.callback = [](rusty::Arc<rrr::Future> fu) {
    if (fu->get_error_code() == 0) {
      string response;
      fu->get_reply() >> response;
      cout << "[Callback] Echo completed successfully! Response: " << response
           << endl;
    } else {
      cout << "[Callback] Echo failed with error: " << fu->get_error_code()
           << endl;
    }
  };

  auto fu_result = proxy.async_Echo("Hello from client!", attr);
  if (fu_result.is_ok()) {
    cout << "Async Echo initiated" << endl;
  }

  // Test 3: Async compute with manual wait
  cout << "\n=== Test 3: Async AsyncCompute with manual wait ===" << endl;
  auto compute_result = proxy.async_AsyncCompute(42);
  if (compute_result.is_ok()) {
    auto fu = compute_result.unwrap();
    cout << "Waiting for AsyncCompute to complete..." << endl;
    fu->wait();
    if (fu->get_error_code() == 0) {
      rrr::i32 result;
      fu->get_reply() >> result;
      cout << "AsyncCompute completed successfully! Result: " << result << endl;
    } else {
      cout << "AsyncCompute failed with error code: " << fu->get_error_code()
           << endl;
    }
  }

  // Give callbacks time to execute
  cout << "\nWaiting for async operations to complete..." << endl;
  this_thread::sleep_for(chrono::seconds(1));

  cout << "\n=== All tests completed ===" << endl;

  // Cleanup
  client->close();
  poll_thread->shutdown();

  return 0;
}
