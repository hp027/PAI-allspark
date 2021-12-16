#ifndef ALLSPARK_H
#define ALLSPARK_H

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace eas {
namespace allspark {

/** @defgroup ftypes "function types" */

/** @{ */

//! Unit is a function that do nothing and hold no information
typedef std::function<void(void)> Unit;

//! MsgHandler processes an input message and returns a output response
typedef std::function<std::string(const std::string &)> MsgHandler;

//! SigHandler processes a signal (represented as a <name, value> pair)
typedef std::function<bool(const std::string &, const std::string &)>
    SigHandler;

//! Initializer returns a pair of (MsgHandler, SigHandler) for new RPC Sessions
typedef std::function<std::pair<MsgHandler, SigHandler>(void)> Initializer;

//! async request on success callback fuction.
typedef std::function<std::string(const std::string &)> OnSuccessCallback;

//! async request on failure callback fuction.
typedef std::function<void(int code, const std::string &)> OnFailureCallback;

/** @} */

typedef Unit Dependency;
template <typename Derived>
struct DependencyContainer {
  using DependencyFactory = std::function<Dependency(Derived &)>;

  std::vector<Dependency> __deps__;

  void with(Dependency dep) { __deps__.push_back(dep); }
  void with(DependencyFactory dep_factory) {
    __deps__.push_back(dep_factory(*static_cast<Derived *>(this)));
  }
};

struct noncopyable {
 public:
  noncopyable() {}

 private:
  noncopyable(const noncopyable &) = delete;
  noncopyable &operator=(const noncopyable &) = delete;
};

class Context;
class Service;
class Session;
class QueuedService;
class Channel;
class ConnectionPoolManager;

class Service : public DependencyContainer<Service>, noncopyable {
 public:
  explicit Service(const Context &ctx);
  explicit Service(const Context &ctx, const std::string &address,
                   const std::string &port);
  ~Service();

  void bind(const std::string &endpoint);
  void bind(const std::string &address, const std::string &port);
  int get_bind_port();

  QueuedService *queued();
  QueuedService *queued(Initializer initializer);

  void run(MsgHandler handler);
  void run(Initializer initializer);

  void inject(const std::string &uri, const std::string &token,
              MsgHandler initializer);
  void inject(const std::string &uri, MsgHandler handler);

  void set_async(bool flag = true);

 private:
  struct Impl;
  Impl *impl_;
};

class QueuedService : public DependencyContainer<QueuedService>, noncopyable {
 public:
  QueuedService(Service &srv);
  QueuedService(Service &srv, MsgHandler handler);
  QueuedService(Service &srv, Initializer initializer);
  ~QueuedService();

  std::size_t max_size();
  std::size_t max_size(std::size_t size);

  std::string read();
  void write(const std::string &msg);
  void error(int error_code, const std::string &error_msg);

  std::string read(int64_t &id);
  void write(const std::string &msg, int64_t &id);
  void error(int error_code, const std::string &error_msg, int64_t &id);

  std::vector<std::string> read_upto(std::size_t numb, int64_t timeout,
                                     bool parallel_dequeue = true);
  void write(const std::vector<std::string> &responses);
  void write(const std::vector<int> &error_codes,
             const std::vector<std::string> &responses);

  Service &service() { return srv_; }

 private:
  struct Impl;
  Impl *impl_;
  Service &srv_;
};

class Channel : public DependencyContainer<Channel>, noncopyable {
 public:
  Channel(Context &ctx, std::function<std::string()> get_endpoint,
          const std::map<std::string, std::string> &options = {});
  Channel(Context &ctx, const std::string &endpoint,
          const std::map<std::string, std::string> &options);
  Channel(Context &ctx, const std::string &uri);

  ~Channel();

  std::string request(const std::string &msg);
  int async_request(std::string msg,  //
                    OnSuccessCallback onsuccess, OnFailureCallback onfailure);

  // request interface with options which can be modified by user.
  std::string request(const std::string &msg,
                      const std::map<std::string, std::string> &opt);
  int async_request(std::string msg,  //
                    OnSuccessCallback onsuccess, OnFailureCallback onfailure,
                    const std::map<std::string, std::string> &opt);

 private:
  struct Impl;
  Impl *impl_;
};

class Context : public DependencyContainer<Context>, noncopyable {
 public:
  Context(std::size_t num_threads);

  Context(std::size_t num_threads, int max_connection_pool_size,
          int recycle_connection_pool_time);

  ~Context();

  void run();
  void stop();

  void apply(std::function<void(void)> func);

  Service *service(const std::string &endpoint);
  Service *service(const std::string &addr, const std::string &port);

  QueuedService *queued_service(const std::string &endpoint,
                                const std::string &token);
  QueuedService *queued_service(const std::string &endpoint) {
    return queued_service(endpoint, "");
  }

  Channel *channel(const std::string &endpoint,
                   const std::map<std::string, std::string> &options) {
    return new Channel(*this, endpoint, options);
  }
  Channel *channel(const std::string &uri) { return new Channel(*this, uri); }

  ConnectionPoolManager *GetConnectionPoolManager();

 private:
  struct Impl;
  Impl *impl_;

 public:
  friend class Service;
  friend class Session;
};

}  // namespace allspark
}  // namespace eas

#endif /* ALLSPARK_H */
