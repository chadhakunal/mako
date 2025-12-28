#pragma once

#include "rrr.hpp"

#include <errno.h>


namespace hello {

class HelloServiceService: public rrr::Service {
public:
    enum {
        SAYHELLO = 0x5a81d0ac,
        ECHO = 0x10584e13,
        ASYNCCOMPUTE = 0x5ab47ca2,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(SAYHELLO, this, &HelloServiceService::__SayHello__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(ECHO, this, &HelloServiceService::__Echo__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(ASYNCCOMPUTE, this, &HelloServiceService::__AsyncCompute__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(SAYHELLO);
        svr->unreg(ECHO);
        svr->unreg(ASYNCCOMPUTE);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, req is rusty::Box (auto-cleaned); weak_sconn requires lock() before use
    virtual void SayHello(const std::string& name, std::string* greeting, rrr::DeferredReply* defer) = 0;
    virtual void Echo(const std::string& message, std::string* response, rrr::DeferredReply* defer) = 0;
    virtual void AsyncCompute(const rrr::i32& value, rrr::i32* result, rrr::DeferredReply* defer) = 0;
private:
    void __SayHello__wrapper__(rusty::Box<rrr::Request> req, rrr::WeakServerConnection weak_sconn) {
        std::string* in_0 = new std::string;
        req->m >> *in_0;
        std::string* out_0 = new std::string;
        auto __marshal_reply__ = [=] {
            auto sconn_opt = weak_sconn.upgrade();
            if (sconn_opt.is_some()) {
                auto sconn = sconn_opt.unwrap();
                const_cast<rrr::ServerConnection&>(*sconn) << *out_0;
            }
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(std::move(req), weak_sconn, __marshal_reply__, __cleanup__);
        this->SayHello(*in_0, out_0, __defer__);
    }
    void __Echo__wrapper__(rusty::Box<rrr::Request> req, rrr::WeakServerConnection weak_sconn) {
        std::string* in_0 = new std::string;
        req->m >> *in_0;
        std::string* out_0 = new std::string;
        auto __marshal_reply__ = [=] {
            auto sconn_opt = weak_sconn.upgrade();
            if (sconn_opt.is_some()) {
                auto sconn = sconn_opt.unwrap();
                const_cast<rrr::ServerConnection&>(*sconn) << *out_0;
            }
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(std::move(req), weak_sconn, __marshal_reply__, __cleanup__);
        this->Echo(*in_0, out_0, __defer__);
    }
    void __AsyncCompute__wrapper__(rusty::Box<rrr::Request> req, rrr::WeakServerConnection weak_sconn) {
        rrr::i32* in_0 = new rrr::i32;
        req->m >> *in_0;
        rrr::i32* out_0 = new rrr::i32;
        auto __marshal_reply__ = [=] {
            auto sconn_opt = weak_sconn.upgrade();
            if (sconn_opt.is_some()) {
                auto sconn = sconn_opt.unwrap();
                const_cast<rrr::ServerConnection&>(*sconn) << *out_0;
            }
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(std::move(req), weak_sconn, __marshal_reply__, __cleanup__);
        this->AsyncCompute(*in_0, out_0, __defer__);
    }
};

class HelloServiceProxy {
protected:
    rrr::Client* __cl__;
public:
    HelloServiceProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::FutureResult async_SayHello(const std::string& name, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        auto __fu_result__ = __cl__->begin_request(HelloServiceService::SAYHELLO, __fu_attr__);
        if (__fu_result__.is_err()) {
            return __fu_result__;  // Propagate error
        }
        auto __fu__ = __fu_result__.unwrap();
        *__cl__ << name;
        __cl__->end_request();
        return rrr::FutureResult::Ok(__fu__);
    }
    rrr::i32 SayHello(const std::string& name, std::string* greeting) {
        auto __fu_result__ = this->async_SayHello(name);
        if (__fu_result__.is_err()) {
            return __fu_result__.unwrap_err();  // Return error code
        }
        auto __fu__ = __fu_result__.unwrap();
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *greeting;
        }
        // Arc auto-released
        return __ret__;
    }
    rrr::FutureResult async_Echo(const std::string& message, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        auto __fu_result__ = __cl__->begin_request(HelloServiceService::ECHO, __fu_attr__);
        if (__fu_result__.is_err()) {
            return __fu_result__;  // Propagate error
        }
        auto __fu__ = __fu_result__.unwrap();
        *__cl__ << message;
        __cl__->end_request();
        return rrr::FutureResult::Ok(__fu__);
    }
    rrr::i32 Echo(const std::string& message, std::string* response) {
        auto __fu_result__ = this->async_Echo(message);
        if (__fu_result__.is_err()) {
            return __fu_result__.unwrap_err();  // Return error code
        }
        auto __fu__ = __fu_result__.unwrap();
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *response;
        }
        // Arc auto-released
        return __ret__;
    }
    rrr::FutureResult async_AsyncCompute(const rrr::i32& value, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        auto __fu_result__ = __cl__->begin_request(HelloServiceService::ASYNCCOMPUTE, __fu_attr__);
        if (__fu_result__.is_err()) {
            return __fu_result__;  // Propagate error
        }
        auto __fu__ = __fu_result__.unwrap();
        *__cl__ << value;
        __cl__->end_request();
        return rrr::FutureResult::Ok(__fu__);
    }
    rrr::i32 AsyncCompute(const rrr::i32& value, rrr::i32* result) {
        auto __fu_result__ = this->async_AsyncCompute(value);
        if (__fu_result__.is_err()) {
            return __fu_result__.unwrap_err();  // Return error code
        }
        auto __fu__ = __fu_result__.unwrap();
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *result;
        }
        // Arc auto-released
        return __ret__;
    }
};

} // namespace hello



