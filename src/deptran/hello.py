import os
from simplerpc.marshal import Marshal
from simplerpc.future import Future

class HelloServiceService(object):
    SAYHELLO = 0x5a81d0ac
    ECHO = 0x10584e13
    ASYNCCOMPUTE = 0x5ab47ca2

    __input_type_info__ = {
        'SayHello': ['std::string'],
        'Echo': ['std::string'],
        'AsyncCompute': ['rrr::i32'],
    }

    __output_type_info__ = {
        'SayHello': ['std::string'],
        'Echo': ['std::string'],
        'AsyncCompute': ['rrr::i32'],
    }

    def __bind_helper__(self, func):
        def f(*args):
            return getattr(self, func.__name__)(*args)
        return f

    def __reg_to__(self, server):
        server.__reg_func__(HelloServiceService.SAYHELLO, self.__bind_helper__(self.SayHello), ['std::string'], ['std::string'])
        server.__reg_func__(HelloServiceService.ECHO, self.__bind_helper__(self.Echo), ['std::string'], ['std::string'])
        server.__reg_func__(HelloServiceService.ASYNCCOMPUTE, self.__bind_helper__(self.AsyncCompute), ['rrr::i32'], ['rrr::i32'])

    def SayHello(__self__, name):
        raise NotImplementedError('subclass HelloServiceService and implement your own SayHello function')

    def Echo(__self__, message):
        raise NotImplementedError('subclass HelloServiceService and implement your own Echo function')

    def AsyncCompute(__self__, value):
        raise NotImplementedError('subclass HelloServiceService and implement your own AsyncCompute function')

class HelloServiceProxy(object):
    def __init__(self, clnt):
        self.__clnt__ = clnt

    def async_SayHello(__self__, name):
        return __self__.__clnt__.async_call(HelloServiceService.SAYHELLO, [name], HelloServiceService.__input_type_info__['SayHello'], HelloServiceService.__output_type_info__['SayHello'])

    def async_Echo(__self__, message):
        return __self__.__clnt__.async_call(HelloServiceService.ECHO, [message], HelloServiceService.__input_type_info__['Echo'], HelloServiceService.__output_type_info__['Echo'])

    def async_AsyncCompute(__self__, value):
        return __self__.__clnt__.async_call(HelloServiceService.ASYNCCOMPUTE, [value], HelloServiceService.__input_type_info__['AsyncCompute'], HelloServiceService.__output_type_info__['AsyncCompute'])

    def sync_SayHello(__self__, name):
        __result__ = __self__.__clnt__.sync_call(HelloServiceService.SAYHELLO, [name], HelloServiceService.__input_type_info__['SayHello'], HelloServiceService.__output_type_info__['SayHello'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_Echo(__self__, message):
        __result__ = __self__.__clnt__.sync_call(HelloServiceService.ECHO, [message], HelloServiceService.__input_type_info__['Echo'], HelloServiceService.__output_type_info__['Echo'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_AsyncCompute(__self__, value):
        __result__ = __self__.__clnt__.sync_call(HelloServiceService.ASYNCCOMPUTE, [value], HelloServiceService.__input_type_info__['AsyncCompute'], HelloServiceService.__output_type_info__['AsyncCompute'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

