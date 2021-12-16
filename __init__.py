from __future__ import unicode_literals, print_function, absolute_import
import ctypes
import ctypes.util
import pkg_resources
import os
import threading
import signal
import traceback
import sys
import multiprocessing as mp


import pkg_resources
if os.uname()[0] != 'Darwin':
    libname = pkg_resources.resource_filename("allspark",
                                              "liballspark_shared.so")
else:
    libname = pkg_resources.resource_filename("allspark",
                                              "liballspark_shared.dylib")
spark = ctypes.CDLL(libname)

__all__ = [
    b'Context', b'QueuedService', b'Property', b'default_properties',
    b'Channel', b'cflags', b'ldflags'
]


class CBuffer(ctypes.Structure):
    r""" CBuffer is used for passing fix-length binary data to/from C

    >>> bin_data = b"\x00\x01\x10\x20\x40"
    >>> cbuffer = CBuffer(ctypes.cast(bin_data, ctypes.POINTER(ctypes.c_char)), len(bin_data))

    >>> new_data = ctypes.cast(cbuffer.data, ctypes.POINTER(ctypes.c_char * cbuffer.size)).contents.raw
    >>> bin_data == new_data
    True
    """
    _fields_ = [("data", ctypes.POINTER(ctypes.c_char)),
                ("code", ctypes.c_int), ("size", ctypes.c_int)]


class CBufferArray(ctypes.Structure):
    r"""
    """
    _fields_ = [("data", ctypes.POINTER(CBuffer)), ("size", ctypes.c_int)]


PyCALLBACK = ctypes.CFUNCTYPE(None, ctypes.c_char_p)
PyHANDLER = ctypes.CFUNCTYPE(ctypes.c_char_p, ctypes.c_char_p)
PyCALLBACK2 = ctypes.CFUNCTYPE(None, CBuffer)

spark.Allspark_Property_Get.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
spark.Allspark_Property_Get.restype = ctypes.c_char_p
spark.Allspark_Property_Put.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
spark.Allspark_Property_Put.restype = None
spark.Allspark_Property_Listen.argtypes = [ctypes.c_char_p, PyCALLBACK]
spark.Allspark_Property_Listen.restype = None
spark.Allspark_Context_Create.argtypes = [ctypes.c_int]
spark.Allspark_Context_Create.restype = ctypes.c_void_p
spark.Allspark_Context_Stop.argtypes = [ctypes.c_void_p]
spark.Allspark_Context_Delete.argtypes = [ctypes.c_void_p]
spark.Allspark_Context_QueuedService.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p
]
spark.Allspark_Context_QueuedService.restype = ctypes.c_void_p
spark.Allspark_Context_Channel.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
spark.Allspark_Context_Channel.restype = ctypes.c_void_p
spark.Allspark_QueuedService_Read.argtypes = [ctypes.c_void_p]
spark.Allspark_QueuedService_Read.restype = CBuffer
spark.Allspark_QueuedService_Write.argtypes = [ctypes.c_void_p, CBuffer]
spark.Allspark_QueuedService_Write.restype = None
spark.Allspark_QueuedService_Free.argtypes = [ctypes.c_void_p, CBuffer]
spark.Allspark_QueuedService_Free.restype = None
spark.Allspark_QueuedService_FreeArray.argtypes = [
    ctypes.c_void_p, CBufferArray
]
spark.Allspark_QueuedService_FreeArray.restype = None
spark.Allspark_QueuedService_Error.argtypes = [
    ctypes.c_void_p, ctypes.c_int, CBuffer
]
spark.Allspark_QueuedService_Error.restype = None
spark.Allspark_QueuedService_ReadUpto.argtypes = [
    ctypes.c_void_p, ctypes.c_int, ctypes.c_int
]
spark.Allspark_QueuedService_ReadUpto.restype = CBufferArray
spark.Allspark_QueuedService_WriteMany.argtypes = [
    ctypes.c_void_p, CBufferArray
]
spark.Allspark_QueuedService_WriteMany.restype = None
spark.Allspark_QueuedService_ReadWithID.argtypes = [
    ctypes.c_void_p, ctypes.POINTER(ctypes.c_longlong)
]
spark.Allspark_QueuedService_ReadWithID.restype = CBuffer
spark.Allspark_QueuedService_WriteWithID.argtypes = [
    ctypes.c_void_p, CBuffer,
    ctypes.POINTER(ctypes.c_longlong)
]
spark.Allspark_QueuedService_WriteWithID.restype = None
spark.Allspark_QueuedService_ErrorWithID.argtypes = [
    ctypes.c_void_p, ctypes.c_int, CBuffer,
    ctypes.POINTER(ctypes.c_longlong)
]
spark.Allspark_QueuedService_ErrorWithID.restype = None
spark.Allspark_QueuedService_Inject.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, PyHANDLER
]
spark.Allspark_QueuedService_Inject.restype = None
spark.Allspark_Channel_Request.argtypes = [ctypes.c_void_p, CBuffer]
spark.Allspark_Channel_Request.restype = CBuffer
spark.Allspark_Channel_AsyncRequest.argtypes = [
    ctypes.c_void_p, CBuffer, PyCALLBACK2, PyCALLBACK2
]
spark.Allspark_Channel_AsyncRequest.restype = None

# ctypes callback functions are cached here to avoid GC.
# if no references is kept, ctypes crashes the programme when a callback is made.
callbacks = []


class Property:
    """ Property is a key-value storage for configuration and performance metrics

    >>> prop = default_properties()
    >>> prop.put(b"rpc.test", b"abc")
    >>> print(prop.get(b"rpc.test").decode())
    abc

    >>> print(prop.get(b"rpc.test_1").decode())
    <BLANKLINE>
    
    >>> print(prop.get(b"rpc.test_1", b"123").decode())
    123
    """

    def get(self, name, default=b''):
        """ get a property, return `default` if property not exists.

        :param bytes name: property name to read
        :param bytes default: default value if property not exists
        :return: property value if exists, `default` otherwise
        :rtype: bytes
        """
        if not isinstance(name, bytes):
            name = str(name).encode()
        retval = spark.Allspark_Property_Get(name, default)
        return type(default)(retval)

    def put(self, name, value):
        """ put a property `name` = `value`

        :param bytes name: property name
        :param bytes value: property value
        """
        if not isinstance(name, bytes):
            name = str(name).encode()
        if not isinstance(value, bytes):  # convert value to bytes if necessary
            value = str(value).encode()
        return spark.Allspark_Property_Put(name, value)

    def listen(self, name, callback=None):
        """ register a property listener on given name

        callback can be passed into register by args or by decorator

        :param bytes name: property name
        :param func callback: property callback

        >>> def foo(x): print(x)
        >>> default_properties().listen(b'rpc.test', foo)

        >>> @default_properties().listen(b'rpc.test')
        ... def foo(x):
        ...     print(x) 
        """
        if not isinstance(name, bytes):
            name = str(name).encode()

        if callback:  # called as api, callback is passed by args
            pycallback = PyCALLBACK(callback)
            callbacks.append(pycallback)
            return spark.Allspark_Property_Listen(name, pycallback)
        else:  # called as decorator, callback will be received by wrapper

            def wrapper(callback):
                pycallback = PyCALLBACK(callback)
                callbacks.append(pycallback)
                spark.Allspark_Property_Listen(name, pycallback)
                return callback

            return wrapper


def default_properties():
    """ get default properties

    >>> defaults = default_properties()
    >>> defaults.put(b"rpc.test", b"123456")
    >>> print(defaults.get(b"rpc.test").decode())
    123456
    """
    return Property()


class Context:
    """ a resource pool including io thread pool, signal handling and performance counters
    """

    def __init__(self, num_thread):
        self.this = spark.Allspark_Context_Create(num_thread)

    def __del__(self):
        spark.Allspark_Context_Delete(self.this)

    def stop(self):
        """ stop io thread pool
        """
        spark.Allspark_Context_Stop(self.this)

    def queued_service(self, endpoint=None):
        """ create a queued rpc service
        """
        # for python processor, we require that the model be loaded before starting service.
        spark.Allspark_Property_Put(b"model.ready", b'1')
        if not endpoint:
            return QueuedService(
                spark.Allspark_Context_QueuedService(self.this, endpoint))
        if not isinstance(endpoint, bytes):
            endpoint = str(endpoint).encode()
        return QueuedService(
            spark.Allspark_Context_QueuedService(self.this, endpoint))

    def channel(self, uri):
        """ create a connection pool to `uri`
        """
        if not isinstance(uri, bytes):
            uri = str(uri).encode()
        chn = spark.Allspark_Context_Channel(self.this, uri)
        return Channel(chn)


class QueuedService:
    """ rpc service with message-queue like api
    """

    def __init__(self, this):
        self.this = this

    def read(self):
        """ read a request from queued service
        
        :return: the request message
        :rtype: `bytes`
        """
        msg = spark.Allspark_QueuedService_Read(self.this)
        retval = ctypes.cast(msg.data,
                             ctypes.POINTER(ctypes.c_char * msg.size))
        retval = retval.contents.raw
        spark.Allspark_QueuedService_Free(self.this, msg)
        return retval

    def write(self, msg):
        """ write a response to queued service

        :param bytes msg: the response
        """
        if not isinstance(msg, bytes):
            raise Exception('bytes is expected, %s is found.' % type(msg))
        msg = ctypes.create_string_buffer(msg, len(msg))
        msg_p = ctypes.cast(msg, ctypes.POINTER(ctypes.c_char))
        cbuffer = CBuffer(msg_p, 0, len(msg))
        spark.Allspark_QueuedService_Write(self.this, cbuffer)

    def error(self, code, msg):
        """ write a custom error response

        :param int code: error code
        :param bytes msg: error message
        """
        if not isinstance(msg, bytes):
            raise Exception('bytes is expected, %s is found.' % type(msg))

        msg = ctypes.create_string_buffer(msg, len(msg))
        msg_p = ctypes.cast(msg, ctypes.POINTER(ctypes.c_char))
        cbuffer = CBuffer(msg_p, code, len(msg))
        spark.Allspark_QueuedService_Error(self.this, code, cbuffer)

    def read_upto(self, batch_size, timeout):
        """ batch read for requests

        :param int batch_size: max num. of request to read
        :param int timeout: max timeout(ms)

        :return: list of batched requests
        :rtype: list(bytes)
        """
        msgs = spark.Allspark_QueuedService_ReadUpto(self.this, batch_size,
                                                     timeout)
        retval = []
        for i in range(msgs.size):
            msg = msgs.data[i]
            msg_p = ctypes.cast(msg.data,
                                ctypes.POINTER(ctypes.c_char * msg.size))
            retval.append(msg_p.contents.raw)
        spark.Allspark_QueuedService_FreeArray(self.this, msgs)
        return retval

    def write_many(self, msgs):
        """ batch write for responses

        :param list(code_msg) msgs: list of responses (tuple(int, bytes)) 
        """
        buffers = (CBuffer * len(msgs))()
        buffer_array = CBufferArray(
            ctypes.cast(buffers, ctypes.POINTER(CBuffer)), len(msgs))
        string_buffers = []
        for i in range(len(msgs)):
            msg = msgs[i]
            if isinstance(msg, tuple):
                code = msg[0]
                msg = msg[1]
            else:
                code = 0
            sbuffer = ctypes.create_string_buffer(msg, len(msg))
            string_buffers.append(sbuffer)
            buffers[i] = CBuffer(
                ctypes.cast(sbuffer, ctypes.POINTER(ctypes.c_char)), code,
                len(msg))
        spark.Allspark_QueuedService_WriteMany(self.this, buffer_array)

    def read_with_id(self):
        """ read a request from queued service

        :return: request message and request id for async response
        :rtype: (bytes, int)
        """
        request_id = ctypes.c_longlong(0)
        msg = spark.Allspark_QueuedService_ReadWithID(
            self.this, ctypes.pointer(request_id))
        retval = ctypes.cast(msg.data,
                             ctypes.POINTER(ctypes.c_char * msg.size))
        retval = retval.contents.raw
        spark.Allspark_QueuedService_Free(self.this, msg)
        return retval, request_id.value

    def write_with_id(self, msg, request_id):
        """ write a response to queued service

        :param bytes msg: response message
        :param int request_id: request id to response
        """
        if not isinstance(msg, bytes):
            raise Exception('bytes is expected, %s is found.' % type(msg))
        msg = ctypes.create_string_buffer(msg, len(msg))
        msg_p = ctypes.cast(msg, ctypes.POINTER(ctypes.c_char))
        cbuffer = CBuffer(msg_p, 0, len(msg))
        request_id = ctypes.c_longlong(request_id)
        spark.Allspark_QueuedService_WriteWithID(self.this, cbuffer,
                                                 ctypes.pointer(request_id))

    def error_with_id(self, code, msg, request_id):
        """ write a response to queued service with a error code

        :parma int code: responde error code
        :param bytes msg: response message
        :param int request_id: request id to response
        """
        if not isinstance(msg, bytes):
            raise Exception('bytes is expected, %s is found.' % type(msg))
        msg = ctypes.create_string_buffer(msg, len(msg))
        msg_p = ctypes.cast(msg, ctypes.POINTER(ctypes.c_char))
        cbuffer = CBuffer(msg_p, 0, len(msg))
        request_id = ctypes.c_longlong(request_id)
        spark.Allspark_QueuedService_ErrorWithID(self.this, code, cbuffer,
                                                 ctypes.pointer(request_id))


    def inject(self, path, handler=None, token=None):
        """ inject a new handler into the service, identified by `path` and `token`
        """
        if not isinstance(path, bytes):
            path = str(path).encode()
        if handler:  # alled as api, handler is passed by args
            pyhandler = PyHANDLER(handler)
            callbacks.append(pyhandler)
            spark.Allspark_QueuedService_Inject(self.this, path, token,
                                                pyhandler)
        else:  # called as decorator, handler will be received by wrapper

            def wrapper(handler):
                pyhandler = PyHANDLER(handler)
                callbacks.append(pyhandler)
                spark.Allspark_QueuedService_Inject(self.this, path, token,
                                                    pyhandler)
                return handler

            return wrapper


class Channel:
    """ rpc client side connection pool
    """

    def __init__(self, this):
        self.this = this
        self.cb = {}
        self.cnt = 0

    def request(self, msg):
        """ sync request

        :param bytes msg: request message to send
        :return: response message
        :rtype: bytes

        :raise Exception: if request is failed
        """
        if not isinstance(msg, bytes):
            raise Exception('bytes is expected, %s is found.' % type(msg))
        msg = ctypes.create_string_buffer(msg, len(msg))
        msg_p = ctypes.cast(msg, ctypes.POINTER(ctypes.c_char))
        request = CBuffer(msg_p, 0, len(msg))
        response = spark.Allspark_Channel_Request(self.this, request)
        if response.code == 0:
            retval = ctypes.cast(response.data,
                                 ctypes.POINTER(ctypes.c_char * response.size))
            retval = retval.contents.raw
            return retval
        else:
            retval = ctypes.cast(response.data,
                                 ctypes.POINTER(ctypes.c_char * response.size))
            retval = retval.contents.raw
            raise Exception(retval)

    def async_request(self, msg, onsuccess=None, onfailure=None, *arg, **kws):
        """ async request

        :param bytes msg: request message to send
        :param func onsuccess: success callback
        :param func onfailure: failure callback
        :param list arg: variable length arguments for onsuccess/onfailure callbacks
        :param dict kws: keyword arguments for onsuccess/onfailure callbacks

        >>> def onsuccess(x): print(x)
        >>> def onfailure(x): print(x)
        >>> channel.async_request(b'request', onsuccess, onfailure)
        ('response')

        >>> def onsuccess(x, arg1, kws1='val1'): print(x, arg1, ksw1)
        >>> def onfailure(x, arg1, kws1='val1'): print(x, arg1, ksw1)
        >>> channel.async_request(b'request', onsuccess, onfailure, 'arg1', kws1='val1')
        ('response', arg1, val1)
        """

        request_id = self.cnt
        self.cb[request_id] = 1
        self.cnt += 1

        def wrapper(callback, cbs, rid, arg, kws):
            def wrapped(buf):
                rsp = ctypes.cast(buf.data,
                                  ctypes.POINTER(ctypes.c_char * buf.size))
                rsp = rsp.contents.raw
                callback(rsp, *arg, **kws)
                del cbs[rid]

            return wrapped

        if onsuccess:
            success_cb = PyCALLBACK2(
                wrapper(onsuccess, self.cb, request_id, arg, kws))
        else:
            success_cb = None
        if onfailure:
            failure_cb = PyCALLBACK2(
                wrapper(onfailure, self.cb, request_id, arg, kws))
        else:
            failure_cb = None

        self.cb[request_id] = (success_cb, failure_cb)

        msg = ctypes.create_string_buffer(msg, len(msg))
        msg_p = ctypes.cast(msg, ctypes.POINTER(ctypes.c_char))
        request = CBuffer(msg_p, 0, len(msg))

        spark.Allspark_Channel_AsyncRequest(self.this, request, success_cb,
                                            failure_cb)


class BaseProcessor:
    """ BaseProcessor is base class, user can implement your
        own processor which inherit this class

        Attributes:
            worker_threads: worker thread pool size
            io_threads: io thread pool size
            context: rpc context
            queued: rpc queue
    """
    def __init__(self, worker_threads=5, io_threads=4, worker_processes=1, endpoint=None):
        """
        Initialize BaseProcessor with specified configuration
        if worker_processes is greater than 1, then the processor is running in
        multiple processing environment, the worker threads do nothing except
        get the request from rpc queue and then put it into the process queue.
        :param worker_threads: number of worker threads to process user request
        :param io_threads: number of the io threads to process io request
        :param worker_processes: number of processes to process user request
        """
        self.worker_threads = worker_threads
        self.worker_processes = worker_processes
        self.io_threads = io_threads
        self.max_queue_size = int(default_properties().get(b'rpc.max_queue_size', b'64'))
        self.processes = []

        self.context = Context(self.io_threads)
        # Ingore the endpoint argument when running in EAS cluster in which case the
        # env 'NAMESPACE' and 'POD_NAME' are set
        if os.getenv('NAMESPACE') is not None and os.getenv('POD_NAME') is not None:
            endpoint = None
        self.queued = self.context.queued_service(endpoint)

    def initialize(self):
        """
        initialize function is called before running proccessor
        it is a virtual function, user need to implement it
        """
        raise Exception('initialize not implemented')

    def process(self, data):
        """
        process function is used to process user's request
        it is a virtual function. user need to implement it
        """
        raise Exception('process not implemented')

    def _run_one_thread(self, req_queue=None):
        """
        Start a loop which read message in a single thread,
        process message and write back response or send to response queue.
        """
        while True:
            try:
                if req_queue is not None:
                    request, id = self.queued.read_with_id()
                    req_queue.put((request, id))
                else:
                    request = self.queued.read()
                    msg, err = self.process(request)
                    if not isinstance(msg, bytes):
                        msg = str(msg).encode()
                    if err == 0 or err == 200:
                        self.queued.write(msg)
                    else:
                        self.queued.error(err, msg)

            except Exception as e:
                self.queued.error(400, str(e).encode())
                traceback.print_exc()

    def _run_one_process(self, req_queue, rsp_queue):
        self.initialize()
        """
        Start a loop to read from the shared Queue to get request from
        the main process, put it back after the request is done
        """
        while True:
            # process was taken over by init process, which means its
            # parents has exited, so break the loop to exit.
            if os.getppid() == 1:
                break
            try:
                # wake up every second to check whether its parent has exited
                request, id = req_queue.get(timeout=1)
            except:
                # when timeout reached, queue.Empty exception is raised, but
                # in python2, the queue package is named as Queue, we ignore
                # the exception type to skip the bothering brought by the version
                # divergence, every time we catch an exception, we regard it as
                # the timeout is reached, do nothing and go to next loop.
                continue

            msg, err = self.process(request)
            rsp_queue.put((msg, err, id))

    def _run_response_thread(self, rsp_queue):
        """
        Start a loop to read response data from the response queue and
        write them to rpc queue to return to the client user
        """
        while True:
            msg, err, id = rsp_queue.get()
            if err != 0 and err != 200:
                self.queued.error_with_id(err, msg, id)
            else:
                self.queued.write_with_id(msg, id)

    def _init_multi_processing(self):
        """
        Initialize a process pool to process request in multiple processes,
        Read request message through eas rpc in main process, and put it into
        a multiprocess.Queue call request queue, the worker processes try to
        consume the request in the queue, and then put the response to a
        response queue, then the main process reads from the response queue,
        and write the response data back to the user client asynchronously
        """
        req_queue = mp.Queue(self.max_queue_size)
        rsp_queue = mp.Queue()

        for _ in range(self.worker_processes):
            p = mp.Process(target=self._run_one_process, args=(req_queue, rsp_queue))
            p.daemon = True
            p.start()
            self.processes.append(p)

        # Create request producer threads, read request message from rpc queue
        # and write them to the request queue for consumer processes to read
        workers = []
        for _ in range(self.worker_threads):
            worker = threading.Thread(target=self._run_one_thread, args=(req_queue,))
            worker.setDaemon(True)
            worker.start()
            workers.append(worker)

        # Create a thread to read responses from the response queue,
        # and write them back to client
        w = threading.Thread(target=self._run_response_thread, args=(rsp_queue,))
        w.setDaemon(True)
        w.start()

        # Wait the status of the child processes, once one of the child
        # processes exited, the main process and all the other child
        # processes should also exit.
        while True:
            status = os.wait()
            child_exists = False
            for p in self.processes:
                if p.pid == status[0]:
                    child_exists = True
                    break
            if not child_exists:
                continue

            for p in self.processes:
                try:
                    os.kill(p.pid, signal.SIGKILL)
                except:
                    pass

    def _init_multi_threading(self):
        """
        Initialize thread pool to process request in multiple threads
        """
        workers = []
        for _ in range(self.worker_threads):
            worker = threading.Thread(target=self._run_one_thread, args=())
            worker.setDaemon(True)
            worker.start()
            workers.append(worker)

        for worker in workers:
            worker.join()

    def run(self):
        """
        Start threads or processes to process user's requests
        """
        if self.worker_processes > 1:
            self._init_multi_processing()
        else:
            self.initialize()
            self._init_multi_threading()


def cflags():
    """ get cflags for compilling a C extension
    """
    pkg_file = pkg_resources.resource_filename(__name__, "__init__.py")
    pkg_path = os.path.dirname(pkg_file)
    return "-I%s -D_GLIBCXX_USE_CXX11_ABI=0" % pkg_path


def ldflags():
    """ get ldflags for compilling a C extension
    """
    pkg_file = pkg_resources.resource_filename(__name__, "__init__.py")
    pkg_path = os.path.dirname(pkg_file)
    return "-L%s -lallspark_shared" % pkg_path
