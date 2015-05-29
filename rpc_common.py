# -*- coding: utf-8 -*-

from SimpleXMLRPCServer import SimpleXMLRPCServer
from xmlrpclib import ServerProxy
from common import *
from spider.fetch_common import *
from threading import Thread
import urllib, urllib2, socket

class RpcCommon:
    m_is_server = False
    m_listen_ip = '0.0.0.0'
    m_listen_port = 8000
    m_started = False
    m_short_conn = False
    m_conn_retry_times  = 2
    m_rcv_request_callback = None
    m_rcv_result_callback  = None
    m_rpc_listen_runtine   = None
    m_rpc_proxys           = {}
    m_accept_thread        = None

    def __init__(self, is_server, listen_port = 8000, listen_eth = 'eth0', \
            connect_time = 5, conn_retry_times = 2, short_conn = False):
        self.m_is_server   = is_server
        self.m_listen_ip   = get_local_ip(listen_eth)
        self.m_listen_port = listen_port
        self.m_short_conn  = short_conn
        self.m_conn_retry_times = conn_retry_times
        socket.setdefaulttimeout(connect_time)
    def __accept(self):
        self.m_rpc_listen_runtine = SimpleXMLRPCServer((self.m_listen_ip, self.m_listen_port))
        if self.m_is_server:
            self.m_rpc_listen_runtine.register_function(self.rpc_rcv_request)
        else:
            self.m_rpc_listen_runtine.register_function(self.rpc_rcv_result)
        log_info('RPC listen at %s:%d.' % (self.m_listen_ip, self.m_listen_port))
        self.m_rpc_listen_runtine.serve_forever()
    def set_rcv_request_callback(self, callback):
        self.m_rcv_request_callback = callback
    def set_rcv_result_callback(self, callback):
        self.m_rcv_result_callback = callback
    def start(self):
        if self.m_started == True:
            log_error('RpcCommon::start already started.\n')
            return
        self.m_accept_thread = Thread(target=self.__accept)
        self.m_accept_thread.start()
        self.m_started = True
    def exit(self):
        if self.m_started == False:
            log_error("RpcCommon::exit not started.")
            return
        log_info('RPC exit start.')
        if self.m_rpc_listen_runtine:
            self.m_rpc_listen_runtine.shutdown()
        if self.m_accept_thread:
            self.m_accept_thread.join()
        self.m_started = False
        log_info('RPC exit end.')
    def __get_cached_proxy(self, peer_ip_port):
        if self.m_short_conn:
            return None 
        return self.m_rpc_proxys.get(peer_ip_port)
    def __update_cached_proxy(self, peer_ip_port, proxy):
        if self.m_short_conn:
            return
        self.m_rpc_proxys[peer_ip_port] = proxy
    def rpc_rcv_request(self, request):
        assert self.m_is_server == True
        fetch_request = FetchRequest('')
        fetch_request.__dict__.update(**request)
        if self.m_rcv_request_callback:
            return self.m_rcv_request_callback(fetch_request)
        return True
    def rpc_rcv_result(self, result):
        try:
            assert self.m_is_server == False
            fetch_result = FetchResult('', '', '', None)
            fetch_result.__dict__.update(**result)
            if fetch_result.m_contex:
                fetch_result.m_contex = fetch_result.m_contex.get('contex')
            else:
                log_error('RpcCommon::rpc_rcv_result find none contex.')
            if self.m_rcv_result_callback:
                return self.m_rcv_result_callback(fetch_result)
            return True
        except Exception, err:
            log_error('RpcCommon::rpc_rcv_result %s:%d %s' % (self.m_listen_ip, self.m_listen_port, err))
            return False
    def rpc_send_result(self, result):
        assert self.m_is_server == True
        if result.m_contex == None:
            log_error('RpcCommon::rpc_send_result find none contex, send failed.')
            return False
        peer_ip_port = result.m_contex.get('ip_port')
        peer_proxy = self.__get_cached_proxy(peer_ip_port)
        for i in range(self.m_conn_retry_times):
            try:
                if i > 0 or peer_proxy is None:
                    peer_proxy = ServerProxy('http://%s' % peer_ip_port, allow_none = True)
                res = peer_proxy.rpc_rcv_result(result)
                self.__update_cached_proxy(peer_ip_port, peer_proxy)
                log_info('RpcCommon send %s %s to %s success.' % (result.m_url, result.m_request_data, peer_ip_port))
                return res
            except Exception, err:
                log_error('RpcCommon::rpc_send_result %s failed: %s, retry %d' % (peer_ip_port, err, i))
        return False
    def rpc_send_request(self, ip, port, request):
        assert self.m_is_server == False
        peer_ip_port   = '%s:%d' % (ip, port)
        listen_ip_port = '%s:%d' % (self.m_listen_ip, self.m_listen_port)
        contex = {'contex': request.m_contex, 'ip_port':listen_ip_port}
        request.m_contex = contex 
        peer_proxy = self.__get_cached_proxy(peer_ip_port)
        for i in range(self.m_conn_retry_times):
            try:
                if i > 0 or peer_proxy == None:
                    peer_proxy = ServerProxy('http://%s' % peer_ip_port, allow_none = True)
                res = peer_proxy.rpc_rcv_request(request)
                log_info('RpcCommon send %s %s to %s success' % (request.m_url, request.m_request_data, peer_ip_port))
                self.__update_cached_proxy(peer_ip_port, peer_proxy)
                return res
            except Exception, err:
                log_error('RpcCommon::rpc_send_request send %s:%d failed: %s, retry %d' % (ip, port, err, i))
        return False
