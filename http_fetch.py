# -*- coding: utf-8 -*-

import urllib, urllib2, time, base64, sys, os, signal, ConfigParser, traceback
from common import *
from spider.fetch_common import *
import threading, thread
from threading import Thread
from StringIO import StringIO
import Queue, socket, webbrowser, shelve, hashlib, gzip, binascii
from SimpleXMLRPCServer import SimpleXMLRPCServer
import rpc_common

class Fetcher:
    '''retry_instant == True:    if one request fetch failed, try to fetch the same request next time'''
    '''host_concurency == False: in condition of m_host_wait_time, two requests of one host cannot fetch at the same time'''
    def __init__(self, thread_num = 2, connect_time = 10, retry_times = 1, default_wait = 10, \
                 max_request_queue_size = 10000, max_result_queue_size = 10000, \
                 retry_instant = True, host_concurency = False, check_interval = 1, \
                 daemon = True, snapshot_storage_file = None, snapshot_expire_sec = 0, \
                 default_http_header = {}, listen_port = 8000):
        self.m_thread_num = thread_num
        socket.setdefaulttimeout(connect_time);
        self.m_thread_list = []
        self.m_host_mutex = threading.Lock()
        self.m_dns_cache_mutex = threading.Lock()
        self.m_host_queue = {}
        self.m_host_retry = {}
        self.m_host_wait_time  = {}
        self.m_host_avail_time = {}
        self.m_default_wait_time = default_wait
        self.m_request_queue_size = max_request_queue_size
        self.m_result_queue_size = max_result_queue_size
        self.m_result_queue = Queue.Queue(self.m_result_queue_size)
        self.m_need_cancel_fun = None
        self.m_exit = False
        self.m_retry_times = retry_times
        self.m_retry_instant = retry_instant
        self.m_host_concurency = host_concurency
        self.m_check_interval = check_interval
        self.m_daemon = daemon
        self.m_snapshot_storage = None
        if snapshot_storage_file != None:
            self.m_snapshot_storage = shelve.open(snapshot_storage_file)
        self.m_snapshot_expire_sec = snapshot_expire_sec
        self.m_default_http_header = default_http_header
        self.m_listen_port = listen_port
        self.m_server = None
        self.m_host_fetching = {}
        self.m_started = False
        self.m_host_http_header = {}
        self.m_dnscache = {}
        self.m_enable_dns_cache = True
    def _setDNSCache(self):  
        def _getaddrinfo(*args, **kwargs):
            self.m_dns_cache_mutex.acquire()
            dns_info = None 
            if args in self.m_dnscache:
                dns_info = self.m_dnscache[args]
            else:
                dns_info = socket._getaddrinfo(*args, **kwargs)
            self.m_dnscache[args] = dns_info 
            self.m_dns_cache_mutex.release()
            return dns_info 
        if not hasattr(socket, '_getaddrinfo'):  
            socket._getaddrinfo = socket.getaddrinfo  
            socket.getaddrinfo = _getaddrinfo
    def _deletDNSCache(self, host):
        self.m_dns_cache_mutex.acquire()
        for host_info, server_info in self.m_dnscache.items():
            cur_host = host_info[0]
            host_str, host_port = urllib.splitport(host)
            if cur_host == host_str:
                del server_info
        self.m_dns_cache_mutex.release()
    def __wait_time(self, host):
        wait_sec = self.m_host_wait_time.get(host)
        if wait_sec == None:
            return self.m_default_wait_time;
        return wait_sec
    '''if request attach wich header, use request header, else use headers defined in configure'''
    def __http_header(self, request):
        request_header = request.m_request_header
        if request_header and len(request_header) != 0:
            return request_header
        request_header = self.m_host_http_header.get(request.host())
        if request_header == None:
            return self.m_default_http_header
        return request_header
    def __http_request(self, request):
        request.m_retry_num += 1
        http_request = urllib2.Request(request.m_url, data = request.m_request_data, headers = self.__http_header(request))
        '''default use gzip encode'''
        if http_request.has_header('Accept-Encoding') == False:
            http_request.add_header('Accept-Encoding', 'gzip')
        if self.m_exit == True:
            log_info('Fetcher::__http_request return for exit.')
            return None
        try:
            response = urllib2.urlopen(http_request)
            result = FetchResult(request.m_url, str(response.info()), response.read(), request.m_contex, request_data = request.m_request_data)
            log_info("Fetcher::__http_request download %s %s success, content_len:%d" % \
                (request.m_url, request.m_request_data, len(result.m_response_content)))
            return result
        except Exception, err:
            self._deletDNSCache(request.host())
            log_error('Fetcher::__http_request download %s %s failed: %s, retry %d' % (request.m_url, request.m_request_data, err, request.m_retry_num))
        return None
    def __should_exit(self):
        return self.m_exit or (not self.m_daemon and self.qsize() == 0)
    def __fetch_thread_end(self):
        for thd in self.m_thread_list:
            if thd.isAlive():
                return False
        return True
    def __write_snapshot(self, result):
        if self.m_snapshot_storage != None and result != None:
            key_str = result.m_url
            if result.m_request_data != None:
                key_str += result.m_request_data
            key = hashlib.md5(key_str).hexdigest()
            self.m_snapshot_storage[str(key)] = gzip_compress(str(result))
            log_info('Fetcher::__write_snapshot write cache %s' % result.m_url)
    def __read_snapshot(self, request):
        if self.m_snapshot_storage == None:
            return None
        key_str = request.m_url
        if request.m_request_data != None:
            key_str += request.m_request_data
        key = hashlib.md5(key_str).hexdigest()
        result_str = self.m_snapshot_storage.get(str(key))
        if result_str == None:
            return None
        if self.m_snapshot_expire_sec and time.time() - request.m_fetch_time > request.m_snapshot_expire_sec:
            self.m_snapshot_storage[key] = None
            log_info('Fetcher::__read_snapshot remove expire cache %s' % request.m_url)
            return None
        result = FetchResult(request.m_url, '', '', request.m_contex, request_data = request.m_request_data)
        result_str = gzip_decompress(result_str)
        result.load_str(result_str)
        log_info('Fetcher::__read_snapshot cache hit %s' % request.m_url)
        return result
    def __put_result(self, result):
        while self.m_exit == False and self.m_result_queue.qsize() >= self.m_result_queue_size:
            log_error('Fetcher::__put_result extend max result queue size: %d' % self.m_result_queue_size)
            time.sleep(self.m_check_interval)
        self.m_result_queue.put(result)
    def set_host_http_header(self, host, headers):
        self.m_host_http_header[host] = headers
    def set_host_wait_time_sec(self, host, wait_time):
        self.m_host_wait_time[host] = wait_time
    def set_cancel_callback(self, need_cancel_fun):
        self.m_need_cancel_fun = need_cancel_fun
    def set_proxy(self, proxy_addr):
        opener = urllib2.build_opener( urllib2.ProxyHandler({'http':proxy_addr}))
        urllib2.install_opener(opener)
    def qsize(self, host_name = None):
        cnt = 0
        #self.m_host_mutex.acquire()
        for host,queue in self.m_host_queue.items():
            if host_name != None and host_name != host:
                continue
            cnt += queue.qsize()
            if self.m_retry_instant == True and self.m_host_retry.get(host) != None:
                cnt += 1
        #self.m_host_mutex.release()
        return cnt
    def start(self):
        if self.m_enable_dns_cache:
            self._setDNSCache()        
        for i in range(self.m_thread_num):
            thread_obj = Thread(target=self.__run, args = (i+1,))
            self.m_thread_list.append(thread_obj)
        for thread_i in self.m_thread_list:
            thread_i.start()
        if self.m_daemon:
            self.m_server = rpc_common.RpcCommon(True, listen_port = self.m_listen_port)
            self.m_server.set_rcv_request_callback(self.put_request)
            self.m_server.start()
        self.m_started = True
    def load_config(self, config_file):
        cf = ConfigParser.ConfigParser()
        global_sec_name = 'Fetcher/Global'
        speed_sec_name  = 'Fetcher/Speed'
        http_header_sec_name = 'Fetcher/HttpHeader'
        try:
            cf.read(config_file)
            if cf.has_option(global_sec_name, 'daemon'):
                self.m_daemon = cf.getboolean(global_sec_name, 'daemon')
            if cf.has_option(global_sec_name, 'proc_thread_num'):
                self.m_thread_num = cf.getint(global_sec_name, 'proc_thread_num')
            if cf.has_option(global_sec_name, 'connect_timeout'):
                socket.setdefaulttimeout(cf.getfloat(global_sec_name, 'connect_timeout'))
            if cf.has_option(global_sec_name,  'retry_times'):
                self.m_retry_times = cf.getint(global_sec_name, 'retry_times')
            if cf.has_option(global_sec_name,  'max_host_request_queue_size'):
                self.m_request_queue_size = cf.getint(global_sec_name, 'max_host_request_queue_size')
            if cf.has_option(global_sec_name,  'max_result_queue_size'):
                self.m_result_queue_size = cf.getint(global_sec_name, 'max_result_queue_size')
                self.m_result_queue = Queue.Queue(self.m_result_queue_size)
            if cf.has_option(global_sec_name,  'fail_retry_instant'):
                self.m_retry_instant = cf.getboolean(global_sec_name, 'fail_retry_instant')
            if cf.has_option(global_sec_name,  'host_concurency'):
                self.m_host_concurency = cf.getboolean(global_sec_name, 'host_concurency')
            if cf.has_option(global_sec_name,  'check_interval'):
                self.m_check_interval = cf.getfloat(global_sec_name, 'check_interval')
            if cf.has_option(global_sec_name, 'dns_cache'):
                self.m_enable_dns_cache = cf.getboolean(global_sec_name, 'dns_cache')
            if cf.has_option(global_sec_name,  'snapshot_storage_file'):
                snapshot_storage_file = cf.get(global_sec_name, 'snapshot_storage_file')
                if snapshot_storage_file != None and snapshot_storage_file.strip(' ') == '':
                    self.m_snapshot_storage = shelve.open(snapshot_storage_file)
                    self.m_snapshot_expire_sec = cf.getfloat(global_sec_name, 'snapshot_expire_time')
            if cf.has_option(global_sec_name,  'listen_port'):
                self.m_listen_port = cf.getint(global_sec_name, 'listen_port')
            '''load speed config'''
            for name, value in cf.items(speed_sec_name):
                wait_sec = float(value)
                if name == 'host_wait_default':
                    self.m_default_wait_time = wait_sec
                    continue
                self.set_host_wait_time_sec(name, wait_sec)
                log_info('Fetcher::load_config %s:%f' % (name, wait_sec))
            if cf.has_section(http_header_sec_name) == False:
                log_info('Fetcher::load_config cannot find Fetcher/HttpHeader in %s' % config_file)
                return
            for name, value in cf.items(http_header_sec_name):
                headers = eval(value)
                if name == 'http_header_default':
                    self.m_default_http_header = headers
                    continue
                self.set_host_http_header(name, headers)
        except Exception, err:
            log_error('Fetcher::load_config %s exception: %s' % (config_file, err))
    def __run(self, thread_id):
        log_info('Fetcher::run start fetch thread %d' % thread_id)
        while True:
            cur_time = time.time()
            min_wait_time = self.m_check_interval
            request = None
            cur_host = None
            cur_queue = None
            self.m_host_mutex.acquire()
            ''' find a fetch request '''
            for host, queue in self.m_host_queue.items():
                if self.qsize(host) == 0 or (self.m_host_concurency and self.m_host_fetching.get(host) == True):
                    continue
                avail_time = self.m_host_avail_time.get(host)
                if avail_time != None and avail_time > cur_time:
                    if min_wait_time > avail_time - cur_time:
                        min_wait_time = avail_time - cur_time
                    continue
                '''get an available element, not sleep'''
                min_wait_time = 0
                cur_host = host
                cur_queue = queue
                while self.qsize(host) != 0:
                    if self.m_retry_instant and self.m_host_retry.get(cur_host):
                        request = self.m_host_retry.pop(cur_host)
                    else:
                        request = cur_queue.get()
                    '''judy wether to cancel request'''
                    if self.m_need_cancel_fun:
                        try:
                            if self.m_need_cancel_fun(request.m_contex):
                                log_info('Fetcher::__run cancel request %s %s' % (request.m_url, request.m_request_data))
                                request = None
                                continue
                        except Exception, err:
                            log_error('Fetcher::__run exception: %s' % err)
                    break
                '''cur_queue has no element, try another queue'''
                if request:
                    self.m_host_fetching[cur_host] = True
                    self.m_host_avail_time[cur_host] = cur_time + self.__wait_time(cur_host)
                break
            self.m_host_mutex.release()
            if request == None or min_wait_time != 0:
                '''judge wether to exit'''
                if self.__should_exit():
                    break
                time.sleep(min_wait_time)
                continue
            log_info("Fetcher::run begin http request [1/%d] %s %s" % (self.qsize(cur_host), request.m_url, request.m_request_data))
            result = self.__http_request(request)
            self.m_host_fetching[cur_host] = False
            self.__write_snapshot(result)
            if result == None:
                if request.m_retry_num < self.m_retry_times:
                    if self.m_retry_instant:
                        self.m_host_retry[cur_host] = request
                    else:
                        cur_queue.put(request)
                else:
                    log_error('Fetcher::run remove failed url %s %s extend max_retry_time %d' % (request.m_url, request.m_request_data, self.m_retry_times))
                continue
            self.__put_result(result)
        log_info('Fetcher::run fetch thread %d end' % thread_id)
    def rpc_request(self, request_dict):
        request = FetchRequest('')
        request.__dict__.update(**request_dict)
        return self.put_request(request)
    def put_request(self, request):
        if self.m_exit:
            log_error('Fetcher::put_request failed for being ready to exit')
            return False
        if request.m_retry_num >= self.m_retry_times:
            log_error('Fetcher::put_request skip invalid url %s extend max_retry_time %d' % (request.m_url, self.m_retry_times))
            return False
        self.m_host_mutex.acquire()
        cache_result = self.__read_snapshot(request)
        if cache_result:
            self.__put_result(cache_result)
            self.m_host_mutex.release()
            return True   
        cur_host = request.host()
        cur_queue = self.m_host_queue.get(cur_host)
        if cur_queue == None:
            cur_queue = Queue.Queue(self.m_request_queue_size)
            self.m_host_queue[cur_host] = cur_queue
        self.m_host_mutex.release()
        if cur_queue.qsize() >= self.m_request_queue_size:
            log_error('Fetcher::put_request request queue full: %d' % cur_queue.qsize())
            return False
        cur_queue.put(request)
        return True
    def wait(self):
        for thread_obj in self.m_thread_list:
            thread_obj.join()
        del self.m_thread_list[:]
        if self.m_snapshot_storage:
            self.m_snapshot_storage.close()
    def exit(self):
        log_info('Fetcher::close begin ...')
        if self.m_exit:
            log_error('Fetcher::close dumplicate close.')
            return
        self.m_exit = True
        if self.m_server:
            self.m_server.exit()
        self.wait()
        if self.m_snapshot_storage:
            self.m_snapshot_storage.close()
        log_info('Fetcher::close end ...')
    '''return None represent result thread should exit'''
    def get_result(self):
        while self.m_exit == False:
            if self.m_started == False:
                log_error('Fetcher::get_result Fetcher may not be started.')
                time.sleep(self.m_check_interval)
                continue
            try:
                result = self.m_result_queue.get(False, self.m_check_interval)
                if result and result.get_header('Content-Encoding') == 'gzip':
                    result.m_response_content = gzip_decompress(result.m_response_content)
                return result
            except Queue.Empty, err:
                if self.__fetch_thread_end():
                    break
        return None

def signal_handler(fetcher):
    fetcher.exit()
    #sys.exit(0)

def handle_result(fetcher):
    while True:
        result = fetcher.get_result()
        if result == None:
            break
        fetcher.m_server.rpc_send_result(result)

def main():
    stdout = sys.stdout
    stderr = sys.stderr
    reload(sys)
    sys.stdout = stdout
    sys.stderr = stderr
    sys.setdefaultencoding('utf-8')

    config_file = 'fetch_config_default.ini'
    if len(sys.argv) != 2:
        log_info('need 1 arguments: use fetch_config_default.ini.')
    else:
        config_file = sys.argv[1]
    fetcher = Fetcher()
    fetcher.load_config(config_file)
    fetcher.start()

    result_thread = Thread(target = handle_result, args = (fetcher, ))
    result_thread.start()

    exit_checker = ExitCheck(signal_handler, fetcher)
    exit_checker.wait()
    result_thread.join()
    
if __name__ == "__main__":
    main()
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
