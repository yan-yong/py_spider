import urllib, urllib2, time
from common import *

class FetchRequest:
    m_url = ''
    m_request_header = {}
    m_request_data = None
    m_retry_num = 0
    m_contex = None
    def __init__(self, url, request_header = {}, request_data = None, contex = None):
        self.m_url = url
        self.m_request_header = request_header
        self.m_request_data = request_data
        self.m_contex = contex
        self.m_retry_num = 0
    def host(self):
        protocol, other = urllib.splittype(self.m_url)
        host, path = urllib.splithost(other)
        return host

class FetchResult:
    m_url  = ''
    m_response_header  = ''
    m_response_content = ''
    m_contex = None
    m_fetch_time = 0
    m_request_data = None
    def __init__(self, url, response_header, content, contex, request_data = None):
        self.m_url  = url
        self.m_response_header = response_header
        self.m_response_content = content
        self.m_contex = contex
        self.m_fetch_time = time.time()
        self.m_request_data = request_data
    def __str__(self):
        if self.m_url == None:
            return None
        result  = self.m_url + '\r\n'
        result += 'FetchTime: %d\r\n' % self.m_fetch_time
        if self.m_response_header:
            result += self.m_response_header
        if self.m_response_content:
            result += self.m_response_content
        return result
    def load_str(self, str_content):
        lines = str_content.split('\r\n')
        if len(lines) < 3:
            log_error('FetchResult::__init__ error.')
        self.m_url = lines[0]
        kv = lines[1].split(':')
        if kv[0] == 'FetchTime':
            self.m_fetch_time = int(kv[1])
        is_header = True
        for line in lines[2:]:
            if is_header:
                self.m_response_header += line + '\r\n'
            else:
                self.m_response_content += line + '\r\n'
            if len(line) == 0:
                is_header = False
    def get_header(self, header):
        lines = self.m_response_header.split('\r\n')
        for line in lines:
            collums = line.split(':')
            if len(collums) != 2 or collums[0].strip(' ') != header.strip(' '):
                continue
            return collums[1].strip(' ')
        return None
