# -*- coding: utf-8 -*-
import time, urllib, sys, thread, StringIO, gzip, signal, os, re

def my_strip(str):
    white_space_lst = ['\n', '\r', '\t', ' ']
    res = ''
    beg = 0
    while beg < len(str):
        if str[beg] not in white_space_lst:
            break
        beg += 1
    end = len(str) - 1
    while end >= 0:
        if str[end] not in white_space_lst:
            break
        end -= 1
    if beg >= len(str) or end < 0 or beg >= end:
        return ''
    return str[beg:end + 1]

def my_split(str, sep):
    result = []
    array = str.split(sep)
    for item in array:
        cur = my_strip(item)
        if len(cur) > 0:
            result.append(cur)
    return result

def log_error(str):
    time_str = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    sys.stderr.write('[%s] [%x] [error] %s\n' % (time_str, thread.get_ident(), str))
    sys.stderr.flush()
    #sys.stdout.flush()
    #sys.stderr.flush()

def log_info(str):
    time_str = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    sys.stdout.write('[%s] [%x] [info] %s\n' % (time_str, thread.get_ident(), str))
    sys.stdout.flush()

def my_urlencode(str):
    str = urllib.urlencode({'key': str})
    key, value = str.split('=')
    return value

def gzip_compress(content):
    str_io = StringIO.StringIO()
    zfile = gzip.GzipFile(mode='wb', fileobj=str_io)
    try:
        zfile.write(content)
    finally:
        zfile.close()
    return str_io.getvalue()

def gzip_decompress(content):
    str_io = StringIO.StringIO(content)
    zfile = gzip.GzipFile(mode='rb', fileobj = str_io)
    try:
        data = zfile.read()
    finally:
        zfile.close()
    return data

def debug_log():
    """Return the frame object for the caller's stack frame."""
    try:
        raise Exception
    except:
        f = sys.exc_info()[2].tb_frame.f_back
        print 'debug_log %d %s:%d' % (thread.get_ident(), f.f_code.co_name, f.f_lineno)

def get_local_ip(ifname):
    try:
        import socket, fcntl, struct 
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
        inet = fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15])) 
        ret = socket.inet_ntoa(inet[20:24]) 
        return ret
    except:
        import socket
        ip = socket.gethostbyname(socket.gethostname())
        return ip
'''file_pattern should be regex pattern'''
def get_file_lst(file_pattern, directory, recursive = True):
    res = []
    for parent,dirnames,filenames in os.walk(directory):
        for file_name in filenames:
            if re.match(file_pattern, file_name):
                res.append(parent+'/'+file_name)
        if not recursive:
            break
    return res

class ExitCheck:
    m_exit = False
    m_exit_callback = None
    m_exit_param = ()
    def __init__(self, exit_callback, *param):
        self.m_exit_callback = exit_callback
        self.m_exit_param = param
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    def signal_handler(self, signum, frame):
        if self.m_exit:
            log_error('process recv dumplicate exit request.')
            return
        self.m_exit = True
        log_info('process recv quit signal ... ')
        if self.m_exit_callback:
            self.m_exit_callback(*self.m_exit_param)
    def wait(self):
        while self.m_exit == False:
            time.sleep(1)
        log_info('process exit success.')
