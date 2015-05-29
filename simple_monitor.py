from send_mail import *
from common import *
import ConfigParser, urllib2, time, os, socket

class Monitor: 
    def __get_lst(self, value):
        value_lst = value.split(',')
        for i in range(len(value_lst)):
            value_lst[i] = value_lst[i].strip(' ')
        return value_lst
    def __init__(self, config_file_name):
        cf = ConfigParser.ConfigParser()
        cf.read(config_file_name)
        self.m_check_interval = cf.getint('Monitor/Global', 'check_interval')
        self.m_monitor_name = cf.get('Monitor/Global', 'monitor_name')
        self.m_last_check = 0
        self.m_mail_username  = cf.get('Monitor/Mail', 'user_name')
        self.m_mail_password = cf.get('Monitor/Mail', 'password')
        self.m_mail_server = cf.get('Monitor/Mail', 'server')
        self.m_mail_postfix =  cf.get('Monitor/Mail', 'postfix')
        self.m_mail_fromname = cf.get('Monitor/Mail', 'from_name')
        self.m_mail_tolist = self.__get_lst(cf.get('Monitor/Mail', 'to_list'))
        self.m_file_path_lst = []
        self.m_file_pattern = ''
        self.m_min_file_num = 0
        if cf.has_section('Monitor/NewFileCheck'):
            self.m_file_path_lst = self.__get_lst(cf.get('Monitor/NewFileCheck', 'file_path')) 
            self.m_file_pattern = cf.get('Monitor/NewFileCheck', 'file_pattern')
            self.m_min_file_num = cf.getint('Monitor/NewFileCheck', 'min_new_file_num')
        self.m_net_check_url = ''
        if cf.has_section('Monitor/NetCheck'):
            self.m_net_check_url = cf.get('Monitor/NetCheck', 'net_check_url')
            socket.setdefaulttimeout(cf.getint('Monitor/NetCheck', 'download_timeout_sec'))
    def monitor(self):
        while True:
            cur_time = time.time()
            err_msg = ''
            if cur_time - self.m_last_check > self.m_check_interval:
                log_info('monitor start checking.')
                check_beg_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.m_last_check))
                check_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(cur_time))
                '''do new file num check'''
                if self.m_min_file_num > 0:
                    for file_path in self.m_file_path_lst:
                        cnt = 0
                        file_lst = get_file_lst(self.m_file_pattern, file_path, False)
                        for file_item in file_lst:
                            if os.path.getmtime(file_item) > self.m_last_check:
                                cnt += 1
                        if cnt < self.m_min_file_num:
                            cur_err_msg = '[NewFileCheck] [FAILED] %s: new files num %d < %d\n\n' \
                                % (file_path, cnt, self.m_min_file_num)
                            if err_msg == '':
                                err_msg += 'From %s To %s:\n\n' % (check_beg_time, check_end_time)
                            err_msg += cur_err_msg
                            log_info(cur_err_msg)
                        else:
                            log_info('[NewFileCheck] [OK] new file num: %d' % cnt)
                if self.m_net_check_url.strip(' ') != '':
                    try:
                        urllib2.urlopen(self.m_net_check_url)
                        log_info('[NetCheck] [OK]')
                    except Exception, err:
                        cur_err_msg = '[NetCheck] [FAILED] %s\n\n' % err
                        if err_msg == '':
                            err_msg += 'From %s To %s:\n\n' % (check_beg_time, check_end_time)
                        err_msg += cur_err_msg
                        log_info(cur_err_msg)
                if err_msg != '':
                    send_mail(self.m_mail_tolist, self.m_monitor_name, err_msg, self.m_mail_server, \
                        self.m_mail_username, self.m_mail_password, self.m_mail_postfix, self.m_mail_fromname)
                self.m_last_check = cur_time
            if cur_time - self.m_last_check < self.m_check_interval:
                time.sleep(self.m_check_interval - (cur_time - self.m_last_check))
                
if __name__ == '__main__':
    monitor_test = Monitor('monitor_config_default.ini')
    monitor_test.monitor()
            
