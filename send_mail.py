#!/usr/bin/env python
# -*- coding: gbk -*-
import smtplib, sys
from email.mime.text import MIMEText
from common import *
import urllib2, time

def send_mail(to_list, sub, content, main_server, mail_user, mail_pass, mail_postfix, from_name):
    me=from_name+"<"+mail_user+"@"+mail_postfix+">"
    content += '\n\n*** This email is sent by robot, please do NOT reply. ***\n\n'
    msg = MIMEText(content)
    msg['Subject'] = sub
    msg['From'] = me
    msg['To'] = ';'.join(to_list)
    try:
        s = smtplib.SMTP()
        s.connect(main_server)
        s.login(mail_user,mail_pass)
        s.sendmail(me, to_list, msg.as_string())
        s.close()
        log_info('mail to %s success.' % to_list)
        return True
    except Exception, e:
        print log_error("mail to %s exception: %s" % (to_list, e))
        return False
    
if __name__ == "__main__":
    stderr = sys.stderr
    stdout = sys.stdout
    reload(sys)
    sys.setdefaultencoding('gbk')
    sys.stderr = stderr
    sys.stdout = stdout
       
    mail_host="smtp.163.com"
    mail_user="yyclyj858888"
    mail_pass="yyclyjyyclyj"
    mail_postfix="163.com"
    mail_to_list = ['yan.yong@founder.com.cn', 'hou.jp@founder.com.cn']
    alert_title   = '【报警】192.168.15.123网络异常，请处理'
    alert_content = '请运维同事处理192.168.15.123机器的网络异常。\r\n\r\n'
    from_name = '监控服务器'
    send_mail(mail_to_list, alert_title, alert_content, mail_host, mail_user, mail_pass, mail_postfix, from_name)
