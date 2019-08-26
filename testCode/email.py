# -*- coding:utf-8 -*-
# __author__ = 'zhaozhiyuan'
import pandas as pd
import smtplib
from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage



import email.MIMEBase# import MIMEBase


def convert_html(title,image=0,content=None,index=False):

    text = ""
    if image:
        for t,i in zip(title,range(image)):
            text += '<p><strong>%s<strong><br /><img src="cid:image%s" /></p>'%(t,i)
    else:
        for t,c in zip(title,content):
            text += "<p><strong>%s</strong><br /></p>%s"%(t,c.to_html(index=index))
    return text


def _format_addr(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(),addr))



def send_mail(html='',att_file=None,has_att=False,att_data=False,timestamp=None,receiver=[],filename = 'list.csv'):
    style = '''<html>
             <style type="text/css">
                h2 {color: blue}
                h3 {color: blue}

                img {float:left; widht:350; height:300}
                .dataframe{ border-collapse:collapse; text-align: center;font-family:Microsoft YaHei}
                .dataframe th{ border:solid 1px #607B8B;text-align: center;background-color:#5CACEE}
                .dataframe td{ border:solid 1px #607B8B;text-align: center}
             </style>
             <body>
             <dir>
            '''
    html = style + html + "</dir></body></html>"
    sender = 'ttt@ppdai.com'

    # subject = 'python email test'
    smtpserver = 'smtp.partner.outlook.cn'
    # 设置邮箱及密码
    username = sender
    password = 'Feb2019!'
    i = 0
    if has_att:
        msgRoot = MIMEMultipart('related')
        msgText = MIMEText(html,'html','utf-8')
        msgRoot.attach(msgText)
        # for pict in att_file:

        #     fp = open(pict, 'rb')
        #     msgImage = MIMEImage(fp.read())
        #     fp.close()
        #     msgImage.add_header('Content-ID', '<image%s>'%i)
        #     msgRoot.attach(msgImage)
        #     i += 1
        contype = 'application/octet-stream'
        maintype, subtype = contype.split('/', 1)

        print "sending..."
        for data  in att_data:
            content = open(data, 'rb')
            file_msg = email.MIMEBase.MIMEBase(maintype, subtype)
            file_msg.set_payload(content.read())
            content.close( )
        # ## 设置附件头
            # basename = os.path.basename(file_name)
            file_msg.add_header('Content-Disposition','attachment', filename = filename)#修改邮件头
            msgRoot.attach(file_msg)
            email.Encoders.encode_base64(file_msg)#把附件编码
            print "email send"

    else:
        msgRoot = MIMEText(html,'html','utf8')






    msgRoot['From'] = _format_addr(u'创新业务 <%s>' % sender)
    # msgRoot['To'] = _format_addr(u'移动组成员 <%s>' % receiver)
    # msgRoot['To'] = receiver
    msgRoot['Subject'] = Header(u'商户贷-饿了么-三日未确认额度用户 <%s>' % timestamp, 'utf-8').encode()




    try:
        smtp = smtplib.SMTP()
        smtp.connect(smtpserver,587)

        smtp.ehlo()
        smtp.starttls()
        smtp.login(username, password)
        smtp.sendmail(sender, receiver, msgRoot.as_string())
        smtp.quit()
        return 'sending success'
    except Exception , e:
        return e

