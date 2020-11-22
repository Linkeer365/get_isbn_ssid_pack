import os
import sys
import re

import subprocess

import redis

from PIL import Image

import time

import math

import requests
from lxml import etree

import pymysql
import mysql.connector

import pandas as pd

xls_path=r"D:\get_isbn_ssid_pack\publisher_identifiers.xlsx"

ucdrs_url="http://book.ucdrs.superlib.net/search?sw="

ssid_pack_path=r"D:\AllDowns\ssid_packs\ssid_packs.txt"

isbn_exist_error_path=r"D:\get_isbn_ssid_pack\isbn_exist_error.txt"

isbn_after_verify_path=r"D:\get_isbn_ssid_pack\after_verify.txt"

ua_list = [ 'Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)',
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2919.83 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2866.71 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19582',
            'Mozilla/5.0 (compatible; U; ABrowse 0.6; Syllable) AppleWebKit/420+ (KHTML, like Gecko)',
           'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; Acoo Browser 1.98.744; .NET CLR   3.5.30729)',
           'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; Acoo Browser; GTB5; Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;   SV1) ; InfoPath.1; .NET CLR 3.5.30729; .NET CLR 3.0.30618)',
           'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; SV1; Acoo Browser; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; Avant Browser)',
           'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1;   .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)',
           'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; GTB5; Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1) ; Maxthon; InfoPath.1; .NET CLR 3.5.30729; .NET CLR 3.0.30618)',
           'Mozilla/4.0 (compatible; Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; Acoo Browser 1.98.744; .NET CLR 3.5.30729); Windows NT 5.1; Trident/4.0)',
           'Mozilla/4.0 (compatible; Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB6; Acoo Browser; .NET CLR 1.1.4322; .NET CLR 2.0.50727); Windows NT 5.1; Trident/4.0; Maxthon; .NET CLR 2.0.50727; .NET CLR 1.1.4322; InfoPath.2)',
           ]

ua_list2=[  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
           "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
           "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
           "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
           "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
           "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
           "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
           "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
           "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
           "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
           "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
           "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
           "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
           "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
           "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
           "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
           "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
           "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"]

proxypool_url = 'http://127.0.0.1:5555/random'

ua_idx=0

headers={
    "User-Agent": ua_list2[ua_idx]
}

yzm_img_path=r"D:\get_isbn_ssid_pack\yzm.png"

# s=requests.session()

# redis login

def redis_login():
    # import redis
    pool = redis.ConnectionPool (decode_response=True)
    db = redis.Redis (connection_pool=pool, host="localhost", port=6379, password='xm111737')
    print("redis login!")

redis_login()

# boot the proxypool

proxypool_path=r"D:\get_isbn_ssid_pack\ProxyPool\run.py"

boot_proxy_comm=f"python \"{proxypool_path}\""

# os.popen(boot_proxy_comm)

# https://stackoverflow.com/questions/546017/how-do-i-run-another-script-in-python-without-waiting-for-it-to-finish
# 设置为后台运行...

subprocess.Popen([sys.executable,proxypool_path],stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

time.sleep(5)

proxy_status=requests.get(proxypool_url).status_code

if proxy_status==200:
    print("proxypool boot!")

def get_random_proxy():
    """
    get random proxy from proxypool
    :return: proxy
    """

    proxy_str=requests.get(proxypool_url).text

    assert ":" in proxy_str

    proxy=proxy_str.strip()

    print("proxy change!")
    print("proxy:",proxy)

    return proxy


def is_isbn_exist(s,isbn,proxy):

    # proxy = "http://" + proxy

    proxies = { 'http': proxy,
                "https": proxy
                }

    check_str=" 0 种"

    assert isinstance(isbn,str)
    url=ucdrs_url+isbn
    try:
        # page_text=s.get(url,headers=headers,proxies=proxies,timeout=120).text
        page_text=s.get(url,headers=headers,proxies=proxies,timeout=60).text
    except requests.exceptions.Timeout:
        print("Timeout!")
    except requests.exceptions.RequestException:
        print("Phase1: Connection Error!")

        proxy=get_random_proxy()
        # s=requests.session()

        s.cookies.clear()

        isbn(s,isbn,proxy)

    # time.sleep(3)
    html=etree.HTML(page_text)

    found_zero_patt="//div[@id='searchinfo']/b//text()"

    find=html.xpath(found_zero_patt)

    if find:
        find=find[-1]
        if check_str in find:
            return False
        else:
            return True
    else:
        # with open(isbn_exist_error_path,"a",encoding="utf-8") as f:
        #     f.write(f"isbn: {isbn}\n")
        #     f.write(page_text)
        #     f.write("\n")
        
        print("capcha")

        global ua_idx

        ua_idx+=1

        if ua_idx==len(ua_list):
            ua_idx=0

        # assert ua_idx<=len(ua_list)-1

        # global headers

        # headers["User-Agent"]=ua_list2[ua_idx]

        # print("ua now:",ua_list2[ua_idx])

        # print("sleep for 60s...")

        # time.sleep(60)

        # s=requests.session()

        s.cookies.clear()

        # 紧急切换proxy

        proxy=get_random_proxy()

        # headers={ua_list[ua_idx]}

        is_isbn_exist(s,isbn,proxy)




        # 验证过程是逃不掉的！
        
        # print("start sleeping for 30 sec...")

        # # if 'antispiderShowVerify.ac' in s.url:

        # time.sleep(30)
        # s=requests.session()

        # is_isbn_exist(s,isbn)

        # 验证码片段

        # huanyizhang=0
        # cnt=0
        #
        # while huanyizhang==1 or cnt==0:
        #
        #     cnt+=1
        #     huanyizhang=0
        #
        #     yzm_url="http://book.ucdrs.superlib.net/antispiderShowVerify.ac"
        #     yzm_page_text=s.get(yzm_url,headers=headers).text
        #     yzm_html=etree.HTML(yzm_page_text)
        #     yzm_img_link_patt="//span[@class='yzmImg']/img//@src"
        #
        #     yzm_img_link_head="http://book.ucdrs.superlib.net"
        #
        #     yzm_img_link_tail=yzm_html.xpath(yzm_img_link_patt)[0]
        #
        #     yzm_img_link=yzm_img_link_head+yzm_img_link_tail
        #
        #     print("yzm pic link:",yzm_img_link)
        #
        #     yzm_img=s.get(yzm_img_link,headers=headers).content
        #
        #     with open(yzm_img_path,"wb") as f:
        #         f.write(yzm_img)
        #
        #     img=Image.open(yzm_img_path)
        #     img.show()
        #
        #     yzm=input("Your input:")
        #
        #     if yzm=="":
        #         # 回车键就是默认换一张
        #         huanyizhang=1
        #         continue
        #     payload={'ucode':yzm}
        #
        #     process_yzm_url=yzm_img_link_head+"/processVerify.ac?ucode=asyc"
        #
        #     checker_text=s.get(process_yzm_url,headers=headers,params=payload).text
        #
        #     with open(isbn_after_verify_path,"a",encoding="utf-8") as f:
        #             f.write(f"isbn:{isbn}")
        #             f.write("\n")
        #             f.write(checker_text)
        #             f.write("\n")
        #
        #
        #
        #     r=requests.get(url,headers=headers)
        #
        #     checker_url=r.url
        #     checker_text=r.text
        #
        #     print("checker url:",checker_url)
        #
        #     if not 'antispiderShowVerify.ac' in checker_url:
        #
        #         print('yanzhengtongguo')
        #
        #         time.sleep(60)
        #
        #         s=requests.session()
        #
        #         is_isbn_exist(s,isbn)
        #     else:
        #         print("gan!")
        #         huanyizhang=1



# bad_isbn="9782220279398"
# is_isbn_exist(bad_isbn)
# sys.exit(0)

def get_ssid_packs(s,isbn,proxy,is_exist=True):
    '''
    pack format: (isbn,ssid,ssid_info,ucdrs_link)

    :param isbn:
    :param is_exist:
    :return: packs
    '''

    proxies = {'http': 'http://' + proxy}

    assert isinstance (isbn, str)

    # max_page_num=10
    max_page_num=1

    packs = []

    for page_num in range(1,max_page_num+1):
        url = ucdrs_url + isbn + f"&Pages={page_num}"
        try:
            page_text = s.get (url, headers=headers,proxies=proxies,timeout=60).text
        except requests.exceptions.Timeout:
            print("Timeout!")
        except requests.exceptions.RequestException:
            print("Phase2: Connection Error!")

            proxy=get_random_proxy()

            s.cookies.clear()

            get_ssid_packs(s,isbn,proxy)





        # time.sleep(3)

        html = etree.HTML (page_text)

        checker_patt="//form[@name='formid']/table[@class='book1']"

        finds=html.xpath(checker_patt)

        if not finds:
            print(f"End at Page {page_num-1}")
            break

        # 我抄我自己，具体见 repo get_ucdrs_links_from_douban_series/get_ucdrs_links_from_douban_series.py Line 192

        ucdrs_link_patt="//input[starts-with(@id,'url')]//@value"
        ssid_patt="//input[starts-with(@id,'ssid')]//@value"

        links=html.xpath(ucdrs_link_patt)
        ssids=html.xpath(ssid_patt)

        ssids_links={ssid:link for ssid,link in zip(ssids,links) if ssid!=""}
        print(ssids_links)

        if ssids==[""] or bool(ssids)==0 or ssids_links=={}:
            continue

        info_patt_node="//span[@class='fc-green']"
        ssid_info_nodes = html.xpath (info_patt_node)

        ssid_infos=[]

        for each_node in ssid_info_nodes:
            info=each_node.xpath("string(.)")
            print("info: ",info)
            ssid_infos.append(info)

        print("ssids:\t",ssids)
        print("ssid-infos:\t",ssid_infos)

        option_idx=0
        option_idxs=[]
        for each_idx,each_info in enumerate(ssid_infos,1):
            if ssids[each_idx-1]:
                print(each_info,"\t\t\t",each_idx)
                option_idx=each_idx-1
                option_idxs.append(option_idx)
        if len(ssids_links)==1:
            choice_idxs=[option_idx]
        elif len(ssids_links)>=2:
            # choice_idxs_in=input("Your choice(multiple is ok, split by ,):")
            # choice_idxs=[int(each)-1 for each in choice_idxs_in.split(",")]
            choice_idxs=option_idxs

        # ucdrs_links=[]


        for choice_idx in choice_idxs:
            choose_info=ssid_infos[choice_idx]
            choose_ssid=ssids[choice_idx]
            ucdrs_link=ssids_links[choose_ssid]
            # ucdrs_links.append(ucdrs_link)
            print("ucdrs link:",ucdrs_link)
            print("ssid:",choose_ssid)
            print("ssid_info:",choose_info)

            if isinstance(choose_ssid,int):
                ssid=str(choose_ssid)

            pack=(isbn,choose_ssid,choose_info,ucdrs_link)

            pack_s="$\t".join(pack)

            with open(ssid_pack_path,"a",encoding="utf-8") as f:
                f.write('\n')
                f.write(pack_s)
                f.write('\n')

            packs.append(pack)

    return packs




# bad_isbn="9787544242516"
# multi_isbn="9787108016386"
# get_ssid_pack(multi_isbn)
# sys.exit(0)



def get_check_digit(initpart):
    # https://wenku.baidu.com/view/b03803a59c3143323968011ca300a6c30c22f1cd.html
    if isinstance(initpart, int):
        initpart = str(initpart)
    assert len(initpart) == 12
    odd_place_digits = [int(val) for idx, val in enumerate(initpart, 1) if idx % 2 == 1]
    even_place_digits = [int(val) for idx, val in enumerate(initpart, 1) if idx % 2 == 0]
    weighted_sum = sum(odd_place_digits) * 1 + sum(even_place_digits) * 3
    # print("Weg Sum",weighted_sum)
    modOf10 = divmod(weighted_sum, 10)[1]
    check_digit = 10 - modOf10
    assert 0 <= check_digit <= 10
    # 强行归零
    if check_digit==10:
        check_digit=0
    return str(check_digit)

# print(get_check_digit("978701000041"))
# sys.exit(0)


def get_max_ti_len(publisher_identifier):
    # https://baike.baidu.com/item/%E5%9B%BD%E9%99%85%E6%A0%87%E5%87%86%E4%B9%A6%E5%8F%B7/3271472?fromtitle=ISBN&fromid=391662&fr=aladdin
    maxlen = 8  # 13-4-1
    if isinstance(publisher_identifier, int):
        publisher_identifier = str(publisher_identifier)
    max_ti_len = maxlen - len(publisher_identifier)

    assert 1 <= max_ti_len <= maxlen
    return max_ti_len

def get_full_ti_str(num,max_ti_len):
    if isinstance(num,int):
        num=str(num)
    full_str=num.zfill(max_ti_len)
    assert len(full_str)==max_ti_len
    return full_str

class ISBN13:

    book_product_code="978"

    def __init__(self,state_identifier,publish_identifier,title_identifier):
        self.state_indentifier=state_identifier
        self.publish_identifier=publish_identifier
        self.title_identifier=title_identifier

        init_part=f"{ISBN13.book_product_code}{state_identifier}{publish_identifier}{title_identifier}"

        self.check_digit=get_check_digit(init_part)

        assert len(init_part+self.check_digit)==13

    def get_full_with_hyphen(self):
        return f"{ISBN13.book_product_code}-{self.state_indentifier}-{self.publish_identifier}-{self.title_identifier}-{self.check_digit}"
    def get_full_without_hyphen(self):
        return f"{ISBN13.book_product_code}{self.state_indentifier}{self.publish_identifier}{self.title_identifier}{self.check_digit}"


def write_publishers_db(xls_path):
    df=pd.read_excel(xls_path,names=None,usecols=[0,3,4],dtype=str) # 1 base
    vals=df.values.tolist()
    packs=[]
    for each in vals:
        if isinstance(each[0],str):
            if isinstance(each[2],float) and math.isnan(each[2]):
                pack=(each[0],each[1],0)
            elif isinstance(each[2],str) and (not "曾用出版社编号" in each[2]):
                pack = (each[0], each[1], 0)
            else:
                old_indentifiers=each[2].replace("曾用出版社编号","").split("、")
                old_indentifiers_s=",".join(old_indentifiers)
                pack=(each[0],each[1],old_indentifiers_s)
            packs.append(pack)

    # please ensure that we have database called publishers

    db_name='publishers'

    db=pymysql.connect('localhost','root','cc',db_name)

    cursor=db.cursor()

    try:
        tb_name="China2020"
        create_table_sql=f"CREATE TABLE {tb_name} " \
            f"(id INT AUTO_INCREMENT PRIMARY KEY," \
            f"publisher_name VARCHAR (255), " \
            f"publisher_identifiers VARCHAR (255), " \
            f"publisher_old_indentifiers VARCHAR (255))"

        cursor.execute(create_table_sql)

    except pymysql.err.OperationalError:
        pass

    insert_packs_sql=f"INSERT INTO {tb_name} " \
        f"(publisher_name,publisher_identifiers,publisher_old_indentifiers)" \
        f"VALUES (%s,%s,%s)"

    cursor.executemany(insert_packs_sql,packs)

    db.commit()

    print("db written.")


    # publishers=list(reversed([val[0] for val in vals if isinstance(val[0],str)]))
    # publisher_identifiers=list(reversed([val[1] for val in vals if isinstance(val[1],str)]))
    # publisher_old_identifiers=list(reversed([val[2] for val in vals if isinstance(val[2],str) and '曾用出版社编号' in val[2]]))
    #
    # print(publishers)
    # print(publisher_identifiers)
    # print(publisher_old_identifiers)


def main():

    is_publishers_finished=1

    if not is_publishers_finished:
        write_publishers_db (xls_path)

    countris_codes={"China":"7"}

    state_identifier=countris_codes["China"]

    tb_name = "China2020"

    ppi_select_sql=f"SELECT publisher_identifiers FROM {tb_name}"

    db_name='publishers'
    db=pymysql.connect('localhost','root','cc',db_name)
    cursor=db.cursor()


    cursor.execute(ppi_select_sql)

    res=[each[0] for each in cursor.fetchall()]

    db_name2='ucdrs_books'
    db2=pymysql.connect('localhost','root','cc',db_name2)
    cursor2=db2.cursor()

    try:
        tb_name2='with_ssids'
        create_table_sql2 = f"CREATE TABLE {tb_name2} " \
            f"(id INT AUTO_INCREMENT PRIMARY KEY," \
            f"isbn VARCHAR (255), " \
            f"ssid VARCHAR (255), " \
            f"ssid_info VARCHAR (255)," \
            f"ucdrs_link VARCHAR (255)" \
            f")"
        cursor2.execute(create_table_sql2)
    except pymysql.err.OperationalError:
        pass

    s=requests.session()
    proxy = get_random_proxy ()

    for publisher_identifier in res:

        all_packs = []

        # get title_identifier num

        max_ti_len=get_max_ti_len(publisher_identifier)

        cnt=0

        for num in range(0,10**max_ti_len):
            full_ti=get_full_ti_str(num,max_ti_len)
            isbn13=ISBN13(state_identifier=state_identifier,publish_identifier=publisher_identifier,
                          title_identifier=full_ti)
            isbn=isbn13.get_full_without_hyphen()

            
            if cnt==15:
                
                # 每15次就大更新一次

                # headers["User-Agent"]=ua_list2[ua_idx]
                proxy=get_random_proxy()

                # s=requests.session()

                s.cookies.clear()

                # time.sleep(20)
                # print("now we sleep for 20s...")
                cnt=0
            is_exist=is_isbn_exist(s,isbn,proxy)
            cnt+=1
            if is_exist:
                packs=get_ssid_packs(s,isbn,proxy)
                all_packs.extend(packs)
        insert_packs_sql2=  f"INSERT INTO {tb_name2} " \
                            f"(isbn,ssid,ssid_info,ucdrs_link)" \
                            f"VALUES (%s,%s,%s,%s)"
        cursor2.executemany(insert_packs_sql2,all_packs)

        db2.commit()

        print(cursor2.rowcount,"条 已插入！")

        time.sleep(5)

    print("all done.")

if __name__ == '__main__':
    main()








    #



