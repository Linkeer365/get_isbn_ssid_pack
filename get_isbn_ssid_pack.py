import os
import sys
import re

import socket

import urllib3.exceptions

import subprocess

import redis

from PIL import Image

import time

import math

import requests
from lxml import etree


import mysql.connector
import pymysql

import pandas as pd

xls_path=r"D:\get_isbn_ssid_pack\publisher_identifiers.xlsx"

ucdrs_url="http://book.ucdrs.superlib.net/search?sw="

ssid_pack_path=r"D:\AllDowns\ssid_packs\ssid_packs.txt"

# no matter good or bad...
isbn_already_path=r"D:\AllDowns\ssid_packs\isbn_already.txt"

isbn_exist_error_path=r"D:\get_isbn_ssid_pack\isbn_exist_error.txt"

isbn_after_verify_path=r"D:\get_isbn_ssid_pack\after_verify.txt"

old_one2='Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)'

ua_list = [ 
            'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
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

old_one="Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"

ua_list2=[  
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
    "User-Agent": ua_list[ua_idx]
}

yzm_img_path=r"D:\get_isbn_ssid_pack\yzm.png"


def is_isbn_exist(s,isbn):

    check_str=" 0 种"

    assert isinstance(isbn,str)
    url=ucdrs_url+isbn
    try:

        page_text=s.get(url,headers=headers,timeout=15).text
        assert page_text!=""
        time.sleep(1)

    # except AssertionError or requests.exceptions.RequestException or requests.exceptions.ProxyError:
    except socket.timeout or AssertionError or requests.exceptions.ProxyError or requests.exceptions.ConnectionError or ConnectionResetError or urllib3.exceptions.MaxRetryError:
        print("Phase1: Connection Error or Proxy Error.")


        global ua_idx

        ua_idx+=1

        if ua_idx==len(ua_list):
            ua_idx=0


        headers["User-Agent"]=ua_list[ua_idx]

        s.cookies.clear()

        time.sleep(60)

        print("Force to sleep 1min...")

        isbn(s,isbn)
    
    # except requests.exceptions.ProxyError:


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
        
        print("capcha")

        # ua_idx+=1

        # if ua_idx==len(ua_list2):
            # ua_idx=0

        # headers["User-Agent"]=ua_list2[ua_idx]


        s.cookies.clear()

        sys.exit(-1)

        time.sleep(90)

        print("sleep 90s...")

        is_isbn_exist(s,isbn)




# bad_isbn="9782220279398"
# is_isbn_exist(bad_isbn)
# sys.exit(0)

def get_ssid_packs(s,isbn,is_exist=True):
    '''
    pack format: (isbn,ssid,ssid_info,ucdrs_link)

    :param isbn:
    :param is_exist:
    :return: packs
    '''


    assert isinstance (isbn, str)

    # max_page_num=10
    max_page_num=1

    packs = []

    for page_num in range(1,max_page_num+1):
        url = ucdrs_url + isbn + f"&Pages={page_num}"
        try:

            page_text = s.get (url, headers=headers,timeout=15).text
            assert page_text!=""
            time.sleep(1)

        except AssertionError or requests.exceptions.ProxyError or ConnectionResetError or urllib3.exceptions.MaxRetryError or requests.exceptions.ConnectionError:
            print("Phase2: Connection Error or Proxy")


            s.cookies.clear()

            print("Force to sleep 1min...")

            time.sleep(60)

            get_ssid_packs(s,isbn)


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

        # print("ssids:\t",ssids)
        # print("ssid-infos:\t",ssid_infos)

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
            choice_idxs=option_idxs

        # ucdrs_links=[]


        for choice_idx in choice_idxs:
            choose_info=ssid_infos[choice_idx]
            choose_ssid=ssids[choice_idx]
            ucdrs_link=ssids_links[choose_ssid]
            # ucdrs_links.append(ucdrs_link)
            print("isbn: ",isbn)
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


def main():

    is_publishers_finished=1

    if not is_publishers_finished:
        write_publishers_db (xls_path)

    countris_codes={"China":"7"}

    state_identifier=countris_codes["China"]

    tb_name = "China2020"

    ppi_select_sql=f"SELECT publisher_identifiers,publisher_old_indentifiers FROM {tb_name}"

    db_name='publishers'
    db=pymysql.connect('localhost','root','cc',db_name)
    cursor=db.cursor()


    cursor.execute(ppi_select_sql)

    res=cursor.fetchall()

    # res=[each[0] for each in cursor.fetchall()]

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
    # proxy = get_random_proxy ()

    with open(isbn_already_path,"r",encoding="utf-8") as f:
        already_isbn_set=set([each.strip("\n") for each in f.readlines() if each!='\n'])
    
    with open(ssid_pack_path,"r",encoding="utf-8") as f:
        old_packs=[tuple(each.strip("\n").split("$\t")) for each in f.readlines() if each!="\n"]
    
    # print(old_packs[0])

    all_packs = []

    all_packs.extend(old_packs)

    for publisher_identifier,publisher_old_indentifier in res:

        # get title_identifier num
        publisher_identifiers=[]
        if publisher_old_indentifier=='0':
            publisher_identifiers=[publisher_identifier]
        else:
            # 上下两个 publisher_identifier 意义不同，注意一下!
            publisher_identifiers=[publisher_identifier]+publisher_identifier.split(",")
        
        print("publisher_identifiers:",publisher_identifiers)

        cnt=0

        for publisher_identifier in publisher_identifiers:
            max_ti_len=get_max_ti_len(publisher_identifier)
            for num in range(0,10**max_ti_len):
                full_ti=get_full_ti_str(num,max_ti_len)
                isbn13=ISBN13(state_identifier=state_identifier,publish_identifier=publisher_identifier,
                            title_identifier=full_ti)
                isbn=isbn13.get_full_without_hyphen()

                if isbn in already_isbn_set:
                    # print("already.")
                    continue

                
                if cnt%15==0:
                    
                    s.cookies.clear()

                start=time.time()

                is_exist=is_isbn_exist(s,isbn)
                cnt+=1
                if is_exist:
                    packs=get_ssid_packs(s,isbn)
                    all_packs.extend(packs)
                end=time.time()

                print("Runtime:",end-start)
                
                with open(isbn_already_path,"a",encoding="utf-8") as f:
                    f.write("\n")
                    f.write(isbn)
                if cnt%2000==0:
                    print("sleep for 5s...")
                    time.sleep(5)
                    s.cookies.clear()

    insert_packs_sql2=  f"INSERT INTO {tb_name2} " \
                        f"(isbn,ssid,ssid_info,ucdrs_link)" \
                        f"VALUES (%s,%s,%s,%s)"
    cursor2.executemany(insert_packs_sql2,all_packs)

    db2.commit()

    print(cursor2.rowcount,"条 已插入！")

    print("db written.")


    print("all done.")

if __name__ == '__main__':
    main()
