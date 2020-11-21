import os
import sys
import re

import selenium
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

from selenium.webdriver.support.ui import WebDriverWait

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# import KEYS
from selenium.webdriver.common.keys import Keys



from PIL import Image

import time

import math

import requests
from lxml import etree

import pymysql
import mysql.connector

import pandas as pd

xls_path=r"D:\AllDowns\uhasnq\publisher_identifiers.xlsx"

ucdrs_url="http://book.ucdrs.superlib.net/search?sw="

ssid_pack_path=r"D:\AllDowns\uhasnq\ssid_packs.txt"

isbn_exist_error_path=r"D:\AllDowns\uhasnq\isbn_exist_error.txt"

isbn_after_verify_path=r"D:\AllDowns\uhasnq\after_verify.txt"

headers={
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36"
}

yzm_img_path=r"D:\AllDowns\uhasnq\yzm.png"

firefox_path=r"D:\selenium_drivers\geckodriver.exe"


options = Options()
options.headless = False

driver=webdriver.Firefox(options=options,executable_path=firefox_path)

def find_element_by_xpath2(patt):
    max_delay=5
    try:
        return WebDriverWait(driver,max_delay).until(EC.presence_of_element_located((By.XPATH, patt)))
    except selenium.common.exceptions.TimeoutException:
        return None

def find_elements_by_xpath2(patt):
    max_delay=5
    try:
        WebDriverWait (driver, max_delay).until (EC.presence_of_element_located ((By.XPATH, patt)))
        return driver.find_elements_by_xpath(patt)
    except selenium.common.exceptions.TimeoutException:
        return []


def is_isbn_exist(isbn):

    check_str=" 0 种"

    assert isinstance(isbn,str)
    url=ucdrs_url+isbn

    driver.get(url)

    # time.sleep(1)
    # html=etree.HTML(page_text)

    found_zero_patt="//div[@id='searchinfo']/b"

    # find=html.xpath(found_zero_patt)

    finds=find_elements_by_xpath2(found_zero_patt)

    if finds:
        find=finds[-1]
        if check_str in find.text:
            return False
        else:
            return True
    else:

        if 'antispiderShowVerify.ac' in driver.current_url:
            verify_link=driver.current_url
            print("capcha")

        img_link_patt="//span[@class='yzmImg']/img"
        img_link=find_elements_by_xpath2(img_link_patt)[0].get_attribute("src")

        print("img_link:",img_link)

        # yzm_img_link_head = "http://book.ucdrs.superlib.net"
        yzm_img_link = img_link

        driver.get(yzm_img_link)
        driver.save_screenshot(yzm_img_path)

        img=Image.open(yzm_img_path)
        img.show()

        yzm = input("Your input:")

        driver.get(verify_link)

        inputBox_patt="//input[@id='ucode' and @name='ucode']"
        inputBox=find_element_by_xpath2(inputBox_patt)
        inputBox.send_keys(yzm)

        inputBox.send_keys(Keys.ENTER)

        is_isbn_exist(isbn)


# bad_isbn="9782220279398"
# is_isbn_exist(bad_isbn)
# sys.exit(0)

def get_ssid_packs(isbn,is_exist=True):
    '''
    pack format: (isbn,ssid,ssid_info,ucdrs_link)

    :param isbn:
    :param is_exist:
    :return: packs
    '''
    assert isinstance (isbn, str)

    max_page_num=10

    packs = []

    for page_num in range(1,max_page_num+1):
        url = ucdrs_url + isbn + f"&Pages={page_num}"

        driver.get(url)

        # page_text = s.get (url, headers=headers).text

        # time.sleep(1)

        # html = etree.HTML (page_text)

        checker_patt="//form[@name='formid']/table[@class='book1']"

        # finds=html.xpath(checker_patt)

        find=find_element_by_xpath2(checker_patt)

        if not find:
            print(f"End at Page {page_num-1}")
            break

        # 我抄我自己，具体见 repo get_ucdrs_links_from_douban_series/get_ucdrs_links_from_douban_series.py Line 192

        ucdrs_link_patt="//input[starts-with(@id,'url')]"
        ssid_patt="//input[starts-with(@id,'ssid')]"

        # links=html.xpath(ucdrs_link_patt)
        # ssids=html.xpath(ssid_patt)

        links=[each.get_attribute('value') for each in find_elements_by_xpath2(ucdrs_link_patt)]
        ssids=[each.get_attribute('value') for each in find_elements_by_xpath2(ssid_patt)]

        ssids_links={ssid:link for ssid,link in zip(ssids,links) if ssid!=""}
        print(ssids_links)

        if ssids==[""] or bool(ssids)==0 or ssids_links=={}:
            continue

        info_patt_node_patt="//span[@class='fc-green']"
        # ssid_info_nodes = html.xpath (info_patt_node)

        ssid_info_nodes=find_elements_by_xpath2(info_patt_node_patt)

        ssid_infos=[]

        for each_node in ssid_info_nodes:
            info=each_node.text
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

    for publisher_identifier in res:

        all_packs = []

        # get title_identifier num

        max_ti_len=get_max_ti_len(publisher_identifier)

        for num in range(0,10**max_ti_len):
            full_ti=get_full_ti_str(num,max_ti_len)
            isbn13=ISBN13(state_identifier=state_identifier,publish_identifier=publisher_identifier,
                          title_identifier=full_ti)
            isbn=isbn13.get_full_without_hyphen()
            is_exist=is_isbn_exist(isbn)
            if is_exist:
                packs=get_ssid_packs(isbn)
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



