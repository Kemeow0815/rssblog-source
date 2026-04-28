# coding=UTF-8

import os
import json
import time
import requests
import feedparser
import pandas
from fetch_utils import *

requests.packages.urllib3.disable_warnings()
fetch_list_source = "https://gist.githubusercontent.com/Kemeow0815/5f4b704a940c409718cf65b3b5f10102/raw/addition.json"
fetch_list = json.loads(requests.get(fetch_list_source, verify=False).text)

# 所有的rss源
rss = []
# 根据不同用户得到的rss源
rss_user = {}
# 按rss提供者分类的rss
rss_fetch_source_dir = "./__tmp__/source/"
# 举例member
rss_fetch_member_dir = "./__tmp__/member/"
# 按用户分类的rss
rss_fetch_user_dir = "./__tmp__/user/"
# 所有的rss
rss_fetch_all_dir = "./__tmp__/all/"
# 按时间年月分类的rss
rss_fetch_date_dir = "./__tmp__/date/"


def fetch():
    global rss
    
    # 支持三种格式：
    # 1. 直接是 RSS 列表（最新格式）：["rss1", "rss2"]
    # 2. 字典格式，值是 URL 字符串：{ "user": "https://.../list.json" }
    # 3. 字典格式，值是 RSS 列表：{ "user": ["rss1", "rss2"] }
    
    if isinstance(fetch_list, list):
        # 格式1：直接是 RSS 列表
        rss_list = fetch_list
        print(f"Got {len(rss_list)} RSS sources directly from list")
        
        # 清理 RSS URL
        cleaned_rss_list = []
        for r in rss_list:
            if isinstance(r, str):
                cleaned_rss_list.append(r.strip("/"))
                print(f"  - {r.strip('/')}")
            else:
                print(f"Warning: Invalid RSS URL: {r}")
        
        rss = cleaned_rss_list
        rss_user["default"] = cleaned_rss_list
        
    elif isinstance(fetch_list, dict):
        # 格式2和3：字典格式
        for key, value in fetch_list.items():
            rss_list = []
            
            if isinstance(value, str):
                # 格式2：值是 URL，需要请求获取 RSS 列表
                try:
                    print(f"Fetching RSS list from URL: {value}")
                    rss_list = json.loads(requests.get(value, verify=False, timeout=10).text)
                    print(f"Got {len(rss_list)} RSS sources from {key}")
                except Exception as e:
                    print(f"Error fetching RSS list from {value}: {e}")
                    continue
            elif isinstance(value, list):
                # 格式3：值直接是 RSS 列表
                rss_list = value
                print(f"Got {len(rss_list)} RSS sources directly from {key}")
            else:
                print(f"Warning: Unsupported format for {key}: {type(value)}")
                continue
            
            # 清理 RSS URL
            cleaned_rss_list = []
            for r in rss_list:
                if isinstance(r, str):
                    cleaned_rss_list.append(r.strip("/"))
                    print(f"  - {r.strip('/')}")
                else:
                    print(f"Warning: Invalid RSS URL in {key}: {r}")
            
            rss = rss + cleaned_rss_list
            rss_user[key] = cleaned_rss_list
    else:
        print(f"Error: Unsupported fetch_list format: {type(fetch_list)}")
        return

    # 所有源根据url去重
    rss = list({r: r for r in rss}.values())
    print(f"\nTotal unique RSS sources: {len(rss)}")
    
    # 个人源不去重, 依赖于个人维护
    # for test
    # rss = ["https://xxxx/feed/",]
    # rss_user["test"] = rss

    fetch_source(rss_fetch_source_dir, rss)
    combine_source(rss_fetch_all_dir, rss_fetch_source_dir)
    combine_member(rss_fetch_member_dir, rss_fetch_all_dir)
    split_date(rss_fetch_date_dir, rss_fetch_all_dir)
    split_user(rss_fetch_user_dir, rss_user, rss_fetch_source_dir)
