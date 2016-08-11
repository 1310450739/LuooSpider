import os
import requests
import time
from bs4 import BeautifulSoup
from faker import Factory
import queue
import threading
from sqlalchemy import Column, String, create_engine
from sqlalchemy import Integer
from sqlalchemy import Text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

fake = Factory.create()
spider_queue = queue.Queue()
Base = declarative_base()

luoo_site = 'http://www.luoo.net/music/'
luoo_site_mp3 = 'http://luoo-mp3.kssws.ks-cdn.com/low/luoo/radio%s/%s.mp3'
dist = '/Users/huanglei/Desktop/aa/'

headers = {
    'Connection': 'keep-alive',
    'User-Agent': fake.user_agent()
}

# 初始化数据库连接: 格式:'数据库类型+数据库驱动名称://用户名:口令@机器地址:端口号/数据库名'
engine = create_engine('mysql+mysqlconnector://root:root@localhost:3306/appserver')
# 创建DBSession类型:
DBSession = sessionmaker(bind=engine)


class LuooSpecial(Base):
    __tablename__ = "luoo_special"

    # create table luoo_special(number int primary key,title varchar(60),cover varchar(256),des text,count int,url varchar(256));
    number = Column(Integer(), primary_key=True)
    title = Column(String(60))
    cover = Column(String(256))
    des = Column(Text())
    count = Column(Integer())
    url = Column(String(256))


class LuooMusic(Base):
    __tablename__ = "luoo_music"

    # create table luoo_music(_id varchar(60) primary key,number int,special_id int,name varchar(60),local_path varchar(256),url_path varchar(256));
    _id = Column(String(60), primary_key=True)
    number = Column(Integer())
    special_id = Column(Integer())
    name = Column(String(60))
    local_path = Column(String(256))
    url_path = Column(String(256))


def fix_characters(s):
    for c in ['<', '>', ':', '"', '/', '\\', '|', '?', '*']:
        s = s.replace(c, '')
    return s


def spider(vol):
    url = luoo_site + vol
    print('crawling: ' + url + '\n')
    res = requests.get(url)

    if res.status_code != 200:
        return

    title = ''

    soup = BeautifulSoup(res.content, 'html.parser')
    titleSoup = soup.find('span', attrs={'class': 'vol-title'})
    if titleSoup != None:
        title = soup.find('span', attrs={'class': 'vol-title'}).text

    if len(title) == 0:
        return

    coverSoup = soup.find('img', attrs={'class': 'vol-cover'})
    if coverSoup != None:
        cover = soup.find('img', attrs={'class': 'vol-cover'})['src']

    if len(cover) == 0:
        return

    desc = soup.find('div', attrs={'class': 'vol-desc'})
    track_names = soup.find_all('a', attrs={'class': 'trackname'})
    track_count = len(track_names)
    tracks = []
    for track in track_names:
        # 12期前的音乐编号1~9是1位（如：1~9），之后的都是2位 1~9会在左边垫0（如：01~09）
        _id = str(int(track.text[:2])) if (int(vol) < 12) else track.text[:2]
        _name = fix_characters(track.text[4:])
        tracks.append({'id': _id, 'name': _name})

    phases = {
        'url': url,
        'phase': vol,  # 期刊编号
        'title': title,  # 期刊标题
        'cover': cover,  # 期刊封面
        'desc': desc,  # 期刊描述
        'track_count': track_count,  # 节目数
        'tracks': tracks  # 节目清单(节目编号，节目名称)
    }
    # print("phases:", phases)

    #spider_queue.put(phases)
    download(phases)


def download(phases):
    desTag = phases['desc']
    des = desTag.text

    # 创建session对象:
    session = DBSession()

    # 创建新LuooSpecial对象:
    luoo_special = LuooSpecial(number=phases['phase'], title=phases['title'],
                               cover=phases['cover'], des=des,
                               count=phases['track_count'], url=phases['url'])
    # 添加到session:
    session.add(luoo_special)

    for track in phases['tracks']:
        file_url = luoo_site_mp3 % (phases['phase'], track['id'])
        local_file_dict = '%s/%s' % (dist, phases['phase'])

        # 添加音乐
        luoo_music = LuooMusic(_id=str(phases['phase'] + '-' + str(track['id']) + "-" + str(time.time())),
                               number=track['id'],
                               special_id=phases['phase'], name=track['name'],
                               local_path=local_file_dict, url_path=file_url)
        session.add(luoo_music)


        # if not os.path.exists(local_file_dict):
        #     os.makedirs(local_file_dict)
        #
        # local_file = '%s/%s.%s.mp3' % (local_file_dict, track['id'], track['name'])
        # if not os.path.isfile(local_file):
        #     print('downloading: ' + track['name'])
        #     res = requests.get(file_url, headers=headers)
        #     with open(local_file, 'wb') as f:
        #         f.write(res.content)
        #         f.close()
        #     print('done.\n')
        # else:
        #     print('break: ' + track['name'])

    # 提交即保存到数据库:
    session.commit()
    # 关闭session:
    session.close()


def downloadLoop():
    print('thread %s is running...' % threading.current_thread().name)
    while True:
        if (spider_queue.qsize() <= 0):
            return
        else:
            phases = spider_queue.get()

            download(phases)
    print('thread %s ended.' % threading.current_thread().name)


def saveLuooInfo():
    pass


if __name__ == '__main__':

    vols = range(1, 10)
    for vol in vols:
        print(str(vol))
        spider(str(vol))

    print('thread %s is running...' % threading.current_thread().name)
    t = threading.Thread(target=downloadLoop, name='LoopThread')  # 创建线程 指定方法和线程名称
    t.start()  # 启动线程
    t.join()  #
    print('thread %s ended.' % threading.current_thread().name)
