import os
import requests
import time
from bs4 import BeautifulSoup
from faker import Factory
import queue
import threading

fake = Factory.create()
spider_queue = queue.Queue()

luoo_site = 'http://www.luoo.net/music/'
luoo_site_mp3 = 'http://luoo-mp3.kssws.ks-cdn.com/low/luoo/radio%s/%s.mp3'
dist = '/Users/huanglei/Desktop/aa/'

headers = {
    'Connection': 'keep-alive',
    'User-Agent': fake.user_agent()
}


def fix_characters(s):
    for c in ['<', '>', ':', '"', '/', '\\', '|', '?', '*']:
        s = s.replace(c, '')
    return s


def spider(vol):
    url = luoo_site + vol
    print('crawling: ' + url + '\n')
    res = requests.get(url)

    soup = BeautifulSoup(res.content, 'html.parser')
    title = soup.find('span', attrs={'class': 'vol-title'}).text
    cover = soup.find('img', attrs={'class': 'vol-cover'})['src']
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
    print("phases:", phases)

    spider_queue.put(phases)


def download(phases):
    for track in phases['tracks']:
        file_url = luoo_site_mp3 % (phases['phase'], track['id'])

        local_file_dict = '%s/%s' % (dist, phases['phase'])
        if not os.path.exists(local_file_dict):
            os.makedirs(local_file_dict)

        local_file = '%s/%s.%s.mp3' % (local_file_dict, track['id'], track['name'])
        if not os.path.isfile(local_file):
            print('downloading: ' + track['name'])
            res = requests.get(file_url, headers=headers)
            with open(local_file, 'wb') as f:
                f.write(res.content)
                f.close()
            print('done.\n')
        else:
            print('break: ' + track['name'])


def downloadLoop():
    print('thread %s is running...' % threading.current_thread().name)
    while True:
        if (spider_queue.qsize() <= 0):
            pass
        else:
            phases = spider_queue.get()
            download(phases)
    print('thread %s ended.' % threading.current_thread().name)


if __name__ == '__main__':

    vols = ['680', '721', '725', '720']

    for vol in vols:
        spider(vol)

    print('thread %s is running...' % threading.current_thread().name)
    t = threading.Thread(target=downloadLoop, name='LoopThread')  # 创建线程 指定方法和线程名称
    t.start()  # 启动线程
    t.join()  #
    print('thread %s ended.' % threading.current_thread().name)
