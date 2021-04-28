from pydub import AudioSegment
from pydub.silence import split_on_silence
import sys
import os
import time
import re
import subprocess
import pymysql
from aip import AipSpeech

APP_ID = '24032991'
API_KEY = '5SYyz0GupHEGv8hxZc82aoXr'
SECRET_KEY = 'afFWr3bIlAb2wCNTLkPKHPL8CBadACK1'

def main():

    #1、创建数据库连接对象
    connect = pymysql.connect(
        # host：表示主机地址
        # 127.0.0.1 本机ip
        # 172.16.21.41 局域网ip
        # localhost (local：本地 host：主机 合在一起为本地主机的意思)
        # host表示mysql安装的地址
        host="199.19.105.117",
        user="root",
        passwd="ostrovsky_z",
        # mysql默认的端口号是3306
        port=3306,
        # 数据库名称
        db="videos"
    )

    #2、创建游标，用于操作表
    cursor = connect.cursor()
    cursor.execute("UPDATE video SET content = 'sssssssss' WHERE video_id = 'YPHcGZt4RoM';")
    connect.commit()
    return
                    
    # 将录音文件拆分成适合百度语音识别的大小
    # chuck_paths = prepare_for_baiduaip(name,sound,channel,silence_thresh,min_silence_len,length_limit,abandon_chunk_len,joint_silence_len)

    subprocess.run('ffmpeg -y -i "%s" -acodec pcm_s16le -f s16le -ac 1 -ar 16000 "%s" '%('chunks/UC5xunxPS6oZ1zzKufgREFuA/sAot0WjBG8E_0000.mp4', 'chunks/UC5xunxPS6oZ1zzKufgREFuA/sAot0WjBG8E_0000.pcm'), shell=True)
    client = AipSpeech(APP_ID, API_KEY, SECRET_KEY)

    # 识别本地文件
    result = client.asr(get_file_content('chunks/UC5xunxPS6oZ1zzKufgREFuA/sAot0WjBG8E_0000.pcm'), 'pcm', 16000, {
        'dev_pid': 1537,
    })
    print(result)


def get_file_content(filePath):
    with open(filePath, 'rb') as fp:
        return fp.read()

if __name__ == '__main__':
    main()


# ffmpeg -y  -i chunks/UC5xunxPS6oZ1zzKufgREFuA/sAot0WjBG8E_0000.mp4 -acodec pcm_s16le -f s16le -ac 1 -ar 16000 chunks/UC5xunxPS6oZ1zzKufgREFuA/sAot0WjBG8E_0000.pcm