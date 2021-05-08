from pydub import AudioSegment
from pydub.silence import split_on_silence
import sys
import os
import time
import re
import subprocess
from aip import AipSpeech
import pymysql
import threading

APP_ID = '24032991'
API_KEY = '5SYyz0GupHEGv8hxZc82aoXr'
SECRET_KEY = 'afFWr3bIlAb2wCNTLkPKHPL8CBadACK1'

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

def main():
    videoDir = "/tmp/youtube-video"
    # 生成输出文件夹
    if not os.path.exists('./chunks'):os.mkdir('./chunks')
    # 整理上次中断后的临时文件
    for path,d,filelist in os.walk(videoDir):
        for filename in filelist:
            if (re.match('^\(tmp\).*\.mp4',filename)):
                filePath = os.path.join(path,filename)
                os.rename(filePath,filePath.replace('(tmp)',''))
            # print(file)

    while 1:
        try:
            # 读视频文件夹
            pathDir = os.listdir(videoDir)
            while len(pathDir)>0:
                channel = pathDir.pop()
                if not os.path.isdir(videoDir + '/' + channel):continue
                print('进入' + channel + '频道查找：--------->>>>>>>')
                # 生成二级输出文件夹
                if not os.path.exists('./chunks/'+channel):os.mkdir('./chunks/'+channel)
                videos = os.listdir(videoDir + '/' + channel)
                while len(videos)>0:
                    video = videos.pop()
                    if '.mp4.tmp' in video:continue
                    if '.DS_Store' in video:continue
                    if '(tmp)' in video:continue
                    while True:
                        thread_count = len(threading.enumerate())
                        if(thread_count < 3):
                            print('当前进程数：%d 将添加一个进程……'%(thread_count))
                            # 载入
                            pre_name = '/tmp/youtube-video/' + channel + '/' + video
                            after_name = '/tmp/youtube-video/' + channel + '/(tmp)' + video
                            # 改名防重读
                            os.rename(pre_name, after_name)
                            
                            thread = threading.Thread(target=read_video, args=('(tmp)' + video,channel,))
                            thread.start()

                            break
                        time.sleep(5)

            print("--完成--")
        except Exception as e:
            print("出现异常")
            print(e)
        
        time.sleep(5)
    

def read_video(video, channel):
    print('处理视频' + video + '----->>>>')
    # 载入
    name = '/tmp/youtube-video/' + channel + '/' + video
    sound = AudioSegment.from_file(name, "mp4")
    #sound = sound[:3*60*1000] # 如果文件较大，先取前3分钟测试，根据测试结果，调整参数
    
    # 设置参数
    silence_thresh=-70      # 小于-70dBFS以下的为静默
    min_silence_len=700     # 静默超过700毫秒则拆分
    length_limit=60*1000    # 拆分后每段不得超过1分钟
    abandon_chunk_len=500   # 放弃小于500毫秒的段
    joint_silence_len=500  # 段拼接时加入1300毫秒间隔用于断句
    
    # 将录音文件拆分成适合百度语音识别的大小
    chuck_paths = prepare_for_baiduaip(name,sound,channel,silence_thresh,min_silence_len,length_limit,abandon_chunk_len,joint_silence_len)

    # 视频文本
    video_text = ''
    for chuck_path in chuck_paths:

        chuck_path_origin = chuck_path
        chuck_path_conv = chuck_path.split('.')[0] + '.pcm'

        subprocess.run('ffmpeg -y -i "%s" -acodec pcm_s16le -f s16le -ac 1 -ar 16000 "%s" '%(chuck_path_origin, chuck_path_conv), shell=True)
        
        client = AipSpeech(APP_ID, API_KEY, SECRET_KEY)
        # 识别本地文件
        result = client.asr(get_file_content(chuck_path_conv), 'pcm', 16000, {
            'dev_pid': 1537,
        })
        video_text += '   '.join(result['result'])
    
    print(video_text)

    #记录结果
    cursor.execute("UPDATE video SET content = '%s', video.read = 1 WHERE video_id = '%s';"%(video_text, video.replace('(tmp)','').split('.')[0]))
    connect.commit()
    #删除掉原视频
    os.remove(name)
    print("删除视频：" + name)
    #删除音频片段
    for chuck_path in chuck_paths:
        mp4_chuck = chuck_path.replace('./', '').split('.')[0] + '.mp4'
        pcm_chuck = chuck_path.replace('./', '').split('.')[0] + '.pcm'
        try:
            os.remove(mp4_chuck)
            os.remove(pcm_chuck)
        except Exception as e:
            pass

        print("删除" + mp4_chuck + ' 及其pcm临时文件')


def prepare_for_baiduaip(name,sound,channel,silence_thresh=-70,min_silence_len=700,length_limit=60*1000,abandon_chunk_len=500,joint_silence_len=1300):
    '''
    将录音文件拆分成适合百度语音识别的大小
    百度目前免费提供1分钟长度的语音识别。
    先按参数拆分录音，拆出来的每一段都小于1分钟。
    然后将，时间过短的相邻段合并，合并后依旧不长于1分钟。

    Args:
        name: 录音文件名
        sound: 录音文件数据
        silence_thresh: 默认-70      # 小于-70dBFS以下的为静默
        min_silence_len: 默认700     # 静默超过700毫秒则拆分
        length_limit: 默认60*1000    # 拆分后每段不得超过1分钟
        abandon_chunk_len: 默认500   # 放弃小于500毫秒的段
        joint_silence_len: 默认1300  # 段拼接时加入1300毫秒间隔用于断句
    Return:
        total：返回拆分个数
    '''

    # 按句子停顿，拆分成长度不大于1分钟录音片段
    print('开始拆分(如果录音较长，请耐心等待)\n',' *'*30)
    chunks = chunk_split_length_limit(sound,min_silence_len=min_silence_len,length_limit=length_limit,silence_thresh=silence_thresh)#silence time:700ms and silence_dBFS<-70dBFS
    print('拆分结束，返回段数:',len(chunks),'\n',' *'*30)

    # 放弃长度小于0.5秒的录音片段
    for i in list(range(len(chunks)))[::-1]:
        if len(chunks[i])<=abandon_chunk_len:
            chunks.pop(i)
    print('取有效分段：',len(chunks))

    # 时间过短的相邻段合并，单段不超过1分钟
    chunks = chunk_join_length_limit(chunks,joint_silence_len=joint_silence_len,length_limit=length_limit)
    print('合并后段数：',len(chunks))

    basename = os.path.basename(name)
    namef,namec = os.path.splitext(basename)
    namec = namec[1:]

    # 保存所有分段
    total = len(chunks)
    chuck_paths = []
    for i in range(total):
        new = chunks[i]
        save_name = '%s_%04d.%s'%(namef,i,namec)
        save_path = './chunks/'+channel+'/'+save_name
        chuck_paths.append(save_path)
        new.export(save_path, format=namec)
        # print('%04d'%i,len(new))
    print('------------------------')
    print(chuck_paths)
    return chuck_paths


def chunk_split_length_limit(chunk,min_silence_len=700,length_limit=60*1000,silence_thresh=-70):
    '''
    将声音文件按正常语句停顿拆分，并限定单句最长时间，返回结果为列表形式
    Args:
        chunk: 录音文件
        min_silence_len: 拆分语句时，静默满足该长度，则拆分，默认0.7秒。
        length_limit：拆分后单个文件长度不超过该值，默认1分钟。
        silence_thresh：小于-70dBFS以下的为静默
    Return:
        done_chunks：拆分后的列表
    '''
    todo_arr = []   #待处理
    done_chunks =[] #处理完
    todo_arr.append([chunk,min_silence_len,silence_thresh])

    while len(todo_arr)>0:
        # 载入一个音频
        temp_chunk,temp_msl,temp_st = todo_arr.pop(0)
        # 不超长的，算是拆分成功
        if len(temp_chunk)<length_limit:
            done_chunks.append(temp_chunk)
        else:
            # 超长的，准备处理
            if temp_msl<=100 and temp_st>=-10:
                # 提升到极致还是不行的，输出异常
                tempname = 'temp_%d.wav'%int(time.time())
                chunk.export(tempname, format='wav')
                print('万策尽。音长%d,静长%d分贝%d依旧超长,片段已保存至%s'%(len(temp_chunk),temp_msl,temp_st,tempname))
                raise Exception
            # 配置参数
            if temp_msl>100: # 优先缩小静默判断时常
                temp_msl-=100
            if temp_st<-10: # 提升认为是静默的分贝数
                temp_st+=10
            # 输出本次执行的拆分，所使用的参数
            localtime = time.asctime( time.localtime(time.time()) )
            msg = '开始拆分…… 音长,剩余,已添加[静长,分贝]:%d,%d,%d[%d,%d] 时间： %s'%(len(temp_chunk),len(todo_arr),len(done_chunks),temp_msl,temp_st,localtime)
            print(msg)
            # 拆分
            temp_chunks = split_on_silence(temp_chunk,min_silence_len=temp_msl,silence_thresh=temp_st)
            # 结束时间
            localtime = time.asctime( time.localtime(time.time()) )
            msg = '拆分完成…… 音长,剩余,已添加[静长,分贝]:%d,%d,%d[%d,%d] 时间： %s'%(len(temp_chunk),len(todo_arr),len(done_chunks),temp_msl,temp_st,localtime)
            print(msg)
            
            # 拆分结果处理
            doning_arr = [[c,temp_msl,temp_st] for c in temp_chunks]
            todo_arr = doning_arr+todo_arr

    return done_chunks


def chunk_join_length_limit(chunks,joint_silence_len=700,length_limit=60*1000):
    '''
    将声音文件合并，并限定单句最长时间，返回结果为列表形式
    Args:
        chunk: 录音文件
        joint_silence_len: 合并时文件间隔，默认1.3秒。
        length_limit：合并后单个文件长度不超过该值，默认1分钟。
    Return:
        adjust_chunks：合并后的列表
    '''
    # 
    silence = AudioSegment.silent(duration=joint_silence_len)
    adjust_chunks=[]
    temp = AudioSegment.empty()
    for chunk in chunks:
        length = len(temp)+len(silence)+len(chunk) # 预计合并后长度
        if length<length_limit: # 小于1分钟，可以合并
            temp+=silence+chunk
        else: # 大于1分钟，先将之前的保存，重新开始累加
            adjust_chunks.append(temp)
            temp=chunk
    else:
        adjust_chunks.append(temp)
    return adjust_chunks


def get_file_content(filePath):
    with open(filePath, 'rb') as fp:
        return fp.read()

if __name__ == '__main__':
    main()


