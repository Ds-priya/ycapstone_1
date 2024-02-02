from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd
import numpy as np
from datetime import datetime 
import streamlit as st

#getting API connect

def api_connect():
    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = "AIzaSyBMaRlnzs435YRH18t8EqHcaS8KM-90aaE"

    youtube = build(api_service_name, api_version, developerKey = DEVELOPER_KEY)
    return(youtube)

youtube=api_connect()

#getting channel information
def get_channel_info(channel_id):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id,
            maxResults=40
        )
    response = request.execute()
    
    for i in response['items']:
        #convertJSON into dictionary
        channel_data=dict(channel_name=i["snippet"]["title"],
                          channel_id=i["id"],
                          #subs_count=i['statistics']["subscriberCount"],
                          views=i["statistics"]["viewCount"],
                          video_count=i["statistics"]["videoCount"],
                          #description=i["snippet"]["description"],
                          playlist_id=i["contentDetails"]["relatedPlaylists"]["uploads"])
                          # status=i["status"])
        return channel_data

#print(response)



#Getting video ids  
#video_ids are going to be stored in #video_detail_l LIST
def get_videos_ids(channel_id):
    video_id_l=[]
    request = youtube.channels().list(id=channel_id,
            part="contentDetails")
    response_p = request.execute()
    #response_p["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    #uploads is playlist_id
    playlist_id=response_p["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    #pagetoken gives each page vidoes id.max video in one page is 50

    next_page_token=None
    #getting all page video ids
    while True:

        request = youtube.playlistItems().list(
                                                part="snippet",
                                                playlistId=playlist_id,
                                                maxResults=50,
                                                pageToken=next_page_token)
        response_v = request.execute()
        #geting 1 videoid
        #response_v["items"][0]["snippet"]["resourceId"]["videoId"]
        #appending 50 videoids (maxresults)

        for i in range(len(response_v["items"])):
            video_id_l.append(response_v["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token= response_v.get("nextPageToken")

        if next_page_token is None:
            break
    return video_id_l



#get  each video information for each viedo ids stored in video_id_l variable(LIST)

def get_video_info(video_ids):

    video_data_l=[]

    for v_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=v_id
        )
        response = request.execute()
        for i in response["items"]:
            data=dict(channel_name=i['snippet']['channelTitle'],
                            channel_id=i['snippet']['channelId'],
                            video_id=i['id'],
                            v_title=i['snippet']['title'],
                            v_description=i['snippet'].get('description'),
                            v_published_date=i['snippet']['publishedAt'],
                            v_thumbnail=i['snippet']['thumbnails']['default']['url'],
                            v_duration=i['contentDetails']['duration'],
                            v_view_count=i['statistics'].get('viewCount'),
                            v_like_count=i['statistics'].get('likeCount'),
                            v_dislike_count=i['statistics'].get('dislikecount'),
                            v_comment=i['statistics'].get('commentCount'),
                            v_favourit_count=i['statistics']['favoriteCount'],
                            v_caption_status =i['contentDetails']['caption']
                            )
            video_data_l.append(data) #each video details append(stored) in video_data list
    return video_data_l  
        #to check each video_id wise details
        #for i in video_data_l:
            #print(i)   



#getting comment details for each video

def get_comment_info(video_ids):
    comment_data_l=[]
    #try to skip the error,  when the comment becomes diabled
    try:
        for v_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=v_id,
                maxResults=50
            )
            response = request.execute()

            for i in response["items"]:
                data=dict(comment_id=i['id'],
                        video_id=i['snippet']['topLevelComment']['snippet']['videoId'],
                        comment_text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                        comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        comment_published_date=i['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                
                comment_data_l.append(data)
    except:
        pass
    return comment_data_l



#getting playlist info

def get_playlist_info(channel_id):
        nxtpgtoken=None
        playlist_detail_l=[]

        while True:
                request=youtube.playlists().list(
                                part='snippet,contentDetails',
                                channelId=channel_id,
                                maxResults=30,
                                pageToken=nxtpgtoken 
                )                      
                response=request.execute()

                for i in response['items']:
                        data=dict(playlist_id=i['id'],
                                channel_id=i['snippet']['channelId'],
                                playlist_name=i['snippet']['title'],
                                video_count=i['contentDetails']['itemCount'])
                        playlist_detail_l.append(data)
                
                nxtpgtoken=response.get('nextPageToken')
                if nxtpgtoken is None:
                        break
        return playlist_detail_l 



#connecting to mongodb atlas and uploading collection into  moangodb atlas
client=pymongo.MongoClient("mongodb+srv://priyarajkumar:Sairam18@cluster0.16upw9m.mongodb.net/?retryWrites=true&w=majority")
#database name
db=client['youtube_data']



def channel_details(channel_id):
    #order of function call should be in order
    
    ch_information=get_channel_info(channel_id)
    pl_information=get_playlist_info(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_information=get_video_info(vi_ids)
    comt_information=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({'channel_information':ch_information,
                      'playlist_information':pl_information,
                      'video_information':vi_information,
                      'comment_information':comt_information})
    return "upload into MongoDb Successfully"



#mysql connection and table creations for channels,playlists,videos,comments
def ch_table():
    myconnection = pymysql.connect(host='127.0.0.1',
                                user='root',
                                passwd='root',
                                database='youtube_data')
    cur = myconnection.cursor() 

    drop_query="drop table if exists channels_info"
    cur.execute(drop_query)
    myconnection.commit() 

    #mysql connection and table creations for channels,playlists,videos,comments                                              

    try:
        create_query="create table if not exists channels_info(channel_name varchar(100),channel_id varchar(100) primary key,views bigint,video_count int, playlist_id varchar(100))"                                             
        cur.execute(create_query)
        myconnection.commit() 
    except:
        print("cnannels_info table already created") 



    # mongoDB database
        
    ch_list=[]   

    db=client['youtube_data']
    coll1=db["channel_details"]

    #extract ch_info from mongoDb and append into a list called ch_list as dict

    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])

    #convert ch_list into Dataframe
        
    df=pd.DataFrame(ch_list)

    #insert data frame values into mysql table channels_info

    insert_query="insert into channels_info(channel_name,channel_id,views,video_count,playlist_id)values(%s,%s,%s,%s,%s)"

    for row in df.itertuples(index=False):
        try:
            cur.execute(insert_query, row)
            myconnection.commit()
        except:
            print('Channels informations are already inserted')


            
def playlist_table():
        myconnection = pymysql.connect(host='127.0.0.1',
                                        user='root',
                                        passwd='root',
                                        database='youtube_data')
        cur = myconnection.cursor() 

        drop_query="drop table if exists playlists_info"
        cur.execute(drop_query)
        myconnection.commit() 

        #mysql connection and table creations for channels,playlists,videos,comments                                              


        create_query='''create table if not exists playlists_info
                                                (
                                                playlist_id varchar(100) primary key,
                                                channel_id varchar(100),
                                                playlist_name varchar(100),
                                                video_count int
                                                )'''                                          
        cur.execute(create_query)
        myconnection.commit() 

        # mongoDB database

        pl_list=[]   

        db=client['youtube_data']
        coll1=db["channel_details"]

        #extract pl_info from mongoDb and append into a list called pl_list as dict

        for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
                for i in range(len(pl_data['playlist_information'])):
                        pl_list.append(pl_data['playlist_information'][i])

        #convert pl_list into Dataframe

        df1=pd.DataFrame(pl_list)

        myconnection = pymysql.connect(host='127.0.0.1',
                                        user='root',
                                        passwd='root',
                                        database='youtube_data')
        cur = myconnection.cursor() 

        #insert data frame values into mysql table channels_info

        insert_query='''insert into playlists_info
                                                (
                                                playlist_id,
                                                channel_id,
                                                playlist_name,
                                                video_count
                                                )
                                                values
                                                (
                                                %s,%s,%s,%s)'''
        for row in df1.itertuples(index=False):
                
                cur.execute(insert_query, row)
                myconnection.commit()




def video_table():
        myconnection = pymysql.connect(host='127.0.0.1',
                                        user='root',
                                        passwd='root',
                                        database='youtube_data')
        cur = myconnection.cursor() 

        drop_query="drop table if exists videos_info"
        cur.execute(drop_query)
        myconnection.commit() 

        #mysql connection and table creations for channels,playlists,videos,comments                                              


        create_query='''create table if not exists videos_info
                                                (
                                                channel_name varchar(100),
                                                channel_id varchar(100),
                                                video_id varchar(20) primary key,
                                                v_title varchar(150),
                                                v_description text,
                                                v_published_date timestamp,
                                                v_thumbnail varchar(100),
                                                v_duration time,
                                                v_view_count bigint,
                                                v_like_count bigint,
                                                v_dislike_count bigint,
                                                v_comment int,
                                                v_favourit_count int,
                                                v_caption_status varchar(10)
                                                )'''                                          
        cur.execute(create_query)
        myconnection.commit()

        # mongoDB database

        vi_list=[]   

        db=client['youtube_data']
        coll1=db["channel_details"]

        #extract pl_info from mongoDb and append into a list called vl_list as dict

        for vi_data in coll1.find({},{'_id':0,'video_information':1}):
                for i in range(len(vi_data['video_information'])):
                        vi_list.append(vi_data['video_information'][i])

        #convert vi_list into Dataframe

        df3=pd.DataFrame(vi_list)

        #df3['v_duration']=df3['v_duration'].str.replace("PT", " ").replace("H", ":").replace("M", ":").replace("S", "")


        myconnection = pymysql.connect(host='127.0.0.1',
                                                user='root',
                                                passwd='root',
                                                database='youtube_data')
        cur = myconnection.cursor() 

        #insert data frame values into mysql table videos_info


        for index,row in df3.iterrows():
                insert_query='''insert  ignore into videos_info
                                                (
                                                channel_name,
                                                channel_id, 
                                                video_id,
                                                v_title,
                                                v_description,
                                                v_published_date,
                                                v_thumbnail,
                                                v_duration,
                                                v_view_count,
                                                v_like_count,
                                                v_dislike_count,
                                                v_comment,
                                                v_favourit_count,
                                                v_caption_status 
                                                        )              
                                                values
                                                (
                
                                                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                duration = row.v_duration.replace("PT", " ").replace("H", ":").replace("M", ":").replace("S", "")
                if not duration.endswith(":00"):  # Add seconds if missing
                        duration += ":00"
                
                values=(row['channel_name'],
                        row['channel_id'], 
                        row['video_id'],
                        row['v_title'],
                        row['v_description'],
                        row['v_published_date'],
                        row['v_thumbnail'],
                        duration,
                        row['v_view_count'],
                        row['v_like_count'] ,
                        row['v_dislike_count'],
                        row['v_comment'],
                        row['v_favourit_count'],
                        row['v_caption_status'])

                cur.execute(insert_query, values)
                myconnection.commit()




def comment_table():
        myconnection = pymysql.connect(host='127.0.0.1',
                                                user='root',
                                                passwd='root',
                                                database='youtube_data')
        cur = myconnection.cursor() 

        drop_query="drop table if exists comments_info"
        cur.execute(drop_query)
        myconnection.commit() 

        #mysql connection and table creations for channels,playlists,videos,comments                                              


        create_query='''create table if not exists comments_info
                                                (
                                                comment_id varchar(100) primary key,
                                                video_id varchar(100),
                                                comment_text text,
                                                comment_author varchar(150),
                                                comment_published_date timestamp
                                                )'''                                          
        cur.execute(create_query)
        myconnection.commit()

        # mongoDB database

        com_list=[]   

        db=client['youtube_data']
        coll1=db["channel_details"]

        #extract com_info from mongoDb and append into a list called com_list as dict

        for com_data in coll1.find({},{'_id':0,'comment_information':1}):
                for i in range(len(com_data['comment_information'])):
                        com_list.append(com_data['comment_information'][i])

        #convert com_list into Dataframe
        df4=pd.DataFrame(com_list) 

        #df4['comment_published_date']=df4['comment_published_date'].replace("T"," ").replace("Z"," ")

        myconnection = pymysql.connect(host='127.0.0.1',
                                                        user='root',
                                                        passwd='root',
                                                        database='youtube_data')
        cur = myconnection.cursor() 

        #insert data frame values into mysql table comments_info
        for index,row in df4.iterrows():
                insert_query='''insert  ignore into comments_info
                                                (
                                                comment_id,
                                                video_id,
                                                comment_text,
                                                comment_author,
                                                comment_published_date   
                                                        )              
                                                values
                                                (
                                                        %s,%s,%s,%s,%s)'''
                
                values=(row['comment_id'],
                        row['video_id'], 
                        row['comment_text'],
                        row['comment_author'],
                        row['comment_published_date'])        
                cur.execute(insert_query,values )
                myconnection.commit()

def tables():
    ch_table()
    playlist_table()
    video_table()
    comment_table()
    return "table are created successfully"

def show_channel_table():
    ch_list=[]   

    db=client['youtube_data']
    coll1=db["channel_details"]

    #extract ch_info from mongoDb and append into a list called ch_list as dict

    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])

    #convert ch_list into Dataframe
        
    df=st.dataframe(ch_list)

    return df


def show_playlist_table():
        pl_list=[]   

        db=client['youtube_data']
        coll1=db["channel_details"]

        #extract pl_info from mongoDb and append into a list called pl_list as dict

        for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
                for i in range(len(pl_data['playlist_information'])):
                        pl_list.append(pl_data['playlist_information'][i])

        #convert pl_list into Dataframe

        df1=st.dataframe(pl_list)

        return df1


def show_video_table():
        vi_list=[]   

        db=client['youtube_data']
        coll1=db["channel_details"]

        #extract pl_info from mongoDb and append into a list called pl_list as dict

        for vi_data in coll1.find({},{'_id':0,'video_information':1}):
                for i in range(len(vi_data['video_information'])):
                        vi_list.append(vi_data['video_information'][i])

        #convert vi_list into Dataframe

        df3=st.dataframe(vi_list)

        return df3


def show_comment_table():
        com_list=[]   

        db=client['youtube_data']
        coll1=db["channel_details"]

        #extract com_info from mongoDb and append into a list called com_list as dict

        for com_data in coll1.find({},{'_id':0,'comment_information':1}):
                for i in range(len(com_data['comment_information'])):
                        com_list.append(com_data['comment_information'][i])

        #convert vi_list into Dataframe
        df4=st.dataframe(com_list) 
        
        return df4



#streamlit part code

st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]") 
   
channel_id=st.text_input("Enter the channel Id ")

# Store data when a button is clicked into mongoDB
if st.button("collect and store data"):

    ch_list=[]   

    db=client['youtube_data']
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information']['channel_id'])
    if channel_id in ch_list:

        st.success("this channel id details are already inserted")
    else:
        upload=channel_details(channel_id)
        st.success(upload)

if st.button("Migrate to mysql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("select the table for view",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channel_table()

elif show_table=="PLAYLISTS":
    show_playlist_table()    

elif show_table=="VIDEOS":
    show_video_table()

elif show_table=="COMMENTS":
    show_comment_table()
         

#MYSQL connection

myconnection = pymysql.connect(host='127.0.0.1',
                                                user='root',
                                                passwd='root',
                                                database='youtube_data')
cur = myconnection.cursor() 

questions=st.selectbox("select the query",("1. What are the names of all the videos and their corresponding channels?",
                     "2.Which channels have the most number of videos, and how many videos do they have?",
                     "3.What are the top 10 most viewed videos and their respective channels?",
                     "4.How many comments were made on each video, and what are their corresponding video names?",
                     "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                     "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                     "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                     "8.What are the names of all the channels that have published videos in the year 2022?",
                     "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                     "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

                    

if questions=="1. What are the names of all the videos and their corresponding channels?":
    query1='''select v_title as video_titile,channel_name as channelname from videos_info '''
    cur.execute(query1)
    myconnection.commit()
    t1=cur.fetchall()  #it fetch all the datas from the above qury and store it ina variable t1
    df1=pd.DataFrame(t1,columns=['video title','channel name'])
    st.write(df1)

elif questions=="2.Which channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name,video_count from channels_info
                order by 2 desc '''
    cur.execute(query2)
    myconnection.commit()
    t2=cur.fetchall()  #it fetch all the datas from the above qury and store it ina variable t1
    df2=pd.DataFrame(t2,columns=['channel name','video count'])
    st.write(df2)
    
    st.bar_chart(df2,x="channel name",y='video count')

elif questions=="3.What are the top 10 most viewed videos and their respective channels?":
    query3='''select channel_name,v_title,v_view_count from videos_info
              where v_view_count is not null
              order by v_view_count desc limit 10; '''
    cur.execute(query3)
    myconnection.commit()
    t3=cur.fetchall()  #it fetch all the datas from the above qury and store it ina variable t1
    df3=pd.DataFrame(t3,columns=['channel name','video tittle','video count'])
    st.write(df3)

elif questions=="4.How many comments were made on each video, and what are their corresponding video names?":
    query4='''select v.v_title,c.video_id,count(c.comment_text) as comment_count 
                from comments_info as c, videos_info as v 
                where v.video_id= c.video_id
                and c.comment_text is not null
                group by video_id; '''
    cur.execute(query4)
    myconnection.commit()
    t4=cur.fetchall()  #it fetch all the datas from the above qury and store it ina variable t1
    df4=pd.DataFrame(t4,columns=['video tittle','video id','comment count'])
    st.write(df4)

elif questions=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select channel_name,v_title,v_like_count 
                from videos_info
                where v_like_count is not null
                order by 3 desc'''
    cur.execute(query5)
    myconnection.commit()
    t5=cur.fetchall()  #it fetch all the datas from the above qury and store it ina variable t1
    df5=pd.DataFrame(t5,columns=['channel name','video title','likes count'])
    st.write(df5)

elif questions=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6=''' select v_title,round(v_like_count),v_dislike_count from videos_info'''
    cur.execute(query6)
    myconnection.commit()
    t6=cur.fetchall()  #it fetch all the datas from the above qury and store it ina variable t1
    df6=pd.DataFrame(t6,columns=['video title','likes count','dislike count'])
    st.write(df6)

elif questions=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7='''select channel_name,views from channels_info 
              where views<>0
                order by 2 desc '''
    cur.execute(query7)
    myconnection.commit()
    t7=cur.fetchall()  #it fetch all the datas from the above qury and store it ina variable t1
    df7=pd.DataFrame(t7,columns=['channel_name','no of views'])
    st.write(df7)

    st.bar_chart(df7,x="channel_name",y="no of views")

elif questions=="8.What are the names of all the channels that have published videos in the year 2022?":
    query8='''select channel_name,v_title,v_published_date from videos_info
                where year(v_published_date)=2022'''
    cur.execute(query8)
    myconnection.commit()
    t8=cur.fetchall()  #it fetch all the datas from the above qury and store it ina variable t1
    df8=pd.DataFrame(t8,columns=['channel_name','video title','video release date'])
    st.write(df8)

elif questions=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''SELECT channel_name,SEC_TO_TIME(AVG(TIME_TO_SEC(v_duration))) AS avg_duration
            FROM videos_info
            group by channel_name
            HAVING avg_duration > '00:00:00';'''
    cur.execute(query9)
    myconnection.commit()
    t9=cur.fetchall()  #it fetch all the datas from the above qury and store it ina variable t1
    df9=pd.DataFrame(t9,columns=['channel_name','avg duration of video'])
  
    # to display time format duration avg in streamlit
    #convert time format into string then append to a list as dict format
    df9['avg duration of video']=df9['avg duration of video'].astype(str)
    T9=[]
    for index,row in df9.iterrows():
    
        T9.append(dict(channeltitle=row['channel_name'],avg_duration=row['avg duration of video']))
    df11=pd.DataFrame(T9)

    st.write(df11)

    

elif questions=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''select channel_name,v_title,v_comment from videos_info 
                                order by 3  desc'''
    cur.execute(query10)
    myconnection.commit()
    t10=cur.fetchall()  #it fetch all the datas from the above qury and store it ina variable t1
    df10=pd.DataFrame(t10,columns=['channel_name','video title','no of comment'])
    st.write(df10)    

