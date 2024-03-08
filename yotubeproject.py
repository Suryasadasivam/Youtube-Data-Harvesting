import streamlit as st

from pprint import pprint
import googleapiclient.discovery
import pymongo
import pandas as pd 
import datetime
import mysql.connector 
from sqlalchemy import create_engine
import plotly .express as px

api_service_name = "youtube"
api_version = "v3"
api_key="AIzaSyCtOmmPYSHLBnXw2kJNxeGMPgIzJ9dLepo"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key )
client=pymongo.MongoClient('your mongodb connection')
vb=client['youtube']
col=vb["channel_details"]
col2=vb["video_details"]
col3=vb["comment_details"]

#database connection
mydb= mysql.connector.connect(
    host='hostid', 
    user="root", 
    password="your password",
    database='YoutubeProject')
mycursor= mydb.cursor() 
# getting channel name from Mongodb 
def channel_name():
    Get_channel_Name=[]
    for i in col.find():
        Get_channel_Name.append(i.get('Channel_Name'))
    return Get_channel_Name 
ch_names=channel_name()

#getting channel details from youtube
def get_channel_info (youtube,channel_id):
    try:
        request = youtube.channels().list(
          part="snippet,contentDetails, statistics",
          id=channel_id
        )
        response=request.execute()   
        if 'items' not in response:
            return("No Channel Found")
        else:
            item=response["items"][0]
            channel_information= dict(Channel_id=channel_id,
                              Channel_Name=item["snippet"]["title"],
                              Channel_Views=int(item["statistics"]["viewCount"]),
                              Channel_Subscribers=int(item["statistics"]["subscriberCount"]),
                              Channel_Videos=int(item["statistics"]["videoCount"]),
                              Channel_description=item["snippet"]["description"],
                              Channel_Playlist_id=item["contentDetails"]["relatedPlaylists"]["uploads"])
      
        return channel_information
    except Execption as e: 
        return e
#getting videoIds 
def get_channel_videos(youtube,playlist_id):
    try:
       video_ids=[]
       next_page_token=None
       while True:
            request = youtube.playlistItems().list(
            part="snippet",
            playlistId= playlist_id,
            maxResults=50)#next page token 
            response = request.execute()
            for i in range(len(response['items'])):
                videoId=response["items"][i]['snippet']['resourceId']['videoId']
                video_ids.append(videoId)
        
            next_page_token=response.get("nextPageToken")
            if not next_page_token:
                break
       return (video_ids) 
    except Exception as e:
        return e 
#getting video details
def get_video_info(youtube,videoid):
    try:
        video_info=[]
        request=youtube.videos().list(
        part="snippet,contentDetails,statistics",
         id=','.join(videoid)
        )
        response=request.execute()
        for i in range(len(response['items'])):
            duration=response["items"][i]['contentDetails']["duration"]
            dur=pd.Timedelta(duration)
            video_information= dict(video_id=response["items"][i]["id"],
                                video_channel_name=response["items"][i]["snippet"]['channelTitle'],
                                video_name=response["items"][i]["snippet"]['title'],
                                video_description=response["items"][i]["snippet"]['description'], 
                                published_date= response['items'][i]['snippet']['publishedAt'][0:10],
                                published_time=response['items'][i]['snippet']['publishedAt'][11:19],
                                view_count=int(response["items"][i]['statistics']['viewCount']),
                                like_count=int(response["items"][i]['statistics'].get('likeCount',0)),#get function in some case 
                                favorite_count=int(response["items"][i]['statistics']['favoriteCount']),
                                comment_count=int(response["items"][i]['statistics'].get('commentCount',0)),
                                duration=int(dur.total_seconds()),#duration convert
                                thumnbnail= response['items'][i]['snippet']['thumbnails']["default"]['url'],
                                caption_status=response['items'][i]['contentDetails']['caption']
                               )
            video_info.append( video_information )
        return video_info
    except  Exception as e:
        return e
#getting comments details
def get_comment(youtube, videoid):
    try:
        all_comments=[]
        next_page_token=None
        while True:
            request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=videoid,
            maxResults=100
            )
            response=request.execute()
            for i in range(0,len(response['items'])):
                data=dict(Comment_id= response["items"][i]['snippet']['topLevelComment']['id'],
                      Comment_text=response["items"][i]["snippet"]["topLevelComment"]['snippet']['textOriginal'],
                      videoId=response["items"][i]['snippet']['videoId'],
                      author_name=response["items"][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                      published_at=response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'],
                      channel_id=response["items"][i]['snippet']['channelId'])
                all_comments.append(data)
            next_page_token=response.get("nextPageToken")
            if not next_page_token:
                break
          
        return all_comments
    except Exception as e:
        return e 
#inserting channel information in Mongodb      
def insert_channel_info(channel_id):
    c=[]
    for i in col.find():
        c.append(i.get('Channel_id'))
    if channel_id not in c:
        channel_data=get_channel_info(youtube,channel_id)
        col.insert_one(channel_data)
        playlist_ids=channel_data.get ('Channel_Playlist_id')
        video_ids_data=get_channel_videos(youtube,playlist_ids)
        videodata=get_video_info(youtube,video_ids_data)
        col2.insert_many(videodata)
        cd=[]
        for i in video_ids_data:
            cd.extend(get_comment(youtube,i))
        for j in range(len(cd)):
            col3.insert_one(cd[j])
        return('Data has been loaded successfully in Mongodb,click Sql Migration tab')
    else:
               return('Channel ID already exist in Mongodb,click Sql Migration tab')
channel_display=[]
#channel retriving from Mongodb 
def retriving_channel_name(Channelname):
    display_channel_info=col.find_one({'Channel_Name':Channelname},{'_id':0})
    channel_display.append(display_channel_info)
    return channel_display
video_display=[]
def retriving_video_detail(Channelname):
    for i in col2.find({'video_channel_name':Channelname},{'_id':0}):
        video_display.append(i)  
comment_display=[]
def retriving_comments_details(Channelid):
    for j in col3.find({'channel_id':Channelid},{'_id':0}):
        comment_display.append(j)  
# Channel Migration to sql  
def inserting_info(chan_name):
    engine= create_engine("mysql+mysqlconnector://{user}:{pw}@localhost/{db}".format(user="root",pw="Surya0807sada",db="youtubeproject"));
    mycursor.execute("SELECT Channel_Name from channel_details;")
    res11=mycursor.fetchall()
    for i in range(len(res11)):
        if res11[i][0]==chan_name:
           return ('channel already exists in SQL Table,click Query tab')
        else:
            pd.DataFrame(channel_display).to_sql('channel_details', con=engine, if_exists="append", chunksize=1000, index=False)
            pd.DataFrame(video_display).to_sql('video_details', con=engine, if_exists="append", chunksize=1000, index=False)
            pd.DataFrame(comment_display).to_sql('comments_details', con=engine, if_exists="append", chunksize=1000, index=False)
            return("Data has been uploaded in SQL successfully, click query tab")
# Analysis query
def sql_query(question):
    if question=='1.What are the names of all the videos and their corresponding channels?':
        mycursor.execute("SELECT video_name,video_channel_name from video_details;")
        
        res1=mycursor.fetchall()
        result1=pd.DataFrame(res1,columns=['videoname','channelname'])
        return st.write(result1)
           
    elif question=='2.Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("SELECT Channel_Name,Channel_Videos as Video_Count from channel_details order by Channel_Videos desc;")
        
        res2=mycursor.fetchall()
        result2=pd.DataFrame(res2,columns=['Channel_Name','Channel_Videos'])
        fig=px.bar(result2, x="Channel_Name",y="Channel_Videos",title="Tota Video Counts by channels")
        fig.update_traces(width=0.2)
        fig.update_xaxes(title_font=dict(size=20, color="Blue"), tickfont=dict(color="Blue",size=18))
        fig.update_yaxes(title_font=dict(size=20, color="Blue"), tickfont=dict(color="Blue",size=18),showgrid=False)
        return st.plotly_chart(fig)
    
           
    elif question=='3.What are the top 10 most viewed videos and their respective channels?':
         mycursor.execute("SELECT video_channel_name,video_name,view_count from video_details where view_count is not null order by view_count desc limit 10;")
         
         res3=mycursor.fetchall()
         result3=pd.DataFrame(res3,columns=['video_channel_name','video_name','view_count'])
         return st.write(result3)
         
    elif question=='4.How many comments were made on each video,and what are their corresponding video names?':
         mycursor.execute("SELECT video_channel_name,video_name,comment_count from video_details where comment_count is not null;")
         
         res4=mycursor.fetchall()
         result4=pd.DataFrame(res4,columns=['video_channel_name','video_name','comment_count'])
         return st.write(result4)
    elif question=='5.Which videos have the highest number of likes, and what are their corresponding channel names?':
         mycursor.execute("SELECT video_channel_name,video_name, like_count from video_details where like_count is not null order by like_count desc;")
         
         res5=mycursor.fetchall()
         result5=pd.DataFrame(res5,columns=['video_channel_name','video_name','like_count'])
         return st.write(result5)
    elif question=='6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
         mycursor.execute("SELECT video_name, like_count from video_details where like_count is not null;")
        
         res6=mycursor.fetchall()
         result6=pd.DataFrame(res6,columns=['video_name','like_count'])
         return st.write(result6)
    elif question=='7.What is the total number of views for each channel, and what are their corresponding channel names?':
         mycursor.execute("SELECT Channel_Name, Channel_Views as Total_views from channel_details;")
         
         res7=mycursor.fetchall()
         result7=pd.DataFrame(res7,columns=['Channel_Name','Total_views'])
         fig1=px.bar(result7, x="Channel_Name",y="Total_views",title="Tota Views by channel")
         fig1.update_traces(width=0.2)
         fig1.update_xaxes(title_font=dict(size=20, color="Blue"), tickangle=-45,tickfont=dict(color="Blue",size=18))
         fig1.update_yaxes(title_font=dict(size=20, color="Blue"), tickfont=dict(color="Blue",size=18),showgrid=False)
         return st.plotly_chart(fig1)
         
    elif question=='8.What are the names of all the channels that have published videos in the year 2022?':
         mycursor.execute("SELECT video_channel_name from video_details where extract(year from published_date)=2022;")
         
         res8=mycursor.fetchall()
         result8=pd.DataFrame(res8,columns=['video_channel_name'])
         return st.write(result8)
    elif question=='9.What is the average duration of all videos in each channel and what are their corresponding channel names?':
         mycursor.execute("SELECT video_channel_name,avg(duration) as avg_duration_seconds from video_details group by video_channel_name order by avg_duration_seconds desc;")
         
         res9=mycursor.fetchall()
         result9=pd.DataFrame(res9,columns=['video_channel_name','avg_duration_seconds'])
         fig2=px.bar(result9, x="video_channel_name",y="avg_duration_seconds",title="Avg duration by channel")
         fig2.update_traces(width=0.2)
         fig2.update_xaxes(title_font=dict(size=20, color="Blue"), tickangle=-45,tickfont=dict(color="Blue",size=18))
         fig2.update_yaxes(title_font=dict(size=20, color="Blue"), tickfont=dict(color="Blue",size=18),showgrid=False)
         return st.plotly_chart(fig2)
    elif question=='10.Which videos have the highest number of comments, and what are their corresponding channel names?':
         mycursor.execute("SELECT video_channel_name,video_name,comment_count  from video_details where comment_count is not null order by comment_count desc;")
         
         res10=mycursor.fetchall()
         result10=pd.DataFrame(res10,columns=['video_channel_name','video_name','comment_count'])
         return st.write(result10)
     
qq=['1.What are the names of all the videos and their corresponding channels?',
    '2.Which channels have the most number of videos, and how many videos do they have?',
    '3.What are the top 10 most viewed videos and their respective channels?',
    '4.How many comments were made on each video,and what are their corresponding video names?',
    '5.Which videos have the highest number of likes, and what are their corresponding channel names?',     
    '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?', 
    '7.What is the total number of views for each channel, and what are their corresponding channel names?',
    '8.What are the names of all the channels that have published videos in the year 2022?',
    '9.What is the average duration of all videos in each channel and what are their corresponding channel names?',
    '10.Which videos have the highest number of comments, and what are their corresponding channel names?' ]
#streamlit application design
with st.sidebar: 
    if st.button("Tutorial"):
        st.markdown(''' 
                    - Go to youtube and copy channel id. 
                    - Enter the channel id and click extract button.
                    - Data will be upload in mongodb. 
                    - Select channel name from available list.
                    - Click migrate button.
                    - Select the question and click answer button for analysis.''')       
st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
tab1, tab2, tab3, tab4 = st.tabs(["About", "Data Harvest","SQL Migration", "QUERY"])
with tab1:
   st.header("About project")
   st.write("The goal of this project is to collect data from YouTube using the YouTube Data API, store it in both SQL and NoSQL databases (SQL and MongoDB, respectively), and then create a user-friendly interface using Streamlit for querying and visualizing the data.") 
   if st.button('Workflow'):
       st.markdown('''
                     - Utilize the YouTube Data API and Python libraries to fetch various data points from YouTube such as video metadata, comments, likes, dislikes, view counts, etc.
                     - NoSQL document database(MongoDB) is used for storing unstructured data retrived from Youtube.
                     - Fetching the data from MongoDB and insertion into MY SQL, Pandas library is used for data transformation before inserting into databases. 
                     - Streamlit is used for building interactive web applications and display data.''' )
       st.write("Now lets dive into Youtube data analysis,click data Harvest") 
    
with tab2:
         st.header("Data Extraction and Uploading in MongoDB")
         Channel=st.text_input("Please Enter Channel Id")
         if st.button(" Extract and Upload in MongoDB"):
               res= insert_channel_info (Channel)
               st.write(res)
               ch_names=channel_name()
               
with tab3:
    opt=st.selectbox("select channel Name", (ch_names),index= None,placeholder="select channel Name")
    if st.button("Migrate to SQL"):
       if not opt:
           st.error("Please select a channel name") 
       else:
          Ch=retriving_channel_name(opt)
          Ch_id=Ch[0].get('Channel_id')
          vd=retriving_video_detail(opt)
          cm=retriving_comments_details(Ch_id)  
          Sq= inserting_info(opt)
          st.write(Sq)   
with tab4:
    st.header("Hello, lets do some data analysis")
    opt1= st.selectbox("Query Question", (qq),index= None,placeholder="Select Question")
    if st.button("Click for Answer"):
        if not opt1:
            st.error('please select the query question')
        else:
           End_result= sql_query(opt1)
           
            
    
            
     

    
    
    
    
    
       
       
