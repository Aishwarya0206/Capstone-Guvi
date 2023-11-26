import streamlit as st
import pandas as pd
import googleapiclient.discovery
import json
import re
from datetime import datetime
from data_ingestion import data_ingestion
from data_mapping_with_transfer import data_mapping_with_transfer
from youtube_harvest import youtube_harvest


if __name__ == '__main__':

    def callback():
        st.session_state.button_clicked1 = True

    def callback1():
        st.session_state.button_clicked2 = True

    def choosen_value(option, connect):
        if(option == '1. What are the names of all the videos and their corresponding channels?'):
                #st.write(option)
            result = sql_conn.getAllVideosAndChannels(connect['cursor'], connect['conn'])
            df0 = pd.DataFrame(result, columns=['Video name', 'Channel name'])
            st.dataframe(df0)
        elif(option == '2. Which channels have the most number of videos, and how many videos do they have?'):
            result = sql_conn.channelNameMostVideo(connect['cursor'], connect['conn'])
            df1 = pd.DataFrame(result, columns=['Channel name', 'Video count'])
            st.dataframe(df1)
        elif(option == '3. What are the top 10 most viewed videos and their respective channels?'):
            result = sql_conn.topTenViewedVideosWithChannels(connect['cursor'],connect['conn'])
            df2 = pd.DataFrame(result, columns=['Channel name', 'Video name'])
            st.dataframe(df2)
        elif(option == '4. How many comments were made on each video, and what are their corresponding video names?'):
            result = sql_conn.commentCountForEachVideos(connect['cursor'],connect['conn'])
            df3 = pd.DataFrame(result, columns=['Video name', 'Comment count'])
            st.dataframe(df3)
        elif(option == '5. Which videos have the highest number of likes, and what are their corresponding channel names?'):
            result = sql_conn.highestLikesChannels(connect['cursor'],connect['conn'])
            df4 = pd.DataFrame(result, columns=['Channel name', 'Video name', 'Like count'])
            st.dataframe(df4)
        elif(option == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?'):
            result = sql_conn.totalLikesOrDislikesOfEachVideo(connect['cursor'],connect['conn'])
            df5 = pd.DataFrame(result, columns=['Video name', 'Total number of likes', 'Total number of dislikes'])
            st.dataframe(df5)
        elif(option == '7. What is the total number of views for each channel, and what are their corresponding channel names?'):
            result = sql_conn.totalViewsAndChannelName(connect['cursor'],connect['conn'])
            df6 = pd.DataFrame(result, columns=['Channel name', 'Total number of views'])
            st.dataframe(df6)
        elif(option == '8. What are the names of all the channels that have published videos in the year 2022?'):
            result = sql_conn.publishedVideosChannels(connect['cursor'],connect['conn'])
            df7 = pd.DataFrame(result, columns=['Channel name'])
            st.dataframe(df7)
        elif(option == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?'):
            result = sql_conn.avgDurationOfAllVideos(connect['cursor'],connect['conn'])
            df8 = pd.DataFrame(result, columns=['Channel name', 'Average Duration'])
            st.dataframe(df8)
        else:
            result = sql_conn.highestCommentAndChannels(connect['cursor'],connect['conn'])
            df9 = pd.DataFrame(result, columns=['Channel name', 'Comment count'])
            st.dataframe(df9)

    try:
        if "button_clicked1" not in st.session_state:
            st.session_state.button_clicked1 = False

        if "button_clicked2" not in st.session_state:
            st.session_state.button_clicked2 = False

        st.title('Welcome to Youtube Harvesting!')
        channel = {}
        collection_val = None
        response = None
        response_back = None
        check_val = None
        transfer_clicked = False
        #session_state = {"button_clicked": True}
        my_form = st.form(key='form1')
        
        channel_id = my_form.text_input('Channel ID', '', placeholder = 'Enter a channel id')
        max_playlist_per_page = my_form.slider('No. of playlist per page', 0, 50, value=5)
        max_videos_per_page = my_form.slider('No. of videos per page', 1, 50, value=5)
        max_comments_per_page = my_form.slider('No. of comments per page', 1, 100, value=20)
        isPageToken_playlist = my_form.checkbox('Do you wanna extract all the playlists?', False)
        isPageToken_videos = my_form.checkbox('Do you wanna extract all the videos?', False)
        transfer_data = my_form.toggle('Do you wanna transfer the data?')

        youtube = googleapiclient.discovery.build("youtube", "v3", developerKey="AIzaSyDNkiWXgrXKGfZLUP3xnaattLDWAJBdAhk")
        youtube_har = youtube_harvest(youtube, max_playlist_per_page, max_videos_per_page, max_comments_per_page, isPageToken_playlist, isPageToken_videos, response_back)

        local_host = "mongodb://localhost:27017/"
        db_name = "youtube_harvest"
        collection_val = "youtube_details"
        mongodb = data_ingestion(local_host, db_name, collection_val)

        sql = {"host": "localhost", "user": "root", "password": "Password123#@!","database":"YoutubeHarvesting"}
        sql_conn = data_mapping_with_transfer(sql)
        connect = sql_conn.connect_db()

        
        if(my_form.form_submit_button("Search", on_click=callback) or st.session_state.button_clicked1):
            #search_clicked = True
            #st.session_state['search_clicked'] = search_clicked
            my_form.write("Search button clicked!")
            if channel_id != '' or channel_id is not None:
                response = youtube_har.channel_api_call(channel_id, channel)
                if response:
                    my_form.json(response, expanded=False)
                    if(my_form.form_submit_button(f'Ingest and transfer data' if(transfer_data) else f'Ingest data', on_click=callback1)):
                        my_form.write(f"Ingest and transfer button clicked!" if(transfer_data) else f'Ingest button clicked!')
                        if "ingestion_done" not in st.session_state:
                            check_val = mongodb.db_creation_with_data_ingestion(response)
                            print(check_val)
                            if check_val["status"] == 'Success':
                                my_form.success("Data ingestion successful!")
                                st.session_state["ingestion_done"] = True
                                #if(my_form.form_submit_button("Transfer") or st.session_state.button_clicked2):
                                #my_form.write("Transfer button clicked!")
                                if(transfer_data):
                                    create_table_ddl = sql_conn.execute_ddl(connect['cursor'], connect['conn'])
                                    print(create_table_ddl)
                                    if(create_table_ddl):
                                        retrieve_values = mongodb.retrieve_data_after_ingestion(check_val["Collection"])
                                        for val in retrieve_values:
                                            #connect = sql_conn.connect_db()
                                            map = sql_conn.insert_data(connect['cursor'], connect['conn'], val)
                                            my_form.success(map)
                                    else:
                                        my_form.warning("Tables not created")
                            else:
                                my_form.warning("Data ingestion failed!")
                            
                        
                else:
                    my_form.warning("No response from API")
            else:
                my_form.warning("Please enter a channel ID.")
        
        #ingest = bool(input("Do you wanna ingest data to MongoDB?"))

        #if search_clicked:
        

        #migrate = bool(input("Do you wanna migrate data to SQL?"))

        #if ingest_clicked:
        #transfer = st.button("Transfer")
        
        #table_form = st.form(key="form2")
        option = st.selectbox(
        "Choose any one to view table?",
        ("1. What are the names of all the videos and their corresponding channels?", 
        "2. Which channels have the most number of videos, and how many videos do they have?", 
        "3. What are the top 10 most viewed videos and their respective channels?",
        "4. How many comments were made on each video, and what are their corresponding video names?",
        "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
        "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
        "7. What is the total number of views for each channel, and what are their corresponding channel names?",
        "8. What are the names of all the channels that have published videos in the year 2022?",
        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "10. Which videos have the highest number of comments, and what are their corresponding channel names?"),
        placeholder="Choose an option")
        if(option):
            choosen_value(option, connect)
        
        
                       
            
        
    except Exception as e:
        print("Error in main "+str(e))