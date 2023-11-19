import streamlit as st
import pandas as pd
import numpy as np
import googleapiclient.discovery
import json
import re
from datetime import timedelta, datetime
from data_ingestion import data_ingestion
from data_mapping_with_transfer import data_mapping_with_transfer

class youtube_harvest:
    #Constructor
    def __init__(self, youtube, max_playlist, max_videos, max_comments, isToken_playlists, isToken_videos, response_back):
        self.youtube = youtube
        self.max_playlist = max_playlist
        self.max_videos = max_videos
        self.max_comments = max_comments
        self.isToken_playlists = isToken_playlists
        self.isToken_videos = isToken_videos
        self.response_back = response_back

    def convert_duration(self, duration_str):
        try:
            # Check if the duration string starts with 'PT'
            if not duration_str.startswith('PT'):
                return "Invalid duration format"

            duration_str = duration_str[2:]  # Remove 'PT' from the beginning
            hours = 0
            minutes = 0
            seconds = 0

            # Parse the duration string
            time_components = duration_str.split('H')  # Split at 'H' for hours
            if len(time_components) == 2:
                hours = int(time_components[0])
                duration_str = time_components[1]  # Remaining duration string after extracting hours
            else:
                duration_str = time_components[0]  # If no 'H' present, take the remaining string

            time_components = duration_str.split('M')  # Split at 'M' for minutes
            if len(time_components) == 2:
                minutes = int(time_components[0])
                duration_str = time_components[1]  # Remaining duration string after extracting minutes
            else:
                duration_str = time_components[0]

            time_components = duration_str.split('S')  # Split at 'S' for seconds
            if len(time_components) == 2:
                seconds = int(time_components[0])

            # Format the result as HH:MM:SS
            formatted_duration = f"{hours:02}:{minutes:02}:{seconds:02}"
            return formatted_duration

        except Exception as e:
            return(e)

    def channel_api_call(self, channel_id, channel):
        try:
            channel_request = self.youtube.channels().list(part="brandingSettings,statistics,status",id=channel_id)
            channel_response = channel_request.execute()
            Channel_Name = {'Channel_Name' : channel_response["items"][0]["brandingSettings"]["channel"]["title"] if channel_response["items"][0]["brandingSettings"]["channel"]["title"] else None,
                            'Channel_Id' : channel_response["items"][0]["id"],
                            'Subscription_Count' : int(channel_response["items"][0]["statistics"]["subscriberCount"]) if channel_response["items"][0]["statistics"]["subscriberCount"] else None,
                            'Channel_Views' : int(channel_response["items"][0]["statistics"]["viewCount"]) if channel_response["items"][0]["statistics"]["viewCount"] else 0,
                            'Channel_Description' : channel_response["items"][0]["brandingSettings"]["channel"]["description"] if channel_response["items"][0]["brandingSettings"]["channel"]["description"] else None,
                            'Channel_Type' : re.findall(r'"([^"]*)"', channel_response["items"][0]["brandingSettings"]["channel"]["keywords"])[:3] if channel_response["items"][0]["brandingSettings"]["channel"]["keywords"] else None,
                            'Channel_Status': channel_response["items"][0]["status"]["privacyStatus"] if channel_response["items"][0]["status"]["privacyStatus"] else None
                            }
            channel['Channel_Name'] = Channel_Name
            playlist_count = 1
            for playlist_item in self.playlist_id_api_call(channel_id):
                playlist_key = f'Playlist_Id_{playlist_count}'
                playlist_data = {
                'Playlist_Id': playlist_item['id'] if playlist_item['id'] else None,
                'Playlist_Name': playlist_item['snippet']['title'] if playlist_item['snippet']['title'] else None,
                'Video_Count': int(playlist_item['contentDetails']['itemCount']) if playlist_item['contentDetails']['itemCount'] else None
                }
                for key, value in self.videos_id_api_call(playlist_item['id']).items():
                #video_details = self.videos_id_api_call(playlist_item['id'])
                    playlist_data[key] = value
                #channel[playlist_key]['Videos'] = list(video_details.values())
                channel[playlist_key] = playlist_data
                playlist_count+=1
            self.response_back = channel
            return(channel)
        except Exception as e:
            return(e)
    
    def playlist_id_api_call(self, channel_id):
        try:
            #playlist_count = 1
            next_page_token = None
            while True:
                playlist_request = self.youtube.playlists().list(
                    part="snippet,contentDetails",
                    channelId=channel_id,
                    maxResults=self.max_playlist,
                    pageToken=next_page_token
                )
                playlist_response = playlist_request.execute()
                for item in playlist_response['items']:
                    #playlist_key = f'Playlist_Id_{playlist_count}'
                    yield item

                next_page_token = playlist_response.get("nextPageToken") if self.isToken_playlists == True else None
                if ((not next_page_token)):
                    break
        except Exception as e:
            return(e)

    def videos_id_api_call(self, playlist_id):
        try:
            video_details = {}
            next_page_token = None
            videos_count = 1
            while True:
                request = self.youtube.playlistItems().list(
                    part="snippet, contentDetails",
                    playlistId=playlist_id,
                    pageToken=next_page_token,
                    maxResults=self.max_videos
                    )
                response = request.execute()
                for item in response["items"]:
                    video_id = item["contentDetails"]["videoId"]
                    video_key = f'Videos_Id_{videos_count}'
                    video_details[video_key] = self.video_details_api_call(video_id)
                    videos_count+=1
                next_page_token = response.get("nextPageToken") if self.isToken_videos == True else None
                if not next_page_token:
                    break
            return video_details
        except Exception as e:
            return(e)

    def video_details_api_call(self, videoId):
        try:
            request = self.youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=videoId
                )
            response = request.execute()
            details = {}
            for item in response["items"]:
                details = {'Video_Id' : item['id'] if item['id'] else None,
                            'Video_Name' : item['snippet']['title'] if item['snippet']['title'] else None,
                            'Video_Description' : item['snippet']['description'] if item['snippet']['description'] else None,
                            'Tags' : [tag for tag in item['snippet']['tags']][:3] if 'tags' in item['snippet'] else None,
                            'PublishedAt' : datetime.strptime(item['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ') if 'publishedAt' in item['snippet'] else None,
                            'View_Count' : int(item['statistics']['viewCount']) if 'viewCount' in item['statistics'] else 0,
                            'Like_Count' : int(item['statistics']['likeCount']) if 'likeCount' in item['statistics'] else 0,
                            'Dislike_Count' : int(item['statistics']['dislikeCount']) if 'dislikeCount' in item['statistics'] else 0,
                            'Favorite_Count' : int(item['statistics']['favoriteCount']) if 'favoriteCount' in item['statistics'] else 0,
                            'Comment_Count' : int(item['statistics']['commentCount']) if 'commentCount' in item['statistics'] else 0,
                            'Duration' : self.convert_duration(item['contentDetails']['duration']) if('duration' in item['contentDetails']) else None,
                            'Thumbnail' : item['snippet']['thumbnails']['default']['url'] if item['snippet']['thumbnails']['default']['url'] else None,
                            'Caption_Status': "Available" if item['contentDetails']['caption'] == 'true' else "Not Available",
                            'Comments': self.comments_api_call(videoId) if('commentCount' in item['statistics'] and int(item['statistics']['commentCount']) != 0) else None
                            }
            if details != {}:
                return details
            else:
                return details
        except Exception as e:
            return(e)

    def comments_api_call(self, video_id):
        try:
            comment_count = 1
            comments = {}
            request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            textFormat="plainText",  # Optional: Set the text format as needed
            maxResults=self.max_comments  # Optional: Adjust the max number of comments to retrieve
            )
            response = request.execute()
            for item in response["items"]:
                comments_details = {'Comment_Id' : item['id'] if item['id'] else None,
                                    'Comment_Text' : item['snippet']['topLevelComment']['snippet']['textOriginal'] if item['snippet']['topLevelComment']['snippet']['textOriginal'] else None,
                                    'Comment_Author' : item['snippet']['topLevelComment']['snippet']['authorDisplayName'] if item['snippet']['topLevelComment']['snippet']['authorDisplayName'] else None,
                                    'Comment_PublishedAt' : datetime.strptime(item['snippet']['topLevelComment']['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ') if item['snippet']['topLevelComment']['snippet']['publishedAt'] else None
                                }
                key = f'Comment_Id_{comment_count}'
                comments[key] = comments_details
                comment_count += 1
            return comments
        except Exception as e:
            return(e)

    # def retrieve_response(self):
    #     try:
    #         res = self.response_back
    #         return res
    #     except Exception as e:
    #         return(e)
            
if __name__ == '__main__':

    def callback():
        st.session_state.button_clicked1 = True

    def callback1():
        st.session_state.button_clicked2 = True


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
        #session_state = {"button_clicked": True}
        channel_id = st.text_input('Channel ID', '', placeholder = 'Enter a channel id')
        max_playlist_per_page = st.slider('No. of playlist per page', 0, 50, value=5)
        max_videos_per_page = st.slider('No. of videos per page', 1, 50, value=5)
        max_comments_per_page = st.slider('No. of comments per page', 1, 100, value=20)
        isPageToken_playlist = st.checkbox('Do you wanna run with page token for playlists?', False)
        isPageToken_videos = st.checkbox('Do you wanna run with page token for videos?', False)

        youtube = googleapiclient.discovery.build("youtube", "v3", developerKey="AIzaSyCYwWfQG1T8TXjOPDEhuRAYfmGfHGToWDU")
        youtube_har = youtube_harvest(youtube, max_playlist_per_page, max_videos_per_page, max_comments_per_page, isPageToken_playlist, isPageToken_videos, response_back)

        local_host = "mongodb://localhost:27017/"
        db_name = "youtube_harvest"
        collection_val = "youtube_details"
        mongodb = data_ingestion(local_host, db_name, collection_val)

        sql = {"host": "localhost", "user": "root", "password": "Password123#@!","database":"YoutubeHarvesting"}
        sql_conn = data_mapping_with_transfer(sql)
        connect = sql_conn.connect_db()

        
        if(st.button("Search", on_click=callback) or st.session_state.button_clicked1):
            #search_clicked = True
            #st.session_state['search_clicked'] = search_clicked
            st.write("Search button clicked!")
            if channel_id != '' or channel_id is not None:
                response = youtube_har.channel_api_call(channel_id, channel)
                if response:
                    st.json(response, expanded=False)
                    if(st.button('Ingest Data', on_click=callback1)):
                        st.write("Ingest button clicked!")
                        if response:
                            check_val = mongodb.db_creation_with_data_ingestion(response)
                            if check_val["status"] == 'Success':
                                st.success("Data ingestion successful!")
                                if(st.button("Transfer", on_click=callback1) or st.session_state.button_clicked2):
                                    st.write("Transfer button clicked!")
                                    create_table_ddl = sql_conn.execute_ddl(connect['cursor'], connect['conn'])
                                    if(create_table_ddl):
                                        retrieve_values = mongodb.retrieve_data_after_ingestion(check_val["Collection"])
                                        for val in retrieve_values:
                                            connect = sql_conn.connect_db()
                                            map = sql_conn.insert_data(connect['cursor'], connect['conn'], val)
                                            st.success(map)
                                    else:
                                        st.warning("Tables not created")
                            else:
                                st.warning("Data ingestion failed!")
                else:
                    st.warning("No response from API")
            else:
                st.warning("Please enter a channel ID.")
        
        #ingest = bool(input("Do you wanna ingest data to MongoDB?"))

        #if search_clicked:
        

        #migrate = bool(input("Do you wanna migrate data to SQL?"))

        #if ingest_clicked:
        #transfer = st.button("Transfer")
        
                    
                       
            
        
    except Exception as e:
        print(e)