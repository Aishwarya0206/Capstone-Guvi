import re
from datetime import datetime

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
            return("Error in convert_duration "+str(e))

    def channel_api_call(self, channel_id, channel):
        try:
            channel_request = self.youtube.channels().list(part="brandingSettings,statistics,status",id=channel_id)
            channel_response = channel_request.execute()
            Channel_Name = {'Channel_Name' : channel_response["items"][0]["brandingSettings"]["channel"]["title"] if channel_response["items"][0]["brandingSettings"]["channel"]["title"] else None,
                            'Channel_Id' : channel_response["items"][0]["id"],
                            'Subscription_Count' : int(channel_response["items"][0]["statistics"]["subscriberCount"]) if channel_response["items"][0]["statistics"]["subscriberCount"] else None,
                            'Channel_Views' : int(channel_response["items"][0]["statistics"]["viewCount"]) if channel_response["items"][0]["statistics"]["viewCount"] else None,
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
            return("Error in channel_api_call "+str(e))
    
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
            return("Error in playlist_id_api_call "+str(e))

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
            return("Error in videos_id_api_call "+str(e))

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
            return("Error in video_details_api_call "+str(e))

    def comments_api_call(self, video_id):
        try:
            comment_count = 1
            comments = {}
            request = self.youtube.commentThreads().list(
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
            return("Error in comments_api_call "+str(e))