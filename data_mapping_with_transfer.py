import mysql.connector

class data_mapping_with_transfer:
    def __init__(self, sql):
        self.host = sql["host"]
        self.user = sql["user"]
        self.password = sql["password"]
        self.database = sql["database"]

    def duration_to_seconds(self, duration):
        # Split the duration string into hours, minutes, and seconds
        hours, minutes, seconds = map(int, duration.split(':'))

        # Calculate total seconds
        total_seconds = hours * 3600 + minutes * 60 + seconds
        return total_seconds

    def connect_db(self):
        try:
            db_conn = mysql.connector.connect(host=self.host,user=self.user,password=self.password,database=self.database,auth_plugin='mysql_native_password')
            cursor_db=db_conn.cursor()
            return({"cursor": cursor_db, "conn": db_conn})
        except Exception as e:
            return (f"Error connecting to MySQL database: {e}")

    def execute_ddl(self, cursor_db, db_conn):
        try:
            channel_sql ='''CREATE TABLE IF NOT EXISTS Channel (channel_id varchar(255) NOT NULL, channel_name varchar(255), channel_type varchar(255), channel_views bigint, channel_description text , channel_status varchar(255), constraint pk Primary Key (channel_id));'''
            cursor_db.execute(channel_sql)
            playlist_sql = '''CREATE TABLE IF NOT EXISTS Playlist (playlist_id varchar(255) NOT NULL, channel_id varchar(255), playlist_name varchar(255),  constraint pk Primary Key (playlist_id), constraint fk_channel Foreign Key (channel_id) REFERENCES Channel(channel_id));'''
            cursor_db.execute(playlist_sql)
            video_sql = '''CREATE TABLE IF NOT EXISTS  Video (video_id varchar(255) NOT NULL, playlist_id varchar(255), video_name varchar(255), video_description text, published_date datetime, tags varchar(255), view_count int, like_count int, dislike_count int, favorite_count int, comment_count int, duration int, thumbnail varchar(255), caption_status varchar(255), constraint pk Primary Key (video_id), constraint fk_playlist Foreign Key (playlist_id) REFERENCES Playlist(playlist_id));'''
            index_playlist = '''CREATE INDEX Playlist_index ON Playlist(playlist_id);'''
            cursor_db.execute(index_playlist)
            cursor_db.execute(video_sql)
            comment_sql = '''CREATE TABLE IF NOT EXISTS Comment (comment_id varchar(255) NOT NULL, video_id varchar(255), comment_text text, comment_author varchar(255), comment_published_date datetime, constraint pk Primary Key (comment_id), constraint fk_video Foreign Key (video_id) REFERENCES Video(video_id));'''
            index_video = '''CREATE INDEX Video_index ON Video(video_id);'''
            cursor_db.execute(index_video)
            cursor_db.execute(comment_sql)
            db_conn.commit() #save 
            #db_connection.rollback() #undo
            #self.close_connection(cursor_db, db_conn)
            return("Tables created")
        except Exception as e:
            db_conn.rollback()
            return("Error in execute_ddl "+str(e))

    def close_connection(self, cursor_db, db_conn):
        db_conn.commit()
        cursor_db.close()
        db_conn.close()

    # def select_channel_id(self, cursor_db, db_conn, chan_id):
    #     select_channel_query = '''SELECT channel_id FROM Channel WHERE channel_id=%s'''
    #     cursor_db.execute(select_channel_query, (chan_id,))
    #     existing = cursor_db.fetchone()
    #     return(existing)

    def insert_channel(self, cursor_db, db_conn, channel_data):
        try:
            insert_channel_query = '''INSERT INTO Channel (channel_id, channel_name, channel_type, channel_views, channel_description, channel_status) VALUES (%s, %s, %s, %s, %s, %s);'''
            channel_type = ', '.join(channel_data['Channel_Type']) if channel_data['Channel_Type'] else None
            print(channel_type)
            channel_desp = channel_data['Channel_Description'].replace("'", "''").replace('\n', ' ')
            print(channel_desp)
            #channel_view_count = channel_data['Channel_Views']['$numberLong'] if '$numberLong' in channel_data['Channel_Views'] else channel_data['Channel_Views']
            #print(channel_view_count)
            values = (
                    channel_data['Channel_Id'],
                    channel_data['Channel_Name'].encode('ascii', 'ignore').decode('ascii'),
                    channel_type.encode('ascii', 'ignore').decode('ascii'),
                    channel_data['Channel_Views'],
                    channel_desp.encode('ascii', 'ignore').decode('ascii'),
                    channel_data['Channel_Status']
                )
            print(insert_channel_query % values)
            cursor_db.execute(insert_channel_query, values)
            db_conn.commit()
            #self.close_connection(cursor_db, db_conn)
            return("Channel added")
        except Exception as e:
            db_conn.rollback()
            return(f"Error in insert_channel: {str(e)}")
        

    def insert_playlist(self, cursor_db, db_conn, playlist_data, channel_id):
        try:
            # Insert playlist data into MySQL Playlist table
            insert_playlist_query = '''INSERT IGNORE INTO Playlist (playlist_id,channel_id,playlist_name) VALUES (%s, %s, %s);'''
            values = (
                playlist_data['Playlist_Id'],
                channel_id,
                playlist_data['Playlist_Name'].encode('ascii', 'ignore').decode('ascii') if playlist_data['Playlist_Name'] else None
            )
            cursor_db.execute(insert_playlist_query, values)
            db_conn.commit()
            #self.close_connection(cursor_db, db_conn)
            return("Playlist added")
        except Exception as e:
            db_conn.rollback()
            return(f"Error in insert_playlist : {str(e)}")

    def insert_video(self, cursor_db, db_conn, video_data, playlist_id):
        try:
            # Insert video data into MySQL Video table
            insert_video_query = '''INSERT IGNORE INTO Video (video_id, playlist_id, video_name, video_description, published_date, tags, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
            video_id = video_data['Video_Id']
            video_duration = self.duration_to_seconds(video_data['Duration'])
            tags = ', '.join(video_data['Tags']) if video_data['Tags'] else None
            values = (
                video_id,
                playlist_id,
                video_data['Video_Name'].encode('ascii', 'ignore').decode('ascii') if video_data['Video_Name'] else None,
                video_data['Video_Description'].encode('ascii', 'ignore').decode('ascii') if video_data['Video_Description'] else None,
                video_data['PublishedAt'],
                tags,
                video_data['View_Count'],
                video_data['Like_Count'],
                video_data['Dislike_Count'],
                video_data['Favorite_Count'],
                video_data['Comment_Count'],
                video_duration,
                video_data['Thumbnail'],
                video_data['Caption_Status']
            )
            cursor_db.execute(insert_video_query, values)
            db_conn.commit()
            #self.close_connection(cursor_db, db_conn)
            return("Videos added")
        except Exception as e:
            db_conn.rollback()
            return(f"Error in insert_video: {str(e)}")

    def insert_comment(self, cursor_db, db_conn, comment_data, video_id):
        try:
            # Insert comment data into MySQL Comment table
            insert_comment_query = '''INSERT IGNORE INTO Comment (comment_id,video_id,comment_text,comment_author,comment_published_date) VALUES (%s, %s, %s, %s, %s);'''
            values = (
                comment_data['Comment_Id'],
                video_id,
                comment_data['Comment_Text'].encode('ascii', 'ignore').decode('ascii') if comment_data['Comment_Text'] else None,
                comment_data['Comment_Author'].encode('ascii', 'ignore').decode('ascii') if comment_data['Comment_Author'] else None,
                comment_data['Comment_PublishedAt']
            )
            cursor_db.execute(insert_comment_query, values)
            db_conn.commit()
            #self.close_connection(cursor_db, db_conn)
            return("Comments added")
        except Exception as e:
            db_conn.rollback()
            return(f"Error in insert_comment: {str(e)}")

    def insert_data(self, cursor_db, db_conn, value_from_doc):
        try:
            channel_name_data = value_from_doc.get('Channel_Name')
            chann_insert = self.insert_channel(cursor_db, db_conn, channel_name_data)
            if chann_insert:
                for play_key, play_value in value_from_doc.items():
                    if(play_key.startswith('Playlist_Id')):
                        playlist_data = play_value
                        insert_play = self.insert_playlist(cursor_db, db_conn,playlist_data, channel_name_data['Channel_Id'])
                        if(insert_play):
                            for video_key, video_value in playlist_data.items():
                                if video_key.startswith('Videos_Id'):
                                    video_data = video_value
                                    if(video_data):
                                        insert_video = self.insert_video(cursor_db, db_conn, video_data, playlist_data['Playlist_Id'])
                                        if(insert_video):
                                            if(video_data['Comments']):
                                                for comment_key, comment_value in video_data['Comments'].items():
                                                    if comment_key.startswith('Comment_Id'):
                                                        comment_data = comment_value
                                                        insert_comm = self.insert_comment(cursor_db, db_conn, comment_data, video_data['Video_Id'])
            #self.close_connection(cursor_db, db_conn)
                return("Transaction was successful!")
            else:
                return("Transaction failed")
        except Exception as e:
            db_conn.rollback()
            return(f"Error in insert_data : {str(e)}")

    def getAllVideosAndChannels(self, cursor_db, db_conn):
        try:
            query = "SELECT video_name, channel_name FROM Video vid LEFT JOIN Playlist play ON play.playlist_id =  vid.playlist_id LEFT JOIN Channel ch ON ch.channel_id = play.channel_id;"
            cursor_db.execute(query)
            result = cursor_db.fetchall()
            return result
        except:
            return(f"Error in getAllVideosAndChannels: {str(e)}")

    def channelNameMostVideo(self, cursor_db, db_conn):
        try:
            query = "SELECT channel_name, COUNT(video_id) as video_count from Video vid LEFT JOIN Playlist play ON play.playlist_id = vid.playlist_id LEFT JOIN Channel ch ON ch.channel_id = play.channel_id GROUP BY play.channel_id ORDER BY video_count DESC;"
            cursor_db.execute(query)
            result = cursor_db.fetchall()
            return result
        except:
            return(f"Error in channelNameMostVideo: {str(e)}")

    def topTenViewedVideosWithChannels(self, cursor_db, db_conn):
        try:
            query = "SELECT channel_name, video_name from Video vid LEFT JOIN Playlist play ON play.playlist_id = vid.playlist_id LEFT JOIN Channel ch ON ch.channel_id = play.channel_id ORDER BY view_count DESC LIMIT 10;"
            cursor_db.execute(query)
            result = cursor_db.fetchall()
            return result
        except:
            return(f"Error in topTenViewedVideosWithChannels: {str(e)}")

    def commentCountForEachVideos(self, cursor_db, db_conn):
        try:
            query = "SELECT video_name, comment_count FROM Video;"
            cursor_db.execute(query)
            result = cursor_db.fetchall()
            return result
        except:
            return(f"Error in commentCountForEachVideos: {str(e)}")

    def highestLikesChannels(self, cursor_db, db_conn):
        try:
            query = "SELECT channel_name, video_name, like_count from Video vid LEFT JOIN Playlist play ON play.playlist_id = vid.playlist_id LEFT JOIN Channel ch ON ch.channel_id = play.channel_id ORDER BY like_count DESC;"
            cursor_db.execute(query)
            result = cursor_db.fetchall()
            return result
        except:
            return(f"Error in highestLikesChannels: {str(e)}")

    def totalLikesOrDislikesOfEachVideo(self, cursor_db, db_conn):
        try:
            query = "SELECT video_name, like_count, dislike_count FROM Video ORDER BY like_count DESC;"
            cursor_db.execute(query)
            result = cursor_db.fetchall()
            return result
        except:
            return(f"Error in totalLikesOrDislikesOfEachVideo: {str(e)}")

    def totalViewsAndChannelName(self, cursor_db, db_conn):
        try:
            query = "SELECT channel_name, channel_views FROM Channel ORDER BY channel_views DESC;"
            cursor_db.execute(query)
            result = cursor_db.fetchall()
            return result
        except:
            return(f"Error in totalViewsAndChannelName: {str(e)}")

    def publishedVideosChannels(self, cursor_db, db_conn):
        try:
            query = "SELECT channel_name FROM Channel ch LEFT JOIN Playlist play ON play.channel_id = ch.channel_id LEFT JOIN Video vid ON vid.playlist_id = play.playlist_id WHERE year(vid.published_date)=2022 GROUP BY ch.channel_id;"
            cursor_db.execute(query)
            result = cursor_db.fetchall()
            return result
        except:
            return(f"Error in publishedVideosChannels: {str(e)}")

    def avgDurationOfAllVideos(self, cursor_db, db_conn):
        try:
            query = "SELECT channel_name, avg(vid.duration) AS average_video_duration FROM Channel ch LEFT JOIN Playlist play ON play.channel_id = ch.channel_id LEFT JOIN Video vid ON vid.playlist_id = play.playlist_id GROUP BY ch.channel_id ORDER BY average_video_duration DESC;"
            cursor_db.execute(query)
            result = cursor_db.fetchall()
            return result
        except:
            return(f"Error in avgDurationOfAllVideos: {str(e)}")

    def highestCommentAndChannels(self, cursor_db, db_conn):
        try:
            query = "SELECT channel_name, SUM(vid.comment_count) AS comment_count FROM Channel ch LEFT JOIN Playlist play ON play.channel_id = ch.channel_id LEFT JOIN Video vid ON vid.playlist_id = play.playlist_id GROUP BY ch.channel_id ORDER BY comment_count DESC;"
            cursor_db.execute(query)
            result = cursor_db.fetchall()
            return result
        except:
            return(f"Error in highestCommentAndChannels: {str(e)}")