import json
import boto3
import pandas as pd
import os
from io import StringIO
from datetime import datetime

def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['name']
        album_href = row['track']['album']['href']
        album_popularity = row['track']['popularity']
        album_total_tracks = row['track']['album']['total_tracks']
        album_element = {"album_ID" : album_id, "album_name" : album_name, "album_href":album_href , "album_popularity":album_popularity, "album_total_tracks": album_total_tracks}
        album_list.append(album_element)
    return album_list



def song(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_url = row['track']['album']['external_urls']['spotify']
        song_release_date = row['track']['album']['release_date']
        song_duration = row['track']['duration_ms']
        song_element = {"song_ID" : song_id, "song_name" : song_name, "song_url":song_url ,"song_release_date" : song_release_date, "song_duration(ms)":song_duration }
        song_list.append(song_element)
    return song_list



def artists(data):
    artist_list = []
    for row in data['items']:
        for key,val in row.items():
            if key == "track":
                for artist in val['artists']:
                    artist_dict = {"artist_ID":artist['id'], "artist_name" : artist['name'], "artist_href":artist['uri']}
                    artist_list.append(artist_dict)
    return artist_list




def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "Bucket-Name"
    Key = "Folder-Path"

    spotify_data = []
    spotify_key = []
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket = Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_key.append(file_key)

    for data in spotify_data:
        album_list = album(data)
        artist_list = artists(data)
        songs_list = song(data)   
        
        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset=['album_ID'])
        song_df = pd.DataFrame.from_dict(songs_list)
        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_ID'])
        
        song_df['song_duration(ms)'] = round(song_df['song_duration(ms)']/60000, 2)
        song_df.rename(columns={'song_duration(ms)':'song_duration(min)'}, inplace=True)
        #song_df['song_release_date'] = pd.to_datetime(song_df['song_release_date'])

        songs_key = "transformed_data/songs_data/songs_transformed" + str(datetime.now()) + ".csv"
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=songs_key, Body=song_content)

        album_key = "transformed_data/album_data/album_transformed" + str(datetime.now()) + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)

        artist_key = "transformed_data/artists_data/artists_transformed" + str(datetime.now()) + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)
        s3_resource = boto3.resource('s3')
        for key in spotify_key:
            copy_source = {
                'Bucket' : Bucket,
                'Key' : key
            }

        s3_resource.meta.client.copy(copy_source, Bucket, 'Folder-Path' + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()
