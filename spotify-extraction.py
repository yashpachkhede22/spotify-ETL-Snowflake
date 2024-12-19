import json
import os     # to access global variables
import boto3  # to load our data into s3
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    client_credentials_manager = SpotifyClientCredentials(client_id = client_id, client_secret = client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlist = "https://open.spotify.com/playlist/4zzUm9eZmeb4t4nUCaNoo5"
    playlist_URI = playlist.split("/")[-1]
    data = sp.playlist_tracks(playlist_URI)
    client = boto3.client('s3')

    file_name = "spotify_raw" + str(datetime.now()) +".json"

    client.put_object(
        Bucket = "Bucket-Name",
        Key = "Folder-Path" + file_name,
        Body = json.dumps(data)
    )
