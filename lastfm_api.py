import requests
import json
import cfg


def get_response(params):
    url = 'https://ws.audioscrobbler.com/2.0/'
    headers = {'user-agent': cfg.USER_AGENT}
    payload = {
        'api_key': cfg.API_KEY,
        'format': 'json'
    }
    payload.update(params)
    response = requests.get(url, headers=headers, params=payload)
    return response


def json_print(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


def get_album_tracklist(artist, album):
    params = {
        'method': 'album.getInfo',
        'artist': artist,
        'album': album
    }
    r = get_response(params)
    album_info = r.json()['album']['tracks']['track']
    album_tracklist = {}
    for track in album_info:
        album_tracklist[track['name']] = get_track_playcount(artist, track['name'])
    return album_tracklist


def get_track_playcount(artist, track):
    params = {
        'method': 'track.getInfo',
        'artist': artist,
        'track': track
    }
    r = get_response(params)

    track_info = r.json()['track']['playcount']
    return int(track_info)


def get_album_stat(albums_dict):
    stat_dict = {}
    for artist in albums_dict:
        temp = {}
        for album in albums_dict[artist]:
            temp[album] = get_album_tracklist(artist, album)
        stat_dict[artist] = temp
    return stat_dict


albums_dict = {
    'Deftones': ['Adrenaline', 'Around The Fur', 'White Pony'],
    'Fleshwater': ["We're Not Here to Be Loved", '2000: In Search Of The Endless Sky']
}
album_info = get_album_stat(albums_dict)
json_print(album_info)