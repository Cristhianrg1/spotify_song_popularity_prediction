import requests
import pandas as pd
import time
from datetime import datetime
from utils.get_spotify_token import get_token


def fetch_tracks(session, headers, genre, limit, offset):
    """Fetch tracks from Spotify by genre."""
    search_url = f"https://api.spotify.com/v1/search?q=genre:{genre}&type=track&limit={limit}&offset={offset}"
    response = session.get(search_url, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json().get("tracks", {}).get("items", [])


def fetch_audio_features(session, headers, track_ids):
    """Fetch audio features for a list of track IDs."""
    audio_features_url = (
        f'https://api.spotify.com/v1/audio-features?ids={",".join(track_ids)}'
    )
    response = session.get(audio_features_url, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json().get("audio_features", [])


def fetch_artist_data(session, headers, artist_ids):
    """Fetch artist data for a list of artist IDs in blocks of 50."""
    artist_data = []
    for i in range(0, len(artist_ids), 50):
        artist_url = (
            f'https://api.spotify.com/v1/artists?ids={",".join(artist_ids[i:i+50])}'
        )
        response = session.get(artist_url, headers=headers, timeout=10)
        response.raise_for_status()
        artist_data.extend(response.json().get("artists", []))
    return artist_data


def update_tracks_with_audio_features(all_songs_data, audio_features_list):
    """Update track data with audio features."""
    for track_data, audio_features in zip(
        all_songs_data[-len(audio_features_list) :], audio_features_list
    ):
        if audio_features:
            track_data.update(
                {
                    "danceability": audio_features.get("danceability"),
                    "energy": audio_features.get("energy"),
                    "key": audio_features.get("key"),
                    "loudness": audio_features.get("loudness"),
                    "mode": audio_features.get("mode"),
                    "speechiness": audio_features.get("speechiness"),
                    "acousticness": audio_features.get("acousticness"),
                    "instrumentalness": audio_features.get("instrumentalness"),
                    "liveness": audio_features.get("liveness"),
                    "valence": audio_features.get("valence"),
                    "tempo": audio_features.get("tempo"),
                    "time_signature": audio_features.get("time_signature"),
                }
            )


def update_tracks_with_artist_data(all_songs_data, artist_data_list):
    """Update track data with artist data."""
    artist_dict = {artist["id"]: artist for artist in artist_data_list}
    for track_data in all_songs_data:
        artist_data = artist_dict.get(track_data["artist_id"])
        if artist_data:
            track_data.update(
                {
                    "artist_genres": artist_data.get("genres", []),
                    "artist_popularity": artist_data.get("popularity"),
                    "artist_followers": artist_data.get("followers", {}).get("total"),
                }
            )


def get_songs_data_by_genre(genre, limit=50, max_results=10000):
    headers = {"Authorization": f"Bearer {get_token()}"}
    all_songs_data = []
    offset = 0
    session = requests.Session()

    while offset < max_results:
        try:
            tracks = fetch_tracks(session, headers, genre, limit, offset)
            if not tracks:
                break

            track_ids = []
            artist_ids = []
            for track in tracks:
                track_id = track["id"]
                artist_id = track["artists"][0]["id"]
                track_ids.append(track_id)
                artist_ids.append(artist_id)

                all_songs_data.append(
                    {
                        "track_name": track["name"],
                        "track_id": track_id,
                        "artist_name": track["artists"][0]["name"],
                        "artist_id": artist_id,
                        "album_name": track["album"]["name"],
                        "release_date": track["album"]["release_date"],
                        "duration_ms": track["duration_ms"],
                        "explicit": track["explicit"],
                        "track_number": track["track_number"],
                        "popularity": track["popularity"],
                        "data_collection_date": datetime.today().date(),
                    }
                )

            # Obtener características de audio en bloques
            if track_ids:
                try:
                    audio_features_list = fetch_audio_features(
                        session, headers, track_ids
                    )
                    update_tracks_with_audio_features(
                        all_songs_data, audio_features_list
                    )
                except requests.exceptions.RequestException as e:
                    print(f"Error al obtener características de audio: {e}")

            # Obtener datos de los artistas en bloques
            unique_artist_ids = list(set(artist_ids))
            if unique_artist_ids:
                try:
                    artist_data_list = fetch_artist_data(
                        session, headers, unique_artist_ids
                    )
                    update_tracks_with_artist_data(all_songs_data, artist_data_list)
                except requests.exceptions.RequestException as e:
                    print(f"Error al obtener datos de artistas: {e}")

            offset += limit
            time.sleep(1)  # Pausa para evitar el límite de API

        except requests.exceptions.RequestException as e:
            print(
                f"Error al realizar la búsqueda de género {genre} en offset {offset}: {e}"
            )
            offset += limit  # Aumentar el offset para evitar bucles infinitos
            time.sleep(5)  # Pausa en caso de error antes de continuar

    session.close()
    return pd.DataFrame(all_songs_data)
