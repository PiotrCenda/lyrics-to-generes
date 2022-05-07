import pandas as pd
from tqdm import tqdm
import re
import lyricsgenius as lg

apikey = 'eb_tJSI8b5c6RTXRzUqMgk7aYkcER93KwLwqLTAT--a7x_4hGhnRJ-ldntEwxGMFziSZEYxCZg-B6lV7ncl4JQ'
genius = lg.Genius(apikey, skip_non_songs=True, excluded_terms=["(Remix)", "(Live)"], remove_section_headers=True)


def clean_lyrics(lyrics):
    lyrics = ' '.join(str(lyrics).splitlines()[:50])
    lyrics = ' '.join(str(lyrics).split()[:200])
    lyrics = re.sub("[\(\[\{].*?[\)\]\}]", "", lyrics)
    return lyrics


def get_lyrics(track, artist):
    lyrics = genius.search_song(track, artist)
    
    if lyrics:
        return clean_lyrics(lyrics.lyrics)
    else:
        return "NAN"

df_chunks = pd.read_csv(r"data_cleaned/data2.csv", iterator=True, chunksize=10)
mod = 'w'
header = True

for i, df in enumerate(df_chunks):
    try:
        df['lyrics'] = df.apply(lambda x: get_lyrics(x['track'], x['artist']), axis=1)
    except Exception as e:
        print(f"ups... {e}")
        
    df.to_csv(r"data_cleaned/data3.csv", index=False, mode=mod, header=header)
    
    mod = 'a'
    header = False