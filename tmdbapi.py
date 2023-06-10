import requests
import pandas as pd
import auth
import time
import numpy as np


class Movie:

    def get_popular_movie(self):
        data = []

        for page in range(1, 2):
            url = f'https://api.themoviedb.org/3/movie/top_rated?language=ko&{page}'

            response = requests.get(url + str(page), self.headers)
            json_response = response.json()
            for movie in json_response['results']:
                # 개봉연도가 2010년 이후인 영화만 저장
                if movie['release_date'][:4] >= '2010':
                    # genre_ids를 이용하여 genre 컬럼 생성
                    genre_ids = movie['genre_ids']
                    genre_columns = [genre['name'] for genre in self.genres if genre['id'] in genre_ids]
                    genre_values = [1 if genre in genre_columns else 0 for genre in genre_columns]

                    # 데이터 추가
                    data.append({
                        'id': movie['id'],
                        'original_title': movie['original_title'],
                        'title': movie['title'],
                        'release_date': movie['release_date'],
                        **dict(zip(genre_columns, genre_values))
                    })

        df = pd.DataFrame(data)

        return df

    # df의 id 값을 기준으로 API 호출 및 정보 추가
    def get_movie_detail(self, df):
        for index, row in df.iterrows():
            movie_id = row['id']
            animation_genre = row['Animation']

            # Animation 컬럼이 1인 경우에만 실행
            if np.isnan(animation_genre):
                # API 호출
                url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=ko"
                response = requests.get(url, headers=self.headers)
                api_data = response.json()

                # 정보 추출 및 추가
                df.at[index, 'adult'] = int(api_data['adult'])
                df.at[index, 'revenue'] = api_data['revenue']
                df.at[index, 'overview'] = api_data['overview']
                time.sleep(0.5)

    def get_movie_credits(self, df):
        # DataFrame 순회하며 API 요청 및 결과 저장
        for index, row in df.iterrows():

            movie_id = row['id']

            url = f'https://api.themoviedb.org/3/movie/{movie_id}/credits?language=ko'

            # API 요청 보내기
            response = requests.get(url, headers=self.headers)
            response_json = response.json()

            # 요구사항에 맞는 정보 추출
            cast_list = response_json.get('cast', [])
            acting_casts = [cast for cast in cast_list if cast.get('known_for_department') == 'Acting']

            # 결과를 저장할 리스트 생성
            actor_ids = []
            popularity_list = []

            if acting_casts:
                # popularity 기준으로 정렬하여 가장 인기 있는 배우 정보 추출
                sorted_casts = sorted(acting_casts, key=lambda x: x.get('popularity'), reverse=True)
                actor_id = sorted_casts[0].get('id')
                popularity = sorted_casts[0].get('popularity')

                # 결과 저장
                actor_ids.append(actor_id)
                popularity_list.append(popularity)
            else:
                # 해당하는 배우 정보가 없을 경우에 대한 처리
                actor_ids.append(None)
                popularity_list.append(None)

            # 결과를 DataFrame에 추가
            df['actor_id'] = actor_ids
            df['popularity'] = popularity_list

    def __init__(self):
        self.headers = auth.get_header()
        self.genres = [
            {"id": 28, "name": "Action"},
            {"id": 12, "name": "Adventure"},
            {"id": 16, "name": "Animation"},
            {"id": 35, "name": "Comedy"},
            {"id": 80, "name": "Crime"},
            {"id": 99, "name": "Documentary"},
            {"id": 18, "name": "Drama"},
            {"id": 10751, "name": "Family"},
            {"id": 14, "name": "Fantasy"},
            {"id": 36, "name": "History"},
            {"id": 27, "name": "Horror"},
            {"id": 10402, "name": "Music"},
            {"id": 9648, "name": "Mystery"},
            {"id": 10749, "name": "Romance"},
            {"id": 878, "name": "Science Fiction"},
            {"id": 10770, "name": "TV Movie"},
            {"id": 53, "name": "Thriller"},
            {"id": 10752, "name": "War"},
            {"id": 37, "name": "Western"}
        ]