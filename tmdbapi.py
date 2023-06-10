import requests
import pandas as pd
import auth
import numpy as np


class Movie:

    def __get_popular_movie(self):
        data = []

        # todo: 실제 수집시 range 바꿔야 함
        for page in range(1, 2):
            url = f"https://api.themoviedb.org/3/movie/popular?language=ko&page={page}&api_key={auth.get_api_key()}"

            response = requests.get(url, self.headers)
            json_response = response.json()
            for movie in json_response['results']:
                # 개봉연도가 2010년 이후인 영화만 저장
                if movie['release_date'][:4] >= '2010':
                    # genre_ids를 이용하여 genre 컬럼 생성
                    genre_ids = movie['genre_ids']
                    genre_columns = [genre['name'] for genre in self.genres if genre['id'] in genre_ids]
                    genre_values = [1 if genre in genre_columns else 0 for genre in self.genres]

                    # 데이터 추가
                    data.append({
                        'id': movie['id'],
                        'original_title': movie['original_title'],
                        'title': movie['title'],
                        'release_date': movie['release_date'],
                        **dict(zip(genre_columns, genre_values))
                    })

            df = pd.DataFrame(data)
            self.movie_df = pd.concat([self.movie_df, df], ignore_index=True)

    # df의 id 값을 기준으로 API 호출 및 정보 추가
    def __get_movie_detail(self):
        for index, row in self.movie_df.iterrows():
            movie_id = row['id']

            # API 호출
            url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=ko&api_key={auth.get_api_key()}"
            response = requests.get(url, headers=self.headers)
            api_data = response.json()

            # 정보 추출 및 추가
            if 'adult' in api_data:
                self.movie_df.at[index, 'adult'] = int(api_data['adult'])
            self.movie_df.at[index, 'revenue'] = api_data['revenue']
            self.movie_df.at[index, 'overview'] = api_data['overview']
            # time.sleep(0.5)

    def __get_movie_credits(self):
        # DataFrame 순회하며 API 요청 및 결과 저장
        for index, row in self.movie_df.iterrows():
            movie_id = row['id']

            url = f'https://api.themoviedb.org/3/movie/{movie_id}/credits?language=ko&api_key={auth.get_api_key()}'

            # API 요청 보내기
            response = requests.get(url, headers=self.headers)
            response_json = response.json()

            # 요구사항에 맞는 정보 추출
            cast_list = response_json.get('cast', [])
            acting_casts = [cast for cast in cast_list if cast.get('known_for_department') == 'Acting']

            if acting_casts:
                # popularity 기준으로 정렬하여 가장 인기 있는 배우 정보 추출
                sorted_casts = sorted(acting_casts, key=lambda x: x.get('popularity'), reverse=True)
                for i in range(len(sorted_casts)):
                    cast = sorted_casts[i]
                    actor_id = cast.get('id')
                    popularity = cast.get('popularity')
                    name = cast.get('original_name')

                    if i < 3:
                        self.movie_df.at[index, f'actor_id{i + 1}'] = actor_id

                    # actorDf에 actor_id가 이미 존재하는지 확인하여 저장
                    # todo: db에 저장할때 id가 있는지 확인해서 저장해야 됨
                    if actor_id not in self.actor_df['id'].values:
                        self.actor_df = pd.concat([self.actor_df, pd.DataFrame(
                            {'id': [actor_id], 'popularity': [popularity], 'name': [name]})], ignore_index=True)

    def make_movie_data_frame(self):
        self.__get_popular_movie()
        self.__get_movie_detail()
        self.__get_movie_credits()

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
        self.movie_data_frame_columns = ['id', 'original_title', 'title', 'release_date', 'Action', 'Crime',
                                         'Thriller', 'Adventure', 'Animation', 'Comedy', 'Family', 'Fantasy',
                                         'Science Fiction', 'Horror', 'Romance', 'Drama', 'Mystery', 'War',
                                         'adult', 'revenue', 'overview', 'actor_id1', 'popularity1', 'actor_id2',
                                         'popularity2', 'actor_id3', 'popularity3']
        self.movie_df = pd.DataFrame(columns=self.movie_data_frame_columns)

        self.actor_data_frame_columns = ['id', 'popularity', 'name']
        self.actor_df = pd.DataFrame(columns=self.actor_data_frame_columns)
