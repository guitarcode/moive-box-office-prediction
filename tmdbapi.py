import requests
import pandas as pd
import auth
import time
import numpy as np
from sqlalchemy import create_engine, text


class MovieDB:

    def __init__(self):
        # 데이터베이스 연결 설정
        self.host = 'localhost'
        self.database = 'big_data_movie'
        self.user = 'root'
        self.password = 'root'
        self.port = '3306'
        self.engine = create_engine(
            f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}')

    def __filter_duplicate_data(self, movie_df, actor_df):

        for index, row in movie_df.iterrows():
            movie_id = row['id']

            # 데이터베이스에서 해당 id를 가진 레코드 조회
            query = f"SELECT * FROM movies WHERE id = {movie_id}"

            if len(pd.read_sql_query(sql=text(query), con=self.engine)) != 0:
                movie_df.drop(index, inplace=True)

        for index, row in actor_df.iterrows():
            actor_id = row['id']

            query = f"SELECT * FROM actors WHERE id = {actor_id}"

            if len(pd.read_sql_query(sql=text(query), con=self.engine)) != 0:
                actor_df.drop(index, inplace=True)

    def save_to_database(self, movie_df, actor_df):
        movie_table_name = 'movies'
        actor_table_name = 'actors'
        # 데이터프레임을 데이터베이스 테이블로 저장

        self.__filter_duplicate_data(movie_df, actor_df)
        movie_df.to_sql(movie_table_name, con=self.engine, if_exists='append', index=False)
        actor_df.to_sql(actor_table_name, con=self.engine, if_exists='append', index=False)

    def close_connection(self):
        # 데이터베이스 연결 종료
        self.engine.dispose()

    def get_data_from_database(self):
        actquery = 'SELECT * FROM actors'
        movquery = 'SELECT * FROM movies'

        # movie 테이블 가져오기
        movies_df = pd.read_sql_query(sql=text(movquery), con=self.engine)

        # actor 테이블 가져오기
        actors_df = pd.read_sql_query(sql=text(actquery), con=self.engine)

        return movies_df, actors_df


class Movie:

    def __get_popular_movie(self, start_idx, end_idx):
        data = []

        # todo: 실제 수집시 range 바꿔야 함
        for page in range(start_idx, end_idx + 1):
            url = f"https://api.themoviedb.org/3/movie/popular?language=ko&page={page}&api_key={auth.get_api_key()}"

            response = requests.get(url, self.headers)
            json_response = response.json()
            print(f"get_popular_movie page: {page}")
            time.sleep(0.2)

            for movie in json_response['results']:
                try:
                    # 개봉연도가 2010년 이후인 영화만 저장
                    if movie['release_date'][:4] >= '2010':
                        genre_ids = movie['genre_ids']
                        genre_columns = [genre['name'] for genre in self.genres]
                        genre_values = {genre: 0 for genre in genre_columns}

                        for genre_id in genre_ids:
                            genre_name = next((genre['name'] for genre in self.genres if genre['id'] == genre_id), None)
                            if genre_name:
                                genre_values[genre_name] = 1

                        if not (genre_values['Animation'] == 1 or genre_values['Documentary'] == 1):
                            data.append({
                                'id': movie['id'],
                                'original_title': movie['original_title'],
                                'title': movie['title'],
                                'release_date': movie['release_date'],
                                **genre_values
                            })

                except KeyError:
                    # 필요한 키가 존재하지 않을 경우 건너뛰기
                    continue

        return pd.DataFrame(data)

    # df의 id 값을 기준으로 API 호출 및 정보 추가
    def __get_movie_detail(self, movie_df):
        for index, row in movie_df.iterrows():
            movie_id = row['id']

            # API 호출
            url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=ko&api_key={auth.get_api_key()}"
            response = requests.get(url, headers=self.headers)
            api_data = response.json()
            time.sleep(0.2)
            print(f"get_movie_detail id: {movie_id}")

            # 정보 추출 및 추가
            try:
                movie_df.at[index, 'adult'] = int(api_data['adult'])
                movie_df.at[index, 'revenue'] = api_data['revenue']
                movie_df.at[index, 'overview'] = api_data['overview']

            except KeyError:
                print('error')

    def __get_movie_credits(self, movie_df):
        actor_data = []
        # DataFrame 순회하며 API 요청 및 결과 저장
        for index, row in movie_df.iterrows():
            movie_id = row['id']

            url = f'https://api.themoviedb.org/3/movie/{movie_id}/credits?language=ko&api_key={auth.get_api_key()}'

            # API 요청 보내기
            response = requests.get(url, headers=self.headers)
            response_json = response.json()
            time.sleep(0.2)

            print(f"get_movie_credits: {movie_id}")

            # 요구사항에 맞는 정보 추출
            try:
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
                            movie_df.at[index, f'actor_id{i + 1}'] = actor_id

                        actor_data.append(
                            {
                                'id': actor_id,
                                'popularity': popularity,
                                'name': name
                            }
                        )
                        # actorDf에 actor_id가 이미 존재하는지 확인하여 저장
                        # todo: db에 저장할때 id가 있는지 확인해서 저장해야 됨

            except KeyError:
                # 필요한 키가 존재하지 않을 경우 건너뛰기
                print('error')

        actor_df = pd.DataFrame(actor_data)
        actor_df.drop_duplicates(subset='id', keep='first', inplace=True)

        return actor_df

    def make_movie_data_and_save_db(self, start_idx, end_idx):
        movie_df = self.__get_popular_movie(start_idx, end_idx)
        self.__get_movie_detail(movie_df)
        actor_df = self.__get_movie_credits(movie_df)
        self.db_util.save_to_database(movie_df=movie_df, actor_df=actor_df)
        # 데이터 프레임 초기화
        self.db_util.close_connection()

    def __init__(self):
        self.db_util = MovieDB()
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
                                         'adult', 'revenue', 'overview', 'actor_id1', 'actor_id2', 'actor_id3']

        self.actor_data_frame_columns = ['id', 'popularity', 'name']
