import pandas as pd
import requests
import sys
import concurrent.futures

"""
   Requires python 3.2 or higher version of python
   This script collects reviews using themoviedb api
   The first argument need to be a file where the timdbId are stored
   The second argument is the csv file where the json data is written
"""

API_KEY = "26664fb2a3597834285c93b7a58dcb1a"

main_api = "https://api.themoviedb.org/3/movie/x/reviews?api_key=26664fb2a3597834285c93b7a58dcb1a&language=en-US"

reviews = {}


def getReview(id):
    request = requests.get(main_api.replace('x',str(id)))
    if(request.status_code == 200):
        json_review = request.json()
        reviews[str(id)] = json_review
        
def main():
    try:
        if len(sys.argv) < 3:
            print("Script missing arguments")
            sys.exit(0)
        
        links = pd.read_csv(sys.argv[1])
        numberofRows = len(links)
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(getReview,links['tmdbId'])
            
        csv_dataframe = pd.DataFrame.from_dict(reviews,orient = 'index')
        csv_dataframe.to_csv(sys.argv[2])
        
    except Exception as e:
        sys.exit(0)
   
    

if __name__ == '__main__':
    main()