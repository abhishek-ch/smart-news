from pymongo import MongoClient



class FetchValue(object):

    counter = 0
    def __init__(self):
        FetchValue.counter += 1
        self.data = []
        self.date_based_value_map = {}



    def fetch_data_from_db(self,date):
        client = MongoClient()
        db = client.news  # use db
        data = db.newswebdump.find({"date": date,'feature':{'$exists':False}})
        return data



    def get_next_value(self,date_time,index = 0):
        extract_date = date_time.split()
        date = extract_date[0]
        try:
            if date not in self.date_based_value_map:
                self.data = self.fetch_data_from_db(date)
                self.date_based_value_map[date] = self.data
                return self.data[index]
            else:
                get_values = self.date_based_value_map[date]
                return get_values[index]
        except Exception,e:
            print("No value Left ",e)
            return -1


