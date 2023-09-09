from sys import settrace
from database import Database
from bson.objectid import ObjectId


class DeviceModel:
    DEVICE_COLLECTION = 'devices'

    def __init__(self):
        self._db = Database()
        self._latest_error = ''

    @property
    def latest_error(self):
        return self._latest_error

    @latest_error.setter
    def latest_error(self, latest_error):
        self._latest_error = latest_error

    def find_by_device_id(self, device_id):
        device_id = {'device_id': device_id}
        result = self._db.get_single_data(DeviceModel.DEVICE_COLLECTION, device_id)
        return self.__find(result)

    def find_by_object_id(self, object_id):
        key = {'_id': ObjectId(object_id)}
        return self.__find(key)

    def __find(self, key):
        result = self._db.get_single_data(DeviceModel.DEVICE_COLLECTION, key)
        return result

    def insert(self, device_id, desc, type, manufacturer):
        self.latest_error = ''
        document = self.find_by_device_id(device_id)

        if (document):
            self.latest_error = f'Device id {device_id} already exists!'
            return -1

        device_data = {
            'device_id': device_id,
            'desc': desc,
            'type': type,
            'manufacturer': manufacturer
        }

        object_id = self._db.insert_single_data(DeviceModel.DEVICE_COLLECTION, device_data)
        return self.find_by_object_id(object_id)


class ReservoirDataModel:
    RESERVOIR_DATA_COLLECTION = 'reservoir_data'

    def __init__(self):
        self._db = Database()
        self._latest_error = ''
        
    @property
    def latest_error(self):
        return self._latest_error

    @latest_error.setter
    def latest_error(self, latest_error):
        self._latest_error = latest_error
    
    def find_by_device_id_and_timestamp(self, device_id, timestamp):
        key = {'device_id': device_id, 'timestamp': timestamp}
        result = self._db.get_single_data(ReservoirDataModel.RESERVOIR_DATA_COLLECTION, key)
        return self.__find(result)

    def find_by_object_id(self, obj_id):
        key = {'_id': ObjectId(obj_id)}
        return self.__find(key)
    
    def find_all(self):
        key = {}
        return self.__find_multiple(key)
    
    def aggregate(self, pipeline):
        documents = self._db.aggregate(ReservoirDataModel.RESERVOIR_DATA_COLLECTION, pipeline)
        return documents
    
    def __find(self, key):
        document = self._db.get_single_data(ReservoirDataModel.RESERVOIR_DATA_COLLECTION, key)
        return document
    
    def __find_multiple(self, key):
        documents = self._db.get_multiple_data(ReservoirDataModel.RESERVOIR_DATA_COLLECTION, key)
        return documents
    
    def insert(self, device_id, value, timestamp):
        self._latest_error = ''
        document = self.find_by_device_id_and_timestamp(device_id, timestamp)
        
        if (document):
            self.latest_error = f'Data for timestamp {timestamp} for device id {device_id} already exists'
            return -1
        
        reservoir_data = {'device_id': device_id, 'value': value, 'timestamp': timestamp}
        object_id = self._db.insert_single_data(ReservoirDataModel.RESERVOIR_DATA_COLLECTION, reservoir_data)
        return self.find_by_object_id(object_id)


class DailyReportModel:
    DAILY_REPORT_COLLECTION = 'daily_report'

    def __init__(self):
        self._db = Database()
        self._latest_error = ''
    
    @property
    def latest_error(self):
        return self._latest_error

    @latest_error.setter
    def latest_error(self, latest_error):
        self._latest_error = latest_error
    
    def find_by_device_id_and_date(self, device_id, date):
        key = {'device_id': device_id, 'date': date}
        result = self._db.get_single_data(DailyReportModel.DAILY_REPORT_COLLECTION, key)
        return self.__find(key)
    
    def find_by_device_id_and_date_range(self, device_id, from_date, to_date):
        key = {'device_id': device_id, '$and': [{'date': {'$gte': from_date}}, {'date': {'$lte': to_date}}]}
        return self.__find_multiple(key)

    def find_first_anomaly_by_date_range(self, device_ids, threshold, from_date, to_date):
        keyid_dateRangeWithThreshold = {"device_id": {"$in": device_ids}, "max_value": {"$gt": threshold},
                                         "date": {"$gte": from_date, "$lte": to_date}}
        return self.__find(keyid_dateRangeWithThreshold)

    def find_by_object_id(self, obj_id):
        key = {'_id': ObjectId(obj_id)}
        return self.__find(key)
    
    def __find(self, key):
        daily_report_documents = self._db.get_single_data(DailyReportModel.DAILY_REPORT_COLLECTION, key)
        return daily_report_documents

    def __find_multiple(self, key):
        daily_report_documents = self._db.get_multiple_data(DailyReportModel.DAILY_REPORT_COLLECTION, key)
        return daily_report_documents

    def aggregate(self, pipeline):
        documents = self._db.aggregate(DailyReportModel.DAILY_REPORT_COLLECTION, pipeline)
        return documents

    def insert(self, device_id, avg_value, min_value, max_value, date):
        self.latest_error = ''

        daily_report_document = self.find_by_device_id_and_date(device_id, date)
        if (daily_report_document):
            self.latest_error = f'Report for date {date} for device id {device_id} already exists'
            return -1
        
        daily_report_data = {
            'device_id': device_id, 
            'avg_value': avg_value, 
            'min_value': min_value, 
            'max_value': max_value, 
            'date': date
            }

        object_id = self._db.insert_single_data(DailyReportModel.DAILY_REPORT_COLLECTION, daily_report_data)
        
        return self.find_by_object_id(object_id)

    def insert_multiple(self, dr_docs):
        object_ids = self._db.insert_multiple_data(DailyReportModel.DAILY_REPORT_COLLECTION, dr_docs)
        
        return object_ids
