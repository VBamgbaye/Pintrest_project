import time
import pickle
import base64
import sys
import os
import json
from json import loads
from json import dumps
from project_pin_API import Data
from kafka import KafkaConsumer
import boto3


class BatchData:
    def __init__(self):
        self.api_data = None
        self.batch_consumer = KafkaConsumer(
            bootstrap_servers="localhost:9092",
            value_deserializer=lambda x: loads(x),
            auto_offset_reset="earliest",
            enable_auto_commit=True
        )
        self.batch_consumer.subscribe(topics=["Pinterestdata"])
        self.agg_message = []
        self.s3_client = boto3.client('s3')
        self.api_data = None

    def get_batch(self):
        for message in self.batch_consumer:
            self.agg_message.append(message)
            print(self.agg_message)
            # return self.agg_message

    def read_message(self):
        # if len(self.agg_message) == 0:
        #     self.get_batch()
        # if len(self.agg_message) > 0:
        #     return self.agg_message.pop(0)
        # else:
        #     print("11")
        return self.batch_consumer.next()

    #         api_event = message.value
    #         # print(type(api_event))
    #         # print(api_event)
    #         # api_event needs to be edited to replace '' with "" to allow it to be json readable in spark.
    #         # api_event_json_readble = api_event.replace("\'", "\"")
    #         # print(api_event_json_readble)
    #         #  api_event to json, send json to s3 & you need a counter; initialise a counter
    #         self.api_data = json.loads(api_event)
    #         # self.api_data = json.dumps(api_event_json_readble)
    #         # print(type(self.api_data))
    #         # print(self.api_data)
    #         # print(str(self.api_data))
    #         i = 0
    #         while os.path.exists(f'api_data{i}.json'):
    #             i += 1
    #         file = open(f'api_data{i}.json', 'w')
    #         file.write(str(self.api_data))
    #         file.close()
    #         if k == 100:
    #             break
    #         else:
    #             continue
    #     print('all done')
    #     # i = 0
    #     # while os.path.exists(f'api_data{i}.json'):
    #     #     i += 1
    #     # with open(f'api_data{i}.json', 'w') as file:
    #     #     file.write(str(self.api_data))
    #     #     file.close()
    #
    # def send_json_to_s3(self):
    #     i = 0
    #     # file_path = f'./'
    #     # for i in range(0, 100):
    #     while os.path.exists(f'api_data{i}.json'):
    #         i += 1
    #         self.s3_client.upload_file(f'api_data{i}.json', 'simeon-streaming-bucket', f'api_data{i}.json')
    #         print('message saved as json file and sent to s3')
    #         time.sleep(1)
    #         exit()
    #         # break
    #     # for p in range(1, 3):
    #     # os.remove(f'./api_data_file{p}')
    #     # print('all api_data file deleted')
    #     # for j, v in enumerate(self.message):
    #     #     self.output_dict[j] = v
    #     #     print(type(self.output_dict))
    #     # y = json.dumps(self.output_dict)
    #     # print(y)
    #     # out_file = open("api_data.json", "w") #encoding='utf-8')
    #     # json.dumps(dict(self.output_dict)) #cls=MyEncoder, indent=4)
    #     # #json.dumps(output_dict, default=lambda o: o._dict_, sort_keys=True, indent=4)
    #     # out_file.close()
    #     # self.s3_client.upload_file('./api_data.json', 'simeon-streaming-bucket', 'api_data')
    #
    # # out_file = open("api_data.json", "w", encoding='utf-8')
    # # json.dumps(self.output_dict, out_file, cls=MyEncoder, indent=4)
    # # #json.dumps(output_dict, default=lambda o: o._dict_, sort_keys=True, indent=4)
    # # out_file.close()
    # # self.s3_client.upload_file('./api_data.json', 'simeon-streaming-bucket', 'api_data')
    # # def delete_files(self):
    # #     for n in range(1, 100000):
    # #         os.remove(f'./api_data{n}.json')
    # #         print('all api_data files deleted')
    # def run(self):
    #     '''
    #     This function is used to run or execute all the methods.
    #     '''
    #     self.consume()
    #     # self.save_api_to_json()
    #     self.send_json_to_s3()


if __name__ == '__main__':
    BatchData.read_message()