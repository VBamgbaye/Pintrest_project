import os.path
from json import loads
from uuid import uuid4

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: loads(x),
    group_id='Pintrestdata_{}'.format(uuid4()),
    auto_offset_reset="earliest",
    # max_poll_records=5,
    enable_auto_commit=True
)
consumer.subscribe(topics=["Pinterest_data"])


def batch_consumer():
    for message in consumer:
        batch_message = message.value
        i = 0
        while os.path.exists(f'batch_data{i}.json'):
            i += 1
        file = open(f'batch_data{i}.json', 'w')
        file.write(str(batch_message).replace("'", '"'))
        file.close()


# def send_json_to_s3(self):
#     i = 0
#     while os.path.exists(f'batch_data{i}.json'):
#         i += 1
#         self.s3_client.upload_file(f'batch_data{i}.json', 'ai-core-bucket', f'batch_data{i}.json')
#         print('message saved as json file and sent to s3')
#         time.sleep(3)
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
    batch_consumer()
