import json
from json import loads
from uuid import uuid4

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: loads(x).decode('utf-8'),
    group_id='Pintrestdata_{}'.format(uuid4()),
    auto_offset_reset="earliest",
    # max_poll_records=5,
    enable_auto_commit=True
)
consumer.subscribe(topics=["Pintrestdata"])


def batch_consumer():
    for message in consumer:
        with open('./pin_data.json', 'w') as f:
            json.dumps(message.value, f, indent=4)



    # def clean_data(self):
    #     for item in self.bank_data:
    #         self.bank_data1.append(item[1])
    #
    # def write_to_json(self):
    #     with open('./world_bank.json', 'w') as f:
    #         json.dump(self.bank_data1, f, indent=4)


# def get_message(no_of_msg_to_consume, local_path):
#
#     messages = []
#     while len(messages) < no_of_msg_to_consume:
#         record = next(consumer)
#         line = (record.key, record.value)
#         print(line)
#         messages.append(line)
#
#     df = pd.DataFrame(messages)
#     table = pa.Table.from_pandas(df)
#     pq.write_table(table, local_path, compression='gzip')
#
#     return messages


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
    batch_consumer()
