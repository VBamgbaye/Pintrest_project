from kafka import KafkaAdminClient
from kafka import KafkaClient
from kafka.admin import NewTopic
from kafka.cluster import ClusterMetadata


def retrieve_metadata():
    meta_cluster_conn = ClusterMetadata(
        bootstrap_servers="localhost:9092",
    )
    print(meta_cluster_conn.brokers())


def check_conn():
    # Create a connection to our KafkaBroker to check if it is running
    client_conn = KafkaClient(
        bootstrap_servers="localhost:9092",  # Specific the broker address to connect to
        client_id="Broker test"  # Create an id from this client for reference
    )
    # Check that the server is connected and running
    print(client_conn.bootstrap_connected())
    # Check our Kafka version number
    print(client_conn.check_version())


def admin():
    # Create a new Kafka client to administrate our Kafka broker
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092",
        client_id="Kafka Administrator"
    )
    # topics must be pass as a list to the create_topics method
    topics = [NewTopic(name="Pinterestdata", num_partitions=3, replication_factor=1)]

    # Topics to create must be passed as a list
    admin_client.create_topics(new_topics=topics)


def simulate():
    retrieve_metadata()
    check_conn()
    admin()


if __name__ == "__main__":
    simulate()
