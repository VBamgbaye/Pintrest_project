from faker import Faker

fake = Faker()


def get_registered_user():
    return {
        "name": fake.name(),
        "address": fake.address(),
        "email": fake.email(),
        "text": fake.text(),
        "country": fake.country(),
        "created_at": fake.date()
    }


if __name__ == "__main__":
    print(get_registered_user())
