import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

fake = Faker()


def random_date():
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2026, 1, 1)

    return start_date + timedelta(
        seconds=random.randint(
            0,
            int((end_date - start_date).total_seconds()),
        )
    )


def generate_data(n=1000):
    data = []

    for _ in range(n):
        data.append(
            {
                "name": fake.name(),
                "amount": round(random.uniform(10, 1000), 2),
                "event_date": random_date(),
                "is_usd": random.choice([True, False]),
            }
        )

    return pd.DataFrame(data)