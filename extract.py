import csv
import random
import string
from faker import Faker


LOCAL_FILE_PATH = "sales.csv"

def generate_sales_data():
    """
    Generates fake customer sales and complaint data and saves it to a local CSV file.
    This function can be used in Airflow or standalone.
    """
    num_customers = 1000
    fake = Faker()
    
    print(f"Generating {num_customers} records...")

    with open(LOCAL_FILE_PATH, mode='w', newline='') as file:
        fieldnames = [
            'customer_id', 'first_name', 'last_name', 'item_name', 
            'purchase_date', 'email', 'address', 'phone_number', 'complaint'
        ]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for _ in range(num_customers):
            writer.writerow({
                "customer_id": fake.random_int(min=100000, max=999999),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "item_name": fake.word(),
                "purchase_date": fake.date(),
                "email": fake.email(),
                "address": fake.city(),
                "phone_number": fake.phone_number(),
                "complaint": fake.sentence()
            })

    print(f"Successfully generated data and saved to {LOCAL_FILE_PATH}")


if __name__ == "__main__":
    generate_sales_data()
