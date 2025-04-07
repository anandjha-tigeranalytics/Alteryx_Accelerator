import csv
import os
import random
from mimesis import Generic
from datetime import datetime

def generate_customer_data(num_customers=1000, output_file="customers.csv", seed=None):
    """
    Generates dummy customer data and writes it to a CSV file.

    Args:
        num_customers (int): Number of customers to generate.
        output_file (str): Output CSV filename.
        seed (int, optional): Seed for reproducibility.
    """
    # Use reproducible randomness if seed is provided
    if seed is not None:
        random.seed(seed)

    # Check if file already exists
    if os.path.exists(output_file):
        print(f"⚠️ File '{output_file}' already exists. Overwriting...")

    generic = Generic()
    field_names = ["customer_id", "first_name", "last_name", "email", "address", "phone"]
    customers_data = []

    for customer_id in range(201, 201 + num_customers):
        first_name = generic.person.first_name()
        last_name = generic.person.last_name()
        email = generic.person.email()
        address = generic.address.address()
        phone = generic.person.telephone()
        customers_data.append([customer_id, first_name, last_name, email, address, phone])

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(field_names)     # Write header
        writer.writerows(customers_data) # Write data

    print(f"✅ Generated {num_customers} customers to '{output_file}' at {datetime.now()}")

# Only run if executed directly (not on import)
if __name__ == "__main__":
    generate_customer_data(num_customers=1000, output_file="customers_new.csv", seed=42)
