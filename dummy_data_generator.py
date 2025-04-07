import csv
import os
import random
from mimesis import Generic
from datetime import date, timedelta,datetime

## generating datetime.csv records
field_names = ["date", "day_of_week", "month", "quarter", "year", "holidays"]
num_times = 1000
start_date = date(2023, 1, 1)
times_data = [field_names]

for i in range(num_times):
    current_date = start_date + timedelta(days=i)
    day_of_week = current_date.strftime("%A")
    month = current_date.strftime("%B")
    quarter = f"Q{((current_date.month - 1) // 3) + 1}"
    year = current_date.year
    holidays = ""

    # You can add holiday logic here if needed

    times_data.append([current_date, day_of_week, month, quarter, year, holidays])

with open("time.csv", "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(times_data)

## generating stores.csv records
field_names = ["store_id", "store_name", "location", "manager", "square_footage"]
num_stores = 1000
stores_data = [field_names]

for store_id in range(301, 301 + num_stores):
    store_name = f"Store {store_id}"
    location = f"Location {store_id}"
    manager = f"Manager {store_id}"
    square_footage = random.randint(2000, 10000)
    stores_data.append([store_id, store_name, location, manager, square_footage])

with open("stores.csv", "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(stores_data)


## generating customers.csv records
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


## generating products.csv

# Define the field names for the CSV file
field_names = ["product_id", "product_name", "category", "manufacturer", "description", "price"]

# Function to generate a random product entry
def generate_product(product_id):
    categories = ["Electronics", "Home & Garden", "Clothing", "Toys", "Sports & Outdoors"]
    manufacturers = ["ABC Corp", "XYZ Inc", "DEF Ltd", "GHI Industries", "LMN Enterprises"]

    product_name = f"Product {product_id}"
    category = random.choice(categories)
    manufacturer = random.choice(manufacturers)
    description = f"Description of {product_name}"
    price = round(random.uniform(10.0, 500.0), 2)

    return [product_id, product_name, category, manufacturer, description, price]

# Generate 1000 product entries

num_products = 1000
products_data = [field_names]

for product_id in range(101, 101 + num_products):
    product_entry = generate_product(product_id)
    products_data.append(product_entry)

# Write the data to a CSV file
with open("products.csv", "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(products_data)

print("Product data generated and saved to 'products.csv'.")

## generating sales_fact.csv records

field_names = ["sales_id", "date", "product_id", "customer_id", "store_id", "sales_amount", "quantity_sold", "discount"]
num_sales = 1000
sales_data = [field_names]

for sales_id in range(1001, 1001 + num_sales):
    date = f"2023-10-{random.randint(1, 31)}"
    product_id = random.randint(101, 1100)
    customer_id = random.randint(201, 1200)
    store_id = random.randint(301, 1300)
    sales_amount = round(random.uniform(10.0, 500.0), 2)
    quantity_sold = random.randint(1, 10)
    discount = round(random.uniform(0.0, 0.3), 2)
    
    sales_data.append([sales_id, date, product_id, customer_id, store_id, sales_amount, quantity_sold, discount])

with open("sales.csv", "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(sales_data)
