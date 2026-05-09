"""
Generate a 50,000-row realistic business dataset for Data Mining Pro testing.
Covers all 4 modules: AI Analyst, Smart ETL, BI Reporter, SQL Analytics
"""
# importing 
import csv
import random
import math
from datetime import datetime, timedelta

random.seed(42)

# Configuration
NUM_ROWS = 50000
OUTPUT_FILE = "test_dataset_50k.csv"

# Realistic data pools
CATEGORIES = [
    "Electronics", "Clothing", "Food & Beverages", "Home & Garden",
    "Sports & Outdoors", "Books & Media", "Health & Beauty",
    "Automotive", "Toys & Games", "Office Supplies"
]

PRODUCTS = {
    "Electronics": ["Laptop", "Smartphone", "Headphones", "Tablet", "Smart Watch", "Monitor", "Keyboard", "Mouse", "Speaker", "Camera"],
    "Clothing": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Dress", "Hoodie", "Shorts", "Blazer", "Boots", "Scarf"],
    "Food & Beverages": ["Coffee", "Juice", "Snack Pack", "Cereal", "Protein Bar", "Tea", "Soda", "Chocolate", "Cookies", "Energy Drink"],
    "Home & Garden": ["Lamp", "Rug", "Curtains", "Plant Pot", "Pillow", "Candle", "Clock", "Mirror", "Shelf", "Vase"],
    "Sports & Outdoors": ["Yoga Mat", "Dumbbell", "Football", "Cricket Bat", "Running Shoes", "Gym Bag", "Water Bottle", "Bicycle", "Tent", "Helmet"],
    "Books & Media": ["Novel", "Textbook", "Comic", "Magazine", "Audiobook", "DVD", "Vinyl Record", "E-book", "Journal", "Calendar"],
    "Health & Beauty": ["Face Cream", "Shampoo", "Sunscreen", "Perfume", "Toothpaste", "Vitamin", "Hair Oil", "Soap", "Lipstick", "Moisturizer"],
    "Automotive": ["Car Cover", "Dash Cam", "Air Freshener", "Phone Mount", "Seat Cover", "Tire Inflator", "Wiper Blades", "LED Lights", "Oil Filter", "Car Wash Kit"],
    "Toys & Games": ["Board Game", "Puzzle", "Action Figure", "LEGO Set", "RC Car", "Doll", "Card Game", "Drone", "Stuffed Animal", "Art Kit"],
    "Office Supplies": ["Notebook", "Pen Set", "Stapler", "Desk Organizer", "Whiteboard", "Paper Ream", "Tape", "Scissors", "File Folder", "Sticky Notes"]
}

REGIONS = ["North", "South", "East", "West", "Central"]
CITIES = {
    "North": ["Delhi", "Chandigarh", "Jaipur", "Lucknow", "Amritsar"],
    "South": ["Bangalore", "Chennai", "Hyderabad", "Kochi", "Coimbatore"],
    "East": ["Kolkata", "Bhubaneswar", "Patna", "Guwahati", "Ranchi"],
    "West": ["Mumbai", "Pune", "Ahmedabad", "Surat", "Goa"],
    "Central": ["Bhopal", "Indore", "Nagpur", "Raipur", "Jabalpur"]
}

PAYMENT_METHODS = ["Credit Card", "Debit Card", "UPI", "Net Banking", "Cash on Delivery", "Wallet"]
CUSTOMER_SEGMENTS = ["Premium", "Regular", "Budget", "Enterprise", "Wholesale"]
SHIPPING_TYPES = ["Standard", "Express", "Same Day", "Economy", "Priority"]
GENDERS = ["Male", "Female", "Other"]

# Price ranges per category
PRICE_RANGES = {
    "Electronics": (999, 89999),
    "Clothing": (299, 7999),
    "Food & Beverages": (49, 999),
    "Home & Garden": (199, 14999),
    "Sports & Outdoors": (199, 24999),
    "Books & Media": (99, 2999),
    "Health & Beauty": (99, 4999),
    "Automotive": (149, 9999),
    "Toys & Games": (149, 9999),
    "Office Supplies": (29, 2999)
}

# Start & end dates
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2025, 12, 31)
TOTAL_DAYS = (END_DATE - START_DATE).days


def random_date():
    """Generate a random date with seasonal bias"""
    day_offset = random.randint(0, TOTAL_DAYS)
    d = START_DATE + timedelta(days=day_offset)
    # Boost probability for Q4 (Oct-Dec) to simulate holiday season sales
    if d.month in [10, 11, 12] and random.random() < 0.3:
        day_offset = random.randint(0, TOTAL_DAYS)
        d = START_DATE + timedelta(days=day_offset)
    return d


def generate_row(row_id):
    """Generate a single data row"""
    category = random.choice(CATEGORIES)
    product = random.choice(PRODUCTS[category])
    region = random.choice(REGIONS)
    city = random.choice(CITIES[region])
    
    # Date
    date = random_date()
    
    # Price with some variance
    base_min, base_max = PRICE_RANGES[category]
    unit_price = round(random.uniform(base_min, base_max), 2)
    
    # Quantity
    quantity = random.randint(1, 20)
    
    # Revenue & Cost
    revenue = round(unit_price * quantity, 2)
    cost_ratio = random.uniform(0.35, 0.75)
    cost = round(revenue * cost_ratio, 2)
    
    # Discount
    discount_pct = random.choice([0, 0, 0, 5, 10, 15, 20, 25, 30])
    discount_amount = round(revenue * discount_pct / 100, 2)
    final_revenue = round(revenue - discount_amount, 2)
    
    # Customer
    customer_id = f"CUST-{random.randint(1000, 9999)}"
    customer_age = random.randint(18, 70)
    gender = random.choice(GENDERS)
    segment = random.choice(CUSTOMER_SEGMENTS)
    
    # Order
    payment = random.choice(PAYMENT_METHODS)
    shipping = random.choice(SHIPPING_TYPES)
    rating = round(random.uniform(1.0, 5.0), 1)
    
    # Satisfaction (correlated with rating)
    if rating >= 4.0:
        satisfaction = random.choice(["Very Satisfied", "Satisfied"])
    elif rating >= 3.0:
        satisfaction = random.choice(["Satisfied", "Neutral"])
    elif rating >= 2.0:
        satisfaction = random.choice(["Neutral", "Dissatisfied"])
    else:
        satisfaction = random.choice(["Dissatisfied", "Very Dissatisfied"])
    
    # Return flag (higher for low ratings)
    return_prob = max(0.02, 0.30 - rating * 0.05)
    returned = "Yes" if random.random() < return_prob else "No"
    
    # Shipping cost
    if shipping == "Same Day":
        shipping_cost = round(random.uniform(150, 500), 2)
    elif shipping == "Express":
        shipping_cost = round(random.uniform(80, 250), 2)
    elif shipping == "Priority":
        shipping_cost = round(random.uniform(60, 200), 2)
    elif shipping == "Standard":
        shipping_cost = round(random.uniform(30, 100), 2)
    else:
        shipping_cost = round(random.uniform(10, 50), 2)
    
    # Introduce some missing values (about 2% per row) for data cleaning testing
    row = [
        row_id,
        date.strftime("%Y-%m-%d"),
        date.strftime("%B"),
        date.year,
        category,
        product,
        region,
        city,
        customer_id,
        customer_age,
        gender,
        segment,
        quantity,
        unit_price,
        revenue,
        discount_pct,
        discount_amount,
        final_revenue,
        cost,
        round(final_revenue - cost, 2),  # Profit
        payment,
        shipping,
        shipping_cost,
        rating,
        satisfaction,
        returned
    ]
    
    # Randomly introduce missing values (~2% chance per cell for numeric/text fields)
    nullable_indices = [9, 13, 14, 16, 17, 18, 19, 22, 23]  # age, price, revenue, etc.
    for idx in nullable_indices:
        if random.random() < 0.02:
            row[idx] = ""
    
    return row


def main():
    headers = [
        "Order_ID",
        "Date",
        "Month",
        "Year",
        "Category",
        "Product",
        "Region",
        "City",
        "Customer_ID",
        "Customer_Age",
        "Gender",
        "Segment",
        "Quantity",
        "Unit_Price",
        "Revenue",
        "Discount_Pct",
        "Discount_Amount",
        "Sales_Amount",
        "Cost",
        "Profit",
        "Payment_Method",
        "Shipping_Type",
        "Shipping_Cost",
        "Rating",
        "Satisfaction",
        "Returned"
    ]
    
    print(f"Generating {NUM_ROWS:,} rows...")
    
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        
        for i in range(1, NUM_ROWS + 1):
            row = generate_row(i)
            writer.writerow(row)
            
            if i % 10000 == 0:
                print(f"  >> {i:,} / {NUM_ROWS:,} rows generated...")
    
    print(f"\n[DONE] File saved as: {OUTPUT_FILE}")
    print(f"   Rows: {NUM_ROWS:,} + 1 header = {NUM_ROWS + 1:,} lines")
    
    # Show file size
    import os
    size_bytes = os.path.getsize(OUTPUT_FILE)
    if size_bytes > 1024 * 1024:
        print(f"   Size: {size_bytes / (1024*1024):.1f} MB")
    else:
        print(f"   Size: {size_bytes / 1024:.1f} KB")
    
    # Preview first 3 rows
    print(f"\n[PREVIEW] First 3 rows:")
    print(f"   Columns: {', '.join(headers)}")
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for i, row in enumerate(reader):
            if i >= 3:
                break
            print(f"   Row {i+1}: {row[:8]}...")


if __name__ == "__main__":
    main()
