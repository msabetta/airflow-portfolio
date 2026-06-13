import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

class EcommerceDataGenerator:
    def __init__(self, output_dir: str):
        self.fake = Faker()
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def generate_users(self, num_users=1000):
        users = []
        for _ in range(num_users):
            users.append({
                'user_id': self.fake.uuid4(),
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'email': self.fake.email(),
                'country': self.fake.country(),
                'registration_date': self.fake.date_between(start_date='-2y', end_date='today').isoformat()
            })
        df = pd.DataFrame(users)
        df.to_csv(os.path.join(self.output_dir, 'users.csv'), index=False)
        return df

    def generate_products(self, num_products=100):
        categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Toys', 'Sports']
        products = []
        for _ in range(num_products):
            products.append({
                'product_id': self.fake.uuid4(),
                'product_name': self.fake.word().capitalize() + ' ' + self.fake.word().capitalize(),
                'category': random.choice(categories),
                'price': round(random.uniform(5.0, 500.0), 2)
            })
        df = pd.DataFrame(products)
        df.to_csv(os.path.join(self.output_dir, 'products.csv'), index=False)
        return df

    def generate_daily_orders(self, users_df, products_df, date=None, num_orders=50):
        if date is None:
            date = datetime.now() - timedelta(days=1)
        
        orders = []
        date_str = date.strftime('%Y-%m-%d')
        
        for _ in range(num_orders):
            user = users_df.sample().iloc[0]
            num_items = random.randint(1, 5)
            
            for _ in range(num_items):
                product = products_df.sample().iloc[0]
                order_id = self.fake.uuid4()
                quantity = random.randint(1, 3)
                
                # Simulating a timestamp within the specified date
                hour = random.randint(0, 23)
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                order_timestamp = f"{date_str} {hour:02d}:{minute:02d}:{second:02d}"
                
                orders.append({
                    'order_id': order_id,
                    'user_id': user['user_id'],
                    'product_id': product['product_id'],
                    'quantity': quantity,
                    'unit_price': product['price'],
                    'order_timestamp': order_timestamp,
                    'status': random.choices(['completed', 'pending', 'cancelled'], weights=[80, 15, 5])[0]
                })
                
        df = pd.DataFrame(orders)
        output_file = os.path.join(self.output_dir, f'orders_{date_str}.csv')
        df.to_csv(output_file, index=False)
        return output_file
