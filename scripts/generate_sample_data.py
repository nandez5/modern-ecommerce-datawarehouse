import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
import uuid
from faker import Faker
import os
from typing import Dict, List, Optional, Tuple

# Initialize Faker for realistic data generation
fake = Faker('en_US')

class EcommerceDataGenerator:
    """
    Advanced E-commerce Data Generator for realistic multi-source data warehouse.
    
    Generates comprehensive dataset including:
    - Customer demographics with segmentation
    - Product catalog with hierarchical categories
    - Order transactions with realistic patterns
    - Web analytics sessions
    - Marketing spend data
    - Inventory movements
    """
    
    def __init__(self, base_date: str = '2023-01-01', num_customers: int = 10000):
        """
        Initialize the data generator with configurable parameters.
        
        Args:
            base_date: Starting date for data generation
            num_customers: Number of unique customers to generate
        """
        self.base_date = datetime.strptime(base_date, '%Y-%m-%d').date()
        self.end_date = datetime.now().date()
        self.num_customers = num_customers
        
        # Initialize DataFrames
        self.customers_df = None
        self.products_df = None
        self.orders_df = None
        self.order_items_df = None
        self.web_sessions_df = None
        self.marketing_spend_df = None
        
        # Business configuration - realistic e-commerce setup
        self.product_categories = {
            'Electronics': ['Smartphones', 'Laptops', 'Audio', 'Gaming', 'Wearables'],
            'Clothing': ['Mens', 'Womens', 'Kids', 'Shoes', 'Accessories'],
            'Home & Garden': ['Furniture', 'Appliances', 'Decor', 'Tools', 'Garden'],
            'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Comics', 'E-books'],
            'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Individual Sports', 'Equipment'],
            'Beauty & Health': ['Skincare', 'Makeup', 'Supplements', 'Personal Care', 'Medical'],
            'Toys & Games': ['Educational', 'Action Figures', 'Board Games', 'Video Games', 'Puzzles'],
            'Automotive': ['Parts', 'Accessories', 'Tools', 'Maintenance', 'Electronics'],
            'Food & Beverages': ['Snacks', 'Beverages', 'Organic', 'International', 'Frozen']
        }
        
        self.marketing_channels = [
            'Google Ads', 'Facebook Ads', 'Instagram Ads', 'TikTok Ads',
            'Email Marketing', 'SEO Organic', 'Influencer Marketing', 
            'TV Advertising', 'Radio', 'Affiliate Marketing', 'Direct Mail'
        ]
        
        self.payment_methods = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay', 'bank_transfer']
        self.order_statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled', 'returned']
        self.device_types = ['desktop', 'mobile', 'tablet']
        self.traffic_sources = ['organic', 'paid_search', 'social_media', 'direct', 'referral', 'email']
        
        print(f"🏗️  Data Generator initialized for {num_customers:,} customers")
        print(f"📅 Date range: {self.base_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")

    def generate_customers(self) -> pd.DataFrame:
        """
        Generate customer dimension with realistic segmentation and demographics.
        
        Implements customer lifecycle stages and behavioral segments:
        - VIP: High-value customers (10%)
        - Regular: Standard customers (60%) 
        - New: Recent acquisitions (30%)
        
        Returns:
            DataFrame: Customer dimension table
        """
        print("👥 Generating customer dimension...")
        
        customers = []
        
        for i in range(self.num_customers):
            # Customer segmentation with realistic distribution
            segment_rand = random.random()
            if segment_rand < 0.10:
                segment = 'VIP'
                avg_order_value_range = (300, 1500)
                churn_probability = 0.05
            elif segment_rand < 0.70:
                segment = 'Regular'
                avg_order_value_range = (75, 400)
                churn_probability = 0.15
            else:
                segment = 'New'
                avg_order_value_range = (40, 200)
                churn_probability = 0.25
            
            # Realistic customer lifecycle dates
            days_since_base = (self.end_date - self.base_date).days
            created_days_ago = random.randint(30, days_since_base)
            created_at = self.base_date + timedelta(days=random.randint(0, days_since_base - 30))
            
            # Last activity based on segment (VIP more active)
            if segment == 'VIP':
                last_activity = created_at + timedelta(days=random.randint(0, 30))
            elif segment == 'Regular':
                last_activity = created_at + timedelta(days=random.randint(0, 90))
            else:
                last_activity = created_at + timedelta(days=random.randint(0, 180))
            
            customer = {
                'customer_id': f'CUST_{i+1:08d}',
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'email': fake.email(),
                'phone': fake.phone_number(),
                'birth_date': fake.date_of_birth(minimum_age=18, maximum_age=75),
                'gender': random.choice(['Male', 'Female', 'Other']),
                'address_line1': fake.street_address(),
                'city': fake.city(),
                'state': fake.state_abbr(),
                'postal_code': fake.zipcode(),
                'country': random.choice(['Portugal', 'Spain', 'France', 'Germany', 'Italy', 'United Kingdom']),
                'customer_segment': segment,
                'acquisition_channel': random.choice(self.marketing_channels),
                'lifetime_value': round(random.uniform(*avg_order_value_range) * random.randint(1, 15), 2),
                'created_at': created_at,
                'updated_at': last_activity,
                'last_order_date': last_activity if random.random() > churn_probability else None,
                'is_active': random.choices([True, False], weights=[0.85, 0.15])[0],
                'email_subscribed': random.choices([True, False], weights=[0.70, 0.30])[0],
                'preferred_contact': random.choice(['email', 'phone', 'sms']),
                'credit_score_range': random.choice(['Excellent', 'Good', 'Fair', 'Poor'])
            }
            customers.append(customer)
        
        self.customers_df = pd.DataFrame(customers)
        print(f"✅ Generated {len(customers):,} customers")
        print(f"   - VIP: {len(self.customers_df[self.customers_df.customer_segment == 'VIP']):,}")
        print(f"   - Regular: {len(self.customers_df[self.customers_df.customer_segment == 'Regular']):,}")
        print(f"   - New: {len(self.customers_df[self.customers_df.customer_segment == 'New']):,}")
        
        return self.customers_df

    def generate_products(self, num_products: int = 5000) -> pd.DataFrame:
        """
        Generate product catalog with hierarchical categories and realistic pricing.
        
        Features:
        - Multi-level category hierarchy
        - Realistic pricing by category
        - Inventory management fields
        - Product performance metrics
        
        Args:
            num_products: Number of products to generate
            
        Returns:
            DataFrame: Product dimension table
        """
        print("📦 Generating product catalog...")
        
        products = []
        
        for i in range(num_products):
            # Select category and subcategory
            category = random.choice(list(self.product_categories.keys()))
            subcategory = random.choice(self.product_categories[category])
            
            # Realistic pricing by category
            price_ranges = {
                'Electronics': (50, 2500),
                'Clothing': (15, 400),
                'Home & Garden': (25, 800),
                'Books': (10, 60),
                'Sports': (20, 500),
                'Beauty & Health': (8, 150),
                'Toys & Games': (15, 200),
                'Automotive': (30, 1200),
                'Food & Beverages': (3, 80)
            }
            
            min_price, max_price = price_ranges.get(category, (10, 100))
            retail_price = round(random.uniform(min_price, max_price), 2)
            
            # Cost calculation with realistic margins by category
            margin_ranges = {
                'Electronics': (0.15, 0.35),
                'Clothing': (0.40, 0.65),
                'Home & Garden': (0.25, 0.45),
                'Books': (0.20, 0.40),
                'Sports': (0.30, 0.50),
                'Beauty & Health': (0.45, 0.70),
                'Toys & Games': (0.35, 0.55),
                'Automotive': (0.20, 0.40),
                'Food & Beverages': (0.25, 0.45)
            }
            
            margin_min, margin_max = margin_ranges.get(category, (0.25, 0.45))
            margin = random.uniform(margin_min, margin_max)
            cost = round(retail_price * (1 - margin), 2)
            
            # Product lifecycle stage
            lifecycle_stage = random.choices(
                ['New', 'Growth', 'Mature', 'Decline'],
                weights=[0.15, 0.25, 0.45, 0.15]
            )[0]
            
            # Inventory levels based on lifecycle
            stock_ranges = {
                'New': (10, 100),
                'Growth': (50, 500),
                'Mature': (100, 1000),
                'Decline': (5, 50)
            }
            stock_min, stock_max = stock_ranges[lifecycle_stage]
            
            product = {
                'product_id': f'PROD_{i+1:08d}',
                'sku': f'SKU-{fake.bothify(text="??##-####")}',
                'product_name': f'{fake.catch_phrase()} {subcategory}',
                'brand': fake.company(),
                'category_l1': category,
                'category_l2': subcategory,
                'retail_price': retail_price,
                'cost': cost,
                'margin_percent': round(margin * 100, 2),
                'weight_kg': round(random.uniform(0.1, 25), 2),
                'dimensions_cm': f'{random.randint(5,60)}x{random.randint(5,60)}x{random.randint(2,40)}',
                'color': fake.safe_color_name() if category in ['Clothing', 'Home & Garden'] else None,
                'size': random.choice(['XS', 'S', 'M', 'L', 'XL', 'XXL']) if category == 'Clothing' else None,
                'stock_quantity': random.randint(stock_min, stock_max),
                'reorder_point': random.randint(10, 50),
                'supplier': fake.company(),
                'lifecycle_stage': lifecycle_stage,
                'is_active': random.choices([True, False], weights=[0.90, 0.10])[0],
                'is_featured': random.choices([True, False], weights=[0.20, 0.80])[0],
                'created_at': fake.date_between(
                    start_date=self.base_date - timedelta(days=1095),  # 3 years ago
                    end_date=self.base_date
                ),
                'avg_rating': round(random.uniform(3.2, 4.8), 1),
                'total_reviews': random.randint(0, 500),
                'total_sales': random.randint(0, 10000)
            }
            products.append(product)
        
        self.products_df = pd.DataFrame(products)
        print(f"✅ Generated {len(products):,} products")
        
        # Show category distribution
        category_counts = self.products_df['category_l1'].value_counts()
        print("   Category distribution:")
        for cat, count in category_counts.head().items():
            print(f"   - {cat}: {count:,}")
        
        return self.products_df

    def generate_orders(self, num_orders: int = 25000) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate realistic order transactions with seasonal patterns and customer behavior.
        
        Features:
        - Seasonal sales patterns
        - Customer segment-based order behavior
        - Realistic order statuses and fulfillment times
        - Multi-item orders with cross-selling patterns
        
        Args:
            num_orders: Number of orders to generate
            
        Returns:
            Tuple[DataFrame, DataFrame]: Orders and order items tables
        """
        if self.customers_df is None or self.products_df is None:
            raise ValueError("Must generate customers and products first!")
        
        print("🛒 Generating order transactions...")
        
        orders = []
        order_items = []
        
        # Get active customers and products for realistic constraints
        active_customers = self.customers_df[self.customers_df['is_active'] == True]['customer_id'].tolist()
        active_products = self.products_df[self.products_df['is_active'] == True]
        
        for i in range(num_orders):
            order_id = f'ORD_{i+1:10d}'
            
            # Select customer with segment-based probability
            customer_id = random.choice(active_customers)
            customer_info = self.customers_df[self.customers_df['customer_id'] == customer_id].iloc[0]
            
            # Order timing with realistic patterns
            # Peak seasons: November-December, Summer months
            order_date = fake.date_between(start_date=self.base_date, end_date=self.end_date)
            month = order_date.month
            
            # Seasonal multiplier for order probability
            seasonal_multiplier = 1.0
            if month in [11, 12]:  # Holiday season
                seasonal_multiplier = 1.8
            elif month in [6, 7, 8]:  # Summer
                seasonal_multiplier = 1.3
            elif month in [1, 2]:  # Post-holiday dip
                seasonal_multiplier = 0.7
            
            # Number of items per order based on customer segment
            if customer_info['customer_segment'] == 'VIP':
                num_items = random.choices([1, 2, 3, 4, 5], weights=[0.2, 0.3, 0.25, 0.15, 0.1])[0]
                discount_probability = 0.4
            elif customer_info['customer_segment'] == 'Regular':
                num_items = random.choices([1, 2, 3, 4], weights=[0.4, 0.35, 0.2, 0.05])[0]
                discount_probability = 0.2
            else:  # New customers
                num_items = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0]
                discount_probability = 0.3  # First-time buyer discount
            
            # Select products for this order
            selected_products = active_products.sample(n=num_items).to_dict('records')
            
            # Calculate order totals
            subtotal = 0
            total_items = 0
            
            for j, product in enumerate(selected_products):
                quantity = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
                unit_price = product['retail_price']
                line_total = unit_price * quantity
                
                order_item = {
                    'order_item_id': f'ITEM_{i+1:10d}_{j+1:02d}',
                    'order_id': order_id,
                    'product_id': product['product_id'],
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'line_total': line_total,
                    'cost_per_unit': product['cost'],
                    'line_cost': product['cost'] * quantity
                }
                order_items.append(order_item)
                
                subtotal += line_total
                total_items += quantity
            
            # Apply discounts
            discount_amount = 0
            if random.random() < discount_probability:
                discount_percent = random.uniform(0.05, 0.25)
                discount_amount = round(subtotal * discount_percent, 2)
            
            # Shipping calculation (European shipping costs)
            if subtotal > 50:  # Free shipping threshold for EU
                shipping_cost = 0
            else:
                shipping_cost = round(random.uniform(3.99, 9.99), 2)  # EU shipping rates
            
            # Tax calculation (European VAT rates)
            # Portugal VAT is 23%, but we'll use mixed EU rates for realism
            tax_rates = {
                'Portugal': 0.23,
                'Spain': 0.21, 
                'France': 0.20,
                'Germany': 0.19,
                'Italy': 0.22,
                'United Kingdom': 0.20
            }
            
            # Get customer's country for tax calculation
            customer_country = customer_info.get('country', 'Portugal')
            tax_rate = tax_rates.get(customer_country, 0.23)  # Default to Portugal rate
            tax_amount = round((subtotal - discount_amount) * tax_rate, 2)
            
            total_amount = subtotal - discount_amount + shipping_cost + tax_amount
            
            # Order status based on order date
            days_since_order = (datetime.now().date() - order_date).days
            if days_since_order < 1:
                status = 'pending'
            elif days_since_order < 3:
                status = random.choice(['pending', 'processing'])
            elif days_since_order < 7:
                status = random.choice(['processing', 'shipped'])
            else:
                status = random.choices(
                    ['delivered', 'cancelled', 'returned'],
                    weights=[0.85, 0.10, 0.05]
                )[0]
            
            order = {
                'order_id': order_id,
                'customer_id': customer_id,
                'order_date': order_date,
                'order_status': status,
                'payment_method': random.choice(self.payment_methods),
                'total_items': total_items,
                'subtotal': round(subtotal, 2),
                'discount_amount': discount_amount,
                'tax_amount': tax_amount,
                'shipping_cost': shipping_cost,
                'total_amount': round(total_amount, 2),
                'currency': 'EUR',
                'acquisition_channel': customer_info['acquisition_channel'],
                'device_type': random.choice(self.device_types),
                'is_first_order': random.choices([True, False], weights=[0.3, 0.7])[0],
                'created_at': datetime.combine(order_date, fake.time_object()),
                'updated_at': datetime.combine(order_date, fake.time_object()) + timedelta(hours=random.randint(1, 72))
            }
            orders.append(order)
        
        self.orders_df = pd.DataFrame(orders)
        self.order_items_df = pd.DataFrame(order_items)
        
        print(f"✅ Generated {len(orders):,} orders with {len(order_items):,} line items")
        print(f"   Average order value: ${self.orders_df['total_amount'].mean():.2f}")
        print(f"   Average items per order: {self.orders_df['total_items'].mean():.1f}")
        
        # Show status distribution
        status_counts = self.orders_df['order_status'].value_counts()
        print("   Order status distribution:")
        for status, count in status_counts.items():
            print(f"   - {status}: {count:,} ({count/len(orders)*100:.1f}%)")
        
        return self.orders_df, self.order_items_df

    def generate_web_sessions(self, num_sessions: int = 50000) -> pd.DataFrame:
        """
        Generate web analytics sessions with realistic user behavior patterns.
        
        Features:
        - Realistic session durations and page views
        - Conversion tracking
        - Traffic source attribution
        - Device and browser information
        - Bounce rate patterns
        
        Args:
            num_sessions: Number of web sessions to generate
            
        Returns:
            DataFrame: Web sessions fact table
        """
        print("🌐 Generating web analytics sessions...")
        
        sessions = []
        browsers = ['Chrome', 'Safari', 'Firefox', 'Edge', 'Opera']
        os_list = ['Windows', 'macOS', 'iOS', 'Android', 'Linux']
        
        # Get customer IDs for attribution (some sessions won't have customer_id - anonymous)
        customer_ids = self.customers_df['customer_id'].tolist() if self.customers_df is not None else []
        
        for i in range(num_sessions):
            session_date = fake.date_between(start_date=self.base_date, end_date=self.end_date)
            traffic_source = random.choice(self.traffic_sources)
            device_type = random.choice(self.device_types)
            
            # Device-specific behavior patterns
            if device_type == 'mobile':
                avg_session_duration = random.uniform(45, 180)  # Shorter on mobile
                page_views = random.choices([1, 2, 3, 4, 5], weights=[0.4, 0.25, 0.2, 0.1, 0.05])[0]
                bounce_rate = 0.65
            elif device_type == 'tablet':
                avg_session_duration = random.uniform(120, 300)
                page_views = random.choices([1, 2, 3, 4, 5, 6], weights=[0.3, 0.25, 0.2, 0.15, 0.08, 0.02])[0]
                bounce_rate = 0.55
            else:  # desktop
                avg_session_duration = random.uniform(180, 600)
                page_views = random.choices([1, 2, 3, 4, 5, 6, 7], weights=[0.25, 0.2, 0.2, 0.15, 0.1, 0.07, 0.03])[0]
                bounce_rate = 0.45
            
            # Traffic source affects conversion
            conversion_rates = {
                'organic': 0.08,
                'paid_search': 0.12,
                'social_media': 0.04,
                'direct': 0.15,
                'referral': 0.06,
                'email': 0.18
            }
            
            converted = random.random() < conversion_rates.get(traffic_source, 0.08)
            is_bounce = random.random() < bounce_rate and page_views == 1
            
            # Customer attribution (70% of sessions have known customer)
            customer_id = random.choice(customer_ids) if customer_ids and random.random() < 0.7 else None
            
            session = {
                'session_id': f'SESS_{i+1:10d}',
                'customer_id': customer_id,
                'session_date': session_date,
                'session_start_time': fake.date_time_between(
                    start_date=datetime.combine(session_date, datetime.min.time()),
                    end_date=datetime.combine(session_date, datetime.max.time())
                ),
                'session_duration_seconds': int(avg_session_duration),
                'page_views': page_views,
                'unique_page_views': min(page_views, random.randint(1, page_views)),
                'bounce': is_bounce,
                'converted': converted,
                'device_type': device_type,
                'browser': random.choice(browsers),
                'operating_system': random.choice(os_list),
                'traffic_source': traffic_source,
                'landing_page': f'/{fake.word()}' if traffic_source != 'direct' else '/',
                'exit_page': f'/{fake.word()}',
                'country': fake.country_code(),
                'city': fake.city(),
                'ip_address': fake.ipv4(),
                'user_agent': fake.user_agent()
            }
            sessions.append(session)
        
        self.web_sessions_df = pd.DataFrame(sessions)
        
        print(f"✅ Generated {len(sessions):,} web sessions")
        print(f"   Average session duration: {self.web_sessions_df['session_duration_seconds'].mean():.0f} seconds")
        print(f"   Overall conversion rate: {self.web_sessions_df['converted'].mean()*100:.2f}%")
        print(f"   Bounce rate: {self.web_sessions_df['bounce'].mean()*100:.2f}%")
        
        return self.web_sessions_df

    def generate_marketing_spend(self) -> pd.DataFrame:
        """
        Generate marketing spend data with realistic campaign patterns.
        
        Features:
        - Channel-specific spend patterns
        - Seasonal variations
        - Performance metrics (impressions, clicks, conversions)
        - Cost models (CPC, CPM, CPA)
        
        Returns:
            DataFrame: Marketing spend fact table
        """
        print("💰 Generating marketing spend data...")
        
        marketing_data = []
        
        # Generate daily marketing spend for each channel
        current_date = self.base_date
        while current_date <= self.end_date:
            
            for channel in self.marketing_channels:
                # Channel-specific budget patterns
                base_daily_budgets = {
                    'Google Ads': random.uniform(500, 2000),
                    'Facebook Ads': random.uniform(300, 1500),
                    'Instagram Ads': random.uniform(200, 1000),
                    'TikTok Ads': random.uniform(100, 800),
                    'Email Marketing': random.uniform(50, 300),
                    'SEO Organic': random.uniform(200, 800),
                    'Influencer Marketing': random.uniform(1000, 5000),
                    'TV Advertising': random.uniform(2000, 10000),
                    'Radio': random.uniform(300, 1500),
                    'Affiliate Marketing': random.uniform(400, 2000),
                    'Direct Mail': random.uniform(500, 2500)
                }
                
                daily_budget = base_daily_budgets.get(channel, 500)
                
                # Seasonal adjustments
                month = current_date.month
                if month in [11, 12]:  # Holiday boost
                    daily_budget *= 1.5
                elif month in [1, 2]:  # Post-holiday reduction
                    daily_budget *= 0.7
                
                # Weekend adjustments for digital channels
                if channel in ['Google Ads', 'Facebook Ads', 'Instagram Ads', 'TikTok Ads']:
                    if current_date.weekday() >= 5:  # Weekend
                        daily_budget *= 0.8
                
                # Performance metrics based on channel type
                if 'Ads' in channel:
                    impressions = int(daily_budget * random.uniform(100, 500))
                    ctr = random.uniform(0.01, 0.05)  # 1-5% CTR
                    clicks = int(impressions * ctr)
                    cpc = daily_budget / clicks if clicks > 0 else 0
                    conversion_rate = random.uniform(0.02, 0.15)
                    conversions = int(clicks * conversion_rate)
                else:
                    impressions = None
                    clicks = None
                    cpc = None
                    conversions = int(daily_budget * random.uniform(0.01, 0.05))
                
                marketing_record = {
                    'spend_date': current_date,
                    'channel': channel,
                    'campaign_name': f'{channel} - {fake.catch_phrase()}',
                    'spend_amount': round(daily_budget, 2),
                    'impressions': impressions,
                    'clicks': clicks,
                    'conversions': conversions,
                    'cpc': round(cpc, 2) if cpc else None,
                    'cpm': round(daily_budget / impressions * 1000, 2) if impressions else None,
                    'cpa': round(daily_budget / conversions, 2) if conversions > 0 else None,
                    'conversion_rate': round(conversions / clicks, 4) if clicks and clicks > 0 else None,
                    'roas': round(random.uniform(2.5, 8.0), 2),  # Return on Ad Spend
                    'currency': 'EUR'
                }
                marketing_data.append(marketing_record)
            
            current_date += timedelta(days=1)
        
        self.marketing_spend_df = pd.DataFrame(marketing_data)
        
        print(f"✅ Generated {len(marketing_data):,} marketing spend records")
        print(f"   Total marketing spend: ${self.marketing_spend_df['spend_amount'].sum():,.2f}")
        print(f"   Average daily spend: ${self.marketing_spend_df.groupby('spend_date')['spend_amount'].sum().mean():,.2f}")
        
        return self.marketing_spend_df

    def save_all_data(self, output_dir: str = 'data/raw') -> None:
        """
        Save all generated datasets to CSV files.
        
        Args:
            output_dir: Directory to save the files
        """
        print(f"💾 Saving all datasets to {output_dir}/...")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        datasets = {
            'customers.csv': self.customers_df,
            'products.csv': self.products_df,
            'orders.csv': self.orders_df,
            'order_items.csv': self.order_items_df,
            'web_sessions.csv': self.web_sessions_df,
            'marketing_spend.csv': self.marketing_spend_df
        }
        
        for filename, df in datasets.items():
            if df is not None:
                filepath = os.path.join(output_dir, filename)
                df.to_csv(filepath, index=False)
                print(f"   ✅ Saved {filename}: {len(df):,} records")
        
        print("🎉 All data files saved successfully!")

    def generate_complete_dataset(self, 
                                num_customers: int = 10000,
                                num_products: int = 5000,
                                num_orders: int = 25000,
                                num_sessions: int = 50000) -> None:
        """
        Generate complete e-commerce dataset with all related tables.
        
        This method orchestrates the generation of all data tables in the correct order
        to maintain referential integrity and realistic business relationships.
        
        Args:
            num_customers: Number of customers to generate
            num_products: Number of products to generate  
            num_orders: Number of orders to generate
            num_sessions: Number of web sessions to generate
        """
        print("🚀 Starting complete dataset generation...")
        print("="*60)
        
        # Step 1: Generate dimension tables first (required for fact tables)
        self.generate_customers()
        self.generate_products(num_products)
        
        # Step 2: Generate fact tables that depend on dimensions
        self.generate_orders(num_orders)
        self.generate_web_sessions(num_sessions)
        self.generate_marketing_spend()
        
        print("="*60)
        print("🎉 Complete dataset generation finished!")
        
        # Summary statistics
        self.print_dataset_summary()

    def print_dataset_summary(self) -> None:
        """Print comprehensive summary of generated dataset."""
        print("\n📊 DATASET SUMMARY")
        print("="*50)
        
        if self.customers_df is not None:
            print(f"👥 Customers: {len(self.customers_df):,}")
            print(f"   - Active: {self.customers_df['is_active'].sum():,}")
            print(f"   - VIP: {len(self.customers_df[self.customers_df.customer_segment == 'VIP']):,}")
        
        if self.products_df is not None:
            print(f"📦 Products: {len(self.products_df):,}")
            print(f"   - Active: {self.products_df['is_active'].sum():,}")
            print(f"   - Categories: {self.products_df['category_l1'].nunique()}")
        
        if self.orders_df is not None:
            print(f"🛒 Orders: {len(self.orders_df):,}")
            print(f"   - Total GMV: ${self.orders_df['total_amount'].sum():,.2f}")
            print(f"   - Avg Order Value: ${self.orders_df['total_amount'].mean():.2f}")
        
        if self.order_items_df is not None:
            print(f"📋 Order Items: {len(self.order_items_df):,}")
        
        if self.web_sessions_df is not None:
            print(f"🌐 Web Sessions: {len(self.web_sessions_df):,}")
            print(f"   - Conversion Rate: {self.web_sessions_df['converted'].mean()*100:.2f}%")
        
        if self.marketing_spend_df is not None:
            print(f"💰 Marketing Records: {len(self.marketing_spend_df):,}")
            print(f"   - Total Spend: ${self.marketing_spend_df['spend_amount'].sum():,.2f}")

    def get_data_quality_report(self) -> Dict:
        """
        Generate data quality report for validation.
        
        Returns:
            Dict: Data quality metrics and validation results
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'tables': {},
            'referential_integrity': {},
            'business_rules': {}
        }
        
        # Table-level quality checks
        tables = {
            'customers': self.customers_df,
            'products': self.products_df, 
            'orders': self.orders_df,
            'order_items': self.order_items_df,
            'web_sessions': self.web_sessions_df,
            'marketing_spend': self.marketing_spend_df
        }
        
        for table_name, df in tables.items():
            if df is not None:
                report['tables'][table_name] = {
                    'row_count': len(df),
                    'column_count': len(df.columns),
                    'null_percentages': df.isnull().mean().to_dict(),
                    'duplicate_rows': df.duplicated().sum(),
                    'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
                }
        
        # Referential integrity checks
        if self.orders_df is not None and self.customers_df is not None:
            orders_with_valid_customers = self.orders_df['customer_id'].isin(
                self.customers_df['customer_id']
            ).sum()
            report['referential_integrity']['orders_customers'] = {
                'valid_references': orders_with_valid_customers,
                'total_orders': len(self.orders_df),
                'integrity_percentage': orders_with_valid_customers / len(self.orders_df) * 100
            }
        
        if self.order_items_df is not None and self.products_df is not None:
            items_with_valid_products = self.order_items_df['product_id'].isin(
                self.products_df['product_id']
            ).sum()
            report['referential_integrity']['order_items_products'] = {
                'valid_references': items_with_valid_products,
                'total_items': len(self.order_items_df),
                'integrity_percentage': items_with_valid_products / len(self.order_items_df) * 100
            }
        
        # Business rule validations
        if self.orders_df is not None:
            report['business_rules']['orders'] = {
                'positive_amounts': (self.orders_df['total_amount'] > 0).sum(),
                'valid_dates': (self.orders_df['order_date'] >= self.base_date).sum(),
                'reasonable_totals': (self.orders_df['total_amount'] <= 10000).sum()  # Sanity check
            }
        
        return report


# Example usage and testing
if __name__ == "__main__":
    # Initialize generator
    generator = EcommerceDataGenerator(
        base_date='2023-01-01',
        num_customers=10000
    )
    
    # Generate complete dataset
    generator.generate_complete_dataset(
        num_customers=10000,
        num_products=5000, 
        num_orders=25000,
        num_sessions=50000
    )
    
    # Save all data
    generator.save_all_data('data/raw')
    
    # Generate quality report
    quality_report = generator.get_data_quality_report()
    
    # Save quality report
    with open('data/data_quality_report.json', 'w') as f:
        json.dump(quality_report, f, indent=2, default=str)
    
    print("\n🔍 Data Quality Report saved to 'data/data_quality_report.json'")
    print("\n✅ Dataset generation complete! Ready for dbt transformation.")