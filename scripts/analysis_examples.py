#!/usr/bin/env python3
"""
E-commerce Data Warehouse Analysis Examples

This script demonstrates how to perform advanced analytics on the data warehouse,
showcasing the business value and insights that can be extracted from the 
dimensional model built with dbt.

Examples include:
- Customer segmentation and lifetime value analysis
- Product performance and inventory optimization
- Sales forecasting and trend analysis
- Marketing attribution and ROI analysis
- Geographic and seasonal patterns
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings('ignore')

class EcommerceAnalytics:
    """
    Advanced analytics class for e-commerce data warehouse insights.
    
    This class demonstrates how data engineers can provide business value
    through advanced analytics on the dimensional model.
    """
    
    def __init__(self, connection_string: str = None):
        """Initialize analytics engine with database connection."""
        if connection_string is None:
            connection_string = "postgresql://dbt_user:dbt_password@localhost:5432/ecommerce_dw"
        
        self.engine = create_engine(connection_string)
        self.schema = "dbt_dev"  # Default to development schema
        
        # Set visualization style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
        print("🔍 E-commerce Analytics Engine Initialized")
        print(f"📊 Connected to database: {connection_string.split('@')[1]}")

    def get_data(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return pandas DataFrame."""
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            print(f"❌ Query execution failed: {e}")
            return pd.DataFrame()

    def customer_lifetime_value_analysis(self) -> dict:
        """
        Comprehensive Customer Lifetime Value (CLV) Analysis
        
        Returns insights on:
        - Customer segments by CLV
        - CLV distribution and trends
        - High-value customer characteristics
        - Churn risk assessment
        """
        print("\n📈 Running Customer Lifetime Value Analysis...")
        
        clv_query = f"""
        WITH customer_metrics AS (
            SELECT 
                c.customer_id,
                c.full_name,
                c.customer_segment,
                c.acquisition_channel,
                c.tenure_days,
                c.age_years,
                c.state_code,
                COUNT(o.order_id) as total_orders,
                SUM(o.total_amount) as total_revenue,
                AVG(o.total_amount) as avg_order_value,
                MAX(o.order_date) as last_order_date,
                MIN(o.order_date) as first_order_date,
                EXTRACT(days FROM MAX(o.order_date) - MIN(o.order_date)) as customer_lifespan_days,
                CASE 
                    WHEN COUNT(o.order_id) >= 10 THEN 'High Frequency'
                    WHEN COUNT(o.order_id) >= 5 THEN 'Medium Frequency'
                    WHEN COUNT(o.order_id) >= 2 THEN 'Low Frequency'
                    ELSE 'One-time'
                END as frequency_segment,
                CASE 
                    WHEN SUM(o.total_amount) >= 2000 THEN 'High Value'
                    WHEN SUM(o.total_amount) >= 500 THEN 'Medium Value'
                    ELSE 'Low Value'
                END as value_segment
            FROM {self.schema}.dim_customers c
            LEFT JOIN {self.schema}.fact_orders o ON c.customer_id = o.customer_id
            WHERE c.is_active = true
            GROUP BY c.customer_id, c.full_name, c.customer_segment, 
                     c.acquisition_channel, c.tenure_days, c.age_years, c.state_code
        ),
        clv_calculations AS (
            SELECT 
                *,
                CASE 
                    WHEN customer_lifespan_days > 0 
                    THEN (total_revenue / customer_lifespan_days) * 365 
                    ELSE total_revenue 
                END as projected_annual_value,
                CASE 
                    WHEN total_orders > 1 AND customer_lifespan_days > 0
                    THEN customer_lifespan_days::float / (total_orders - 1)
                    ELSE NULL
                END as avg_days_between_orders
            FROM customer_metrics
        )
        SELECT * FROM clv_calculations
        ORDER BY total_revenue DESC;
        """
        
        clv_data = self.get_data(clv_query)
        
        if clv_data.empty:
            return {"error": "No customer data available"}
        
        # Calculate key metrics
        insights = {
            "total_customers": len(clv_data),
            "avg_clv": clv_data['total_revenue'].mean(),
            "median_clv": clv_data['total_revenue'].median(),
            "top_10_percent_clv": clv_data['total_revenue'].quantile(0.9),
            "churn_risk_customers": len(clv_data[clv_data['last_order_date'] < pd.Timestamp.now() - pd.Timedelta(days=90)]),
            "high_value_customers": len(clv_data[clv_data['value_segment'] == 'High Value']),
            "one_time_customers": len(clv_data[clv_data['frequency_segment'] == 'One-time'])
        }
        
        # Create visualizations
        self._create_clv_visualizations(clv_data)
        
        print(f"   📊 Analyzed {insights['total_customers']:,} customers")
        print(f"   💰 Average CLV: ${insights['avg_clv']:.2f}")
        print(f"   🎯 Top 10% CLV threshold: ${insights['top_10_percent_clv']:.2f}")
        print(f"   ⚠️  Customers at churn risk: {insights['churn_risk_customers']:,}")
        
        return {"data": clv_data, "insights": insights}

    def _create_clv_visualizations(self, clv_data: pd.DataFrame):
        """Create comprehensive CLV visualizations."""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('CLV Distribution', 'CLV by Segment', 
                          'Frequency vs Value', 'CLV by Acquisition Channel'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # CLV Distribution
        fig.add_trace(
            go.Histogram(x=clv_data['total_revenue'], nbinsx=50, name='CLV Distribution'),
            row=1, col=1
        )
        
        # CLV by Customer Segment
        segment_avg = clv_data.groupby('customer_segment')['total_revenue'].mean().reset_index()
        fig.add_trace(
            go.Bar(x=segment_avg['customer_segment'], y=segment_avg['total_revenue'], 
                   name='Avg CLV by Segment'),
            row=1, col=2
        )
        
        # Frequency vs Value scatter
        fig.add_trace(
            go.Scatter(x=clv_data['total_orders'], y=clv_data['total_revenue'],
                      mode='markers', name='Frequency vs Value',
                      text=clv_data['customer_segment'],
                      marker=dict(size=8, opacity=0.6)),
            row=2, col=1
        )
        
        # CLV by Acquisition Channel
        channel_avg = clv_data.groupby('acquisition_channel')['total_revenue'].mean().reset_index()
        fig.add_trace(
            go.Bar(x=channel_avg['acquisition_channel'], y=channel_avg['total_revenue'],
                   name='CLV by Channel'),
            row=2, col=2
        )
        
        fig.update_layout(height=800, showlegend=False, title_text="Customer Lifetime Value Analysis")
        fig.write_html("clv_analysis.html")
        print("   📊 CLV visualizations saved to clv_analysis.html")

    def product_performance_analysis(self) -> dict:
        """
        Advanced Product Performance Analysis
        
        Analyzes:
        - Top performing products by revenue and margin
        - Product lifecycle analysis
        - Inventory optimization recommendations
        - Cross-selling opportunities
        - Seasonal product trends
        """
        print("\n📦 Running Product Performance Analysis...")
        
        product_query = f"""
        WITH product_performance AS (
            SELECT 
                p.product_id,
                p.product_name,
                p.brand,
                p.category_primary,
                p.category_secondary,
                p.retail_price,
                p.cost,
                p.margin_percent,
                p.stock_quantity,
                p.lifecycle_stage,
                p.avg_rating,
                p.total_reviews,
                COUNT(DISTINCT oi.order_id) as order_count,
                SUM(oi.quantity) as total_units_sold,
                SUM(oi.line_total) as total_revenue,
                SUM(oi.line_cost) as total_cost,
                SUM(oi.line_total - oi.line_cost) as total_profit,
                AVG(oi.quantity) as avg_quantity_per_order,
                COUNT(DISTINCT o.customer_id) as unique_customers,
                EXTRACT(days FROM CURRENT_DATE - MIN(o.order_date)) as days_since_first_sale,
                MAX(o.order_date) as last_sale_date
            FROM {self.schema}.dim_products p
            LEFT JOIN {self.schema}.fact_order_items oi ON p.product_id = oi.product_id
            LEFT JOIN {self.schema}.fact_orders o ON oi.order_id = o.order_id
            WHERE p.is_active = true
            GROUP BY p.product_id, p.product_name, p.brand, p.category_primary, 
                     p.category_secondary, p.retail_price, p.cost, p.margin_percent,
                     p.stock_quantity, p.lifecycle_stage, p.avg_rating, p.total_reviews
        ),
        product_metrics AS (
            SELECT 
                *,
                CASE 
                    WHEN total_units_sold >= 100 THEN 'High Volume'
                    WHEN total_units_sold >= 50 THEN 'Medium Volume'
                    WHEN total_units_sold >= 10 THEN 'Low Volume'
                    ELSE 'No Sales'
                END as volume_category,
                CASE 
                    WHEN days_since_first_sale > 0 
                    THEN total_units_sold::float / days_since_first_sale * 30
                    ELSE 0 
                END as monthly_velocity,
                CASE 
                    WHEN stock_quantity > 0 AND monthly_velocity > 0
                    THEN stock_quantity / monthly_velocity
                    ELSE NULL
                END as months_of_inventory,
                ROW_NUMBER() OVER (ORDER BY total_revenue DESC) as revenue_rank,
                ROW_NUMBER() OVER (ORDER BY total_profit DESC) as profit_rank,
                ROW_NUMBER() OVER (ORDER BY total_units_sold DESC) as volume_rank
            FROM product_performance
        )
        SELECT * FROM product_metrics
        ORDER BY total_revenue DESC;
        """
        
        product_data = self.get_data(product_query)
        
        if product_data.empty:
            return {"error": "No product data available"}
        
        # Calculate insights
        insights = {
            "total_products": len(product_data),
            "products_with_sales": len(product_data[product_data['total_revenue'] > 0]),
            "avg_product_revenue": product_data['total_revenue'].mean(),
            "top_revenue_product": product_data.iloc[0]['product_name'] if len(product_data) > 0 else None,
            "avg_margin": product_data['margin_percent'].mean(),
            "low_stock_products": len(product_data[product_data['months_of_inventory'] < 1]),
            "overstocked_products": len(product_data[product_data['months_of_inventory'] > 6])
        }
        
        # Category analysis
        category_performance = product_data.groupby('category_primary').agg({
            'total_revenue': 'sum',
            'total_profit': 'sum',
            'total_units_sold': 'sum',
            'margin_percent': 'mean'
        }).reset_index().sort_values('total_revenue', ascending=False)
        
        self._create_product_visualizations(product_data, category_performance)
        
        print(f"   📦 Analyzed {insights['total_products']:,} products")
        print(f"   💰 Products with sales: {insights['products_with_sales']:,}")
        print(f"   📈 Average margin: {insights['avg_margin']:.1f}%")
        print(f"   ⚠️  Low stock alerts: {insights['low_stock_products']:,}")
        
        return {
            "data": product_data, 
            "insights": insights,
            "category_performance": category_performance
        }

    def _create_product_visualizations(self, product_data: pd.DataFrame, category_data: pd.DataFrame):
        """Create product performance visualizations."""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Revenue by Category', 'Price vs Volume', 
                          'Inventory Health', 'Margin Distribution'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Revenue by Category
        fig.add_trace(
            go.Bar(x=category_data['category_primary'], y=category_data['total_revenue'],
                   name='Revenue by Category'),
            row=1, col=1
        )
        
        # Price vs Volume scatter
        fig.add_trace(
            go.Scatter(x=product_data['retail_price'], y=product_data['total_units_sold'],
                      mode='markers', name='Price vs Volume',
                      text=product_data['product_name'],
                      marker=dict(size=8, opacity=0.6)),
            row=1, col=2
        )
        
        # Inventory Health
        inventory_health = product_data['months_of_inventory'].fillna(0)
        fig.add_trace(
            go.Histogram(x=inventory_health, nbinsx=20, name='Months of Inventory'),
            row=2, col=1
        )
        
        # Margin Distribution
        fig.add_trace(
            go.Histogram(x=product_data['margin_percent'], nbinsx=20, name='Margin %'),
            row=2, col=2
        )
        
        fig.update_layout(height=800, showlegend=False, title_text="Product Performance Analysis")
        fig.write_html("product_analysis.html")
        print("   📊 Product visualizations saved to product_analysis.html")

    def sales_forecasting_analysis(self) -> dict:
        """
        Sales Forecasting and Trend Analysis
        
        Provides:
        - Daily/weekly/monthly sales trends
        - Seasonal pattern identification
        - Growth rate calculations
        - Forecasting recommendations
        """
        print("\n📈 Running Sales Forecasting Analysis...")
        
        sales_query = f"""
        WITH daily_sales AS (
            SELECT 
                o.order_date,
                EXTRACT(year FROM o.order_date) as year,
                EXTRACT(month FROM o.order_date) as month,
                EXTRACT(dow FROM o.order_date) as day_of_week,
                EXTRACT(quarter FROM o.order_date) as quarter,
                COUNT(DISTINCT o.order_id) as order_count,
                COUNT(DISTINCT o.customer_id) as unique_customers,
                SUM(o.total_amount) as daily_revenue,
                AVG(o.total_amount) as avg_order_value,
                SUM(o.total_items) as total_items_sold
            FROM {self.schema}.fact_orders o
            WHERE o.order_status = 'Delivered'
            GROUP BY o.order_date, year, month, day_of_week, quarter
        ),
        monthly_trends AS (
            SELECT 
                year,
                month,
                SUM(daily_revenue) as monthly_revenue,
                SUM(order_count) as monthly_orders,
                AVG(unique_customers) as avg_daily_customers,
                LAG(SUM(daily_revenue)) OVER (ORDER BY year, month) as prev_month_revenue
            FROM daily_sales
            GROUP BY year, month
        )
        SELECT 
            ds.*,
            mt.monthly_revenue,
            mt.prev_month_revenue,
            CASE 
                WHEN mt.prev_month_revenue > 0 
                THEN (mt.monthly_revenue - mt.prev_month_revenue) / mt.prev_month_revenue * 100
                ELSE NULL 
            END as month_over_month_growth
        FROM daily_sales ds
        LEFT JOIN monthly_trends mt ON ds.year = mt.year AND ds.month = mt.month
        ORDER BY ds.order_date;
        """
        
        sales_data = self.get_data(sales_query)
        
        if sales_data.empty:
            return {"error": "No sales data available"}
        
        # Calculate trend insights
        total_revenue = sales_data['daily_revenue'].sum()
        avg_daily_revenue = sales_data['daily_revenue'].mean()
        revenue_trend = sales_data['daily_revenue'].pct_change().mean() * 100
        
        # Seasonal analysis
        seasonal_data = sales_data.groupby(['month']).agg({
            'daily_revenue': 'mean',
            'order_count': 'mean',
            'unique_customers': 'mean'
        }).reset_index()
        
        # Day of week analysis
        dow_data = sales_data.groupby('day_of_week').agg({
            'daily_revenue': 'mean',
            'order_count': 'mean'
        }).reset_index()
        
        # Map day numbers to names
        dow_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
        dow_data['day_name'] = dow_data['day_of_week'].map(lambda x: dow_names[int(x)])
        
        insights = {
            "total_revenue": total_revenue,
            "avg_daily_revenue": avg_daily_revenue,
            "revenue_trend_percent": revenue_trend,
            "best_month": seasonal_data.loc[seasonal_data['daily_revenue'].idxmax(), 'month'],
            "best_day_of_week": dow_data.loc[dow_data['daily_revenue'].idxmax(), 'day_name'],
            "growth_rate": sales_data['month_over_month_growth'].mean()
        }
        
        self._create_sales_visualizations(sales_data, seasonal_data, dow_data)
        
        print(f"   💰 Total revenue analyzed: ${total_revenue:,.2f}")
        print(f"   📊 Average daily revenue: ${avg_daily_revenue:,.2f}")
        print(f"   📈 Revenue trend: {revenue_trend:.2f}% daily change")
        print(f"   🏆 Best performing month: {insights['best_month']}")
        
        return {
            "data": sales_data,
            "insights": insights,
            "seasonal_data": seasonal_data,
            "dow_data": dow_data
        }

    def _create_sales_visualizations(self, sales_data: pd.DataFrame, 
                                   seasonal_data: pd.DataFrame, dow_data: pd.DataFrame):
        """Create sales forecasting visualizations."""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Daily Revenue Trend', 'Seasonal Patterns', 
                          'Day of Week Performance', 'Growth Rate'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Daily Revenue Trend
        fig.add_trace(
            go.Scatter(x=sales_data['order_date'], y=sales_data['daily_revenue'],
                      mode='lines', name='Daily Revenue'),
            row=1, col=1
        )
        
        # Seasonal Patterns
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        fig.add_trace(
            go.Bar(x=[month_names[int(m)-1] for m in seasonal_data['month']], 
                   y=seasonal_data['daily_revenue'],
                   name='Avg Revenue by Month'),
            row=1, col=2
        )
        
        # Day of Week Performance
        fig.add_trace(
            go.Bar(x=dow_data['day_name'], y=dow_data['daily_revenue'],
                   name='Avg Revenue by Day'),
            row=2, col=1
        )
        
        # Growth Rate
        fig.add_trace(
            go.Scatter(x=sales_data['order_date'], y=sales_data['month_over_month_growth'],
                      mode='lines', name='MoM Growth %'),
            row=2, col=2
        )
        
        fig.update_layout(height=800, showlegend=False, title_text="Sales Forecasting Analysis")
        fig.write_html("sales_analysis.html")
        print("   📊 Sales visualizations saved to sales_analysis.html")

    def marketing_attribution_analysis(self) -> dict:
        """
        Marketing Attribution and ROI Analysis
        
        Analyzes:
        - Channel performance and ROI
        - Customer acquisition costs
        - Attribution modeling
        - Campaign effectiveness
        """
        print("\n📢 Running Marketing Attribution Analysis...")
        
        marketing_query = f"""
        WITH channel_performance AS (
            SELECT 
                c.acquisition_channel,
                COUNT(DISTINCT c.customer_id) as customers_acquired,
                SUM(o.total_amount) as total_revenue,
                AVG(o.total_amount) as avg_order_value,
                COUNT(DISTINCT o.order_id) as total_orders,
                SUM(ms.spend_amount) as total_spend,
                SUM(ms.conversions) as total_conversions
            FROM {self.schema}.dim_customers c
            LEFT JOIN {self.schema}.fact_orders o ON c.customer_id = o.customer_id
            LEFT JOIN {self.schema}.fact_marketing_spend ms ON c.acquisition_channel = ms.channel
            GROUP BY c.acquisition_channel
        ),
        channel_metrics AS (
            SELECT 
                *,
                CASE 
                    WHEN customers_acquired > 0 
                    THEN total_spend / customers_acquired 
                    ELSE NULL 
                END as customer_acquisition_cost,
                CASE 
                    WHEN total_spend > 0 
                    THEN total_revenue / total_spend 
                    ELSE NULL 
                END as return_on_ad_spend,
                CASE 
                    WHEN customers_acquired > 0 
                    THEN total_revenue / customers_acquired 
                    ELSE NULL 
                END as customer_lifetime_value
            FROM channel_performance
        )
        SELECT * FROM channel_metrics
        ORDER BY total_revenue DESC;
        """
        
        marketing_data = self.get_data(marketing_query)
        
        if marketing_data.empty:
            return {"error": "No marketing data available"}
        
        # Calculate insights
        total_spend = marketing_data['total_spend'].sum()
        total_revenue = marketing_data['total_revenue'].sum()
        overall_roas = total_revenue / total_spend if total_spend > 0 else 0
        
        best_channel = marketing_data.loc[marketing_data['return_on_ad_spend'].idxmax()]
        worst_channel = marketing_data.loc[marketing_data['return_on_ad_spend'].idxmin()]
        
        insights = {
            "total_marketing_spend": total_spend,
            "total_attributed_revenue": total_revenue,
            "overall_roas": overall_roas,
            "best_performing_channel": best_channel['acquisition_channel'],
            "best_channel_roas": best_channel['return_on_ad_spend'],
            "worst_performing_channel": worst_channel['acquisition_channel'],
            "avg_customer_acquisition_cost": marketing_data['customer_acquisition_cost'].mean()
        }
        
        self._create_marketing_visualizations(marketing_data)
        
        print(f"   💰 Total marketing spend: ${total_spend:,.2f}")
        print(f"   📈 Overall ROAS: {overall_roas:.2f}x")
        print(f"   🏆 Best channel: {insights['best_performing_channel']} ({insights['best_channel_roas']:.2f}x ROAS)")
        print(f"   💸 Avg CAC: ${insights['avg_customer_acquisition_cost']:.2f}")
        
        return {"data": marketing_data, "insights": insights}

    def _create_marketing_visualizations(self, marketing_data: pd.DataFrame):
        """Create marketing attribution visualizations."""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('ROAS by Channel', 'CAC vs CLV', 
                          'Revenue Attribution', 'Spend Distribution'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # ROAS by Channel
        fig.add_trace(
            go.Bar(x=marketing_data['acquisition_channel'], 
                   y=marketing_data['return_on_ad_spend'],
                   name='ROAS by Channel'),
            row=1, col=1
        )
        
        # CAC vs CLV scatter
        fig.add_trace(
            go.Scatter(x=marketing_data['customer_acquisition_cost'], 
                      y=marketing_data['customer_lifetime_value'],
                      mode='markers+text',
                      text=marketing_data['acquisition_channel'],
                      textposition="top center",
                      name='CAC vs CLV'),
            row=1, col=2
        )
        
        # Revenue Attribution pie chart
        fig.add_trace(
            go.Pie(labels=marketing_data['acquisition_channel'], 
                   values=marketing_data['total_revenue'],
                   name='Revenue Attribution'),
            row=2, col=1
        )
        
        # Spend Distribution pie chart
        fig.add_trace(
            go.Pie(labels=marketing_data['acquisition_channel'], 
                   values=marketing_data['total_spend'],
                   name='Spend Distribution'),
            row=2, col=2
        )
        
        fig.update_layout(height=800, showlegend=False, title_text="Marketing Attribution Analysis")
        fig.write_html("marketing_analysis.html")
        print("   📊 Marketing visualizations saved to marketing_analysis.html")

    def comprehensive_business_report(self) -> dict:
        """
        Generate a comprehensive business intelligence report combining all analyses.
        """
        print("\n📋 Generating Comprehensive Business Report...")
        
        # Run all analyses
        clv_results = self.customer_lifetime_value_analysis()
        product_results = self.product_performance_analysis()
        sales_results = self.sales_forecasting_analysis()
        marketing_results = self.marketing_attribution_analysis()
        
        # Compile executive summary
        executive_summary = {
            "report_date": pd.Timestamp.now().strftime("%Y-%m-%d"),
            "key_metrics": {
                "total_customers": clv_results.get("insights", {}).get("total_customers", 0),
                "total_products": product_results.get("insights", {}).get("total_products", 0),
                "total_revenue": sales_results.get("insights", {}).get("total_revenue", 0),
                "avg_clv": clv_results.get("insights", {}).get("avg_clv", 0),
                "overall_roas": marketing_results.get("insights", {}).get("overall_roas", 0)
            },
            "recommendations": [
                "Focus on high-CLV customer retention programs",
                "Optimize inventory for fast-moving products",
                "Reallocate marketing budget to highest ROAS channels",
                "Implement seasonal promotional strategies",
                "Develop cross-selling programs for complementary products"
            ]
        }
        
        print("   ✅ Comprehensive report generated")
        print(f"   📊 Total customers analyzed: {executive_summary['key_metrics']['total_customers']:,}")
        print(f"   💰 Total revenue: ${executive_summary['key_metrics']['total_revenue']:,.2f}")
        print(f"   📈 Marketing ROAS: {executive_summary['key_metrics']['overall_roas']:.2f}x")
        
        return {
            "executive_summary": executive_summary,
            "clv_analysis": clv_results,
            "product_analysis": product_results,
            "sales_analysis": sales_results,
            "marketing_analysis": marketing_results
        }


def main():
    """Main function to demonstrate analytics capabilities."""
    print("🚀 E-commerce Data Warehouse Analytics Demo")
    print("=" * 50)
    
    # Initialize analytics engine
    analytics = EcommerceAnalytics()
    
    try:
        # Generate comprehensive business report
        report = analytics.comprehensive_business_report()
        
        print("\n" + "=" * 50)
        print("📊 EXECUTIVE SUMMARY")
        print("=" * 50)
        
        summary = report["executive_summary"]
        metrics = summary["key_metrics"]
        
        print(f"Report Date: {summary['report_date']}")
        print(f"Total Customers: {metrics['total_customers']:,}")
        print(f"Total Products: {metrics['total_products']:,}")
        print(f"Total Revenue: ${metrics['total_revenue']:,.2f}")
        print(f"Average CLV: ${metrics['avg_clv']:,.2f}")
        print(f"Marketing ROAS: {metrics['overall_roas']:.2f}x")
        
        print("\n🎯 KEY RECOMMENDATIONS:")
        for i, rec in enumerate(summary["recommendations"], 1):
            print(f"{i}. {rec}")
        
        print("\n📊 Detailed analysis reports saved as HTML files:")
        print("   • clv_analysis.html - Customer lifetime value insights")
        print("   • product_analysis.html - Product performance metrics")
        print("   • sales_analysis.html - Sales trends and forecasting")
        print("   • marketing_analysis.html - Marketing attribution analysis")
        
        print("\n✅ Analytics demo completed successfully!")
        
    except Exception as e:
        print(f"❌ Analytics demo failed: {e}")
        return False
    
    return True


if __name__ == "__main__":
    main()