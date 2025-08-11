# 🚀 Quick Start Guide - E-commerce Data Warehouse

Get your professional data warehouse up and running in **less than 10 minutes**!

## ⚡ One-Command Setup

```bash
# Clone the repository (replace with your actual repo URL)
git clone <your-repo-url>
cd ecommerce-data-warehouse

# Run automated setup
python setup.py
```

That's it! The setup script will automatically:
- ✅ Install all dependencies
- ✅ Start PostgreSQL database
- ✅ Generate 25,000+ realistic records
- ✅ Run complete dbt transformation
- ✅ Execute data quality tests
- ✅ Generate documentation

## 🎯 Setup Options

### Quick Demo (Minimal Data)
```bash
python setup.py --quick
```
- 1,000 customers, 500 products, 2,000 orders
- Perfect for testing and demos
- Setup time: ~3 minutes

### Full Production (Large Dataset)
```bash
python setup.py --production
```
- 50,000 customers, 10,000 products, 200,000 orders
- Production-scale demonstration
- Setup time: ~8 minutes

### Validate Existing Setup
```bash
python setup.py --validate-only
```

## 🔍 Verify Installation

After setup completes, verify everything works:

```bash
# Check database is running
docker-compose ps

# Test dbt connection
cd dbt_project && dbt debug

# Run a simple query
cd dbt_project && dbt run-operation test_connection
```

## 📊 Access Your Data Warehouse

### 1. View dbt Documentation
```bash
cd dbt_project
dbt docs serve
```
Open: http://localhost:8080

### 2. Connect to Database
- **Host**: localhost:5432
- **Database**: ecommerce_dw
- **Username**: dbt_user
- **Password**: dbt_password

### 3. pgAdmin (Database GUI)
Open: http://localhost:8080
- **Email**: admin@ecommerce-dw.com
- **Password**: admin123

### 4. Metabase (BI Tool)
```bash
docker-compose up -d metabase
```
Open: http://localhost:3000

## 🔧 Common Commands

### dbt Operations
```bash
cd dbt_project

# Run all models
dbt run

# Run only staging models
dbt run --select staging

# Run tests
dbt test

# Generate fresh sample data
dbt seed --full-refresh

# Update documentation
dbt docs generate && dbt docs serve
```

### Database Management
```bash
# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs postgres

# Reset database (WARNING: Deletes all data)
docker-compose down -v
```

### Data Analysis
```bash
# Run business intelligence examples
python scripts/analysis_examples.py

# Generate new sample data
python scripts/generate_sample_data.py
```

## 🛠️ Customization

### Change Data Volume
Edit `setup.py` and modify:
```python
self.setup_config["sample_data_config"] = {
    "customers": 5000,     # Adjust these numbers
    "products": 2000,
    "orders": 10000,
    "sessions": 20000
}
```

### Add New Data Sources
1. Add CSV files to `data/raw/`
2. Define sources in `dbt_project/models/staging/_sources.yml`
3. Create staging models in `dbt_project/models/staging/`

### Modify Business Logic
- Edit models in `dbt_project/models/`
- Add custom tests in `dbt_project/tests/`
- Create macros in `dbt_project/macros/`

## 📈 Sample Queries

### Top Customers by Revenue
```sql
SELECT 
    customer_id, 
    full_name, 
    total_revenue,
    customer_segment
FROM dbt_dev.dim_customers 
ORDER BY total_revenue DESC 
LIMIT 10;
```

### Best Selling Products
```sql
SELECT 
    p.product_name,
    p.category_primary,
    SUM(oi.quantity) as units_sold,
    SUM(oi.line_total) as revenue
FROM dbt_dev.dim_products p
JOIN dbt_dev.fact_order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name, p.category_primary
ORDER BY revenue DESC
LIMIT 10;
```

### Monthly Revenue Trend
```sql
SELECT 
    EXTRACT(year FROM order_date) as year,
    EXTRACT(month FROM order_date) as month,
    COUNT(*) as order_count,
    SUM(total_amount) as monthly_revenue
FROM dbt_dev.fact_orders 
WHERE order_status = 'Delivered'
GROUP BY year, month
ORDER BY year, month;
```

## 🚨 Troubleshooting

### Database Connection Issues
```bash
# Check if PostgreSQL is running
docker-compose ps postgres

# Restart PostgreSQL
docker-compose restart postgres

# Check logs for errors
docker-compose logs postgres
```

### dbt Issues
```bash
# Check dbt installation
dbt --version

# Verify profiles
dbt debug

# Check model compilation
dbt compile
```

### Python Dependencies
```bash
# Reinstall requirements
pip install -r requirements.txt --force-reinstall

# Check Python version (needs 3.8+)
python --version
```

### Port Conflicts
If ports are in use, edit `docker-compose.yml`:
```yaml
ports:
  - "5433:5432"  # Change 5432 to 5433
```

## 🎯 Next Steps

### 1. Customize for Your Business
- Modify product categories
- Add industry-specific metrics
- Implement custom business rules

### 2. Add Advanced Features
- **Real-time processing**: Integrate Kafka/Airflow
- **Machine Learning**: Add dbt-ml models
- **Advanced BI**: Connect Tableau/Power BI
- **Cloud deployment**: Deploy to AWS/GCP/Azure

### 3. Production Deployment
- Set up CI/CD with GitHub Actions
- Configure monitoring and alerting
- Implement backup strategies
- Add security measures

### 4. Expand Data Sources
- Connect to APIs (Shopify, Stripe, etc.)
- Add web analytics (Google Analytics)
- Integrate CRM data (Salesforce, HubSpot)
- Include marketing platforms (Facebook, Google Ads)

## 📚 Learning Resources

### dbt Resources
- [dbt Official Docs](https://docs.getdbt.com/) - Complete dbt documentation
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices) - Industry standards
- [dbt Packages Hub](https://hub.getdbt.com/) - Community packages

### Data Engineering
- [Analytics Engineering Guide](https://www.getdbt.com/analytics-engineering/) - Modern data stack
- [Dimensional Modeling](https://www.kimballgroup.com/) - Data warehouse design
- [Data Quality Testing](https://greatexpectations.io/) - Advanced validation

### Business Intelligence
- [SQL Style Guide](https://www.sqlstyle.guide/) - Clean SQL practices
- [KPI Documentation](https://amplitude.com/blog/kpi-guide) - Metrics that matter

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/amazing-feature`
3. Run tests: `dbt test`
4. Commit changes: `git commit -m 'Add amazing feature'`
5. Push branch: `git push origin feature/amazing-feature`
6. Open Pull Request

## 💡 Pro Tips

### Performance Optimization
- Use `dbt run --select +model_name` to run model and dependencies
- Add indexes on frequently queried columns
- Use incremental models for large tables
- Monitor query performance with `dbt compile --debug`

### Data Quality
- Always add tests for primary keys and foreign keys
- Use `dbt_expectations` for advanced data validation
- Set up freshness monitoring for critical data sources
- Document all models and columns

### Production Readiness
- Use environment variables for sensitive data
- Implement proper error handling
- Set up monitoring and alerting
- Document deployment procedures

## 📞 Support

- **Issues**: Create GitHub issues for bugs
- **Questions**: Check dbt Community Slack
- **Documentation**: Run `dbt docs serve` for local docs
- **Performance**: Use `dbt --debug` for troubleshooting

---

**🎉 Happy Data Engineering!**

*This data warehouse demonstrates enterprise-level capabilities and is designed to showcase advanced data engineering skills for portfolio and interview purposes.*