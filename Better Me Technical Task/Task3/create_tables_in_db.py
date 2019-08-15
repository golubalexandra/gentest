import json
from sqlalchemy import create_engine



if __name__ == '__main__':

	with open('db_config.json') as file:
		db_config=json.load(file)

	db_engine = create_engine('mysql://{user}:{password}@{host}:{port}/{database}'.format(**db_config), echo=False)

	with db_engine.connect() as conn:

		conn.execute('''
					CREATE TABLE IF NOT EXISTS apps (
						id BIGINT NOT NULL,
						name VARCHAR(100) NOT NULL,
						PRIMARY KEY (id)
						)
				''')


		conn.execute('''
					CREATE TABLE IF NOT EXISTS exchange_rates (
						exchange_date DATE NOT NULL,
						currency_code VARCHAR(3) NOT NULL,
						usd_to_currency DECIMAL(16, 8) NOT NULL,
						PRIMARY KEY (exchange_date, currency_code,
						            usd_to_currency)

						)
				''')

		conn.execute('''
					CREATE TABLE IF NOT EXISTS subscriptions (
						id BIGINT NOT NULL,
						name VARCHAR(100) NOT NULL,
						duration VARCHAR(20) NOT NULL,
						group_id VARCHAR(100) NOT NULL,
						PRIMARY KEY (id)
						)
				''')


		conn.execute('''
					CREATE TABLE IF NOT EXISTS country_to_currency (
						country_code VARCHAR(2) NOT NULL,
						currency_code VARCHAR(3) NOT NULL,
						PRIMARY KEY (country_code)
						)
				''')

		conn.execute('''
					CREATE TABLE IF NOT EXISTS events (
						date DATE NOT NULL,
						app_apple_id BIGINT NOT NULL,
	                    subscription_apple_id BIGINT NOT NULL,
	                    introductory_price_type VARCHAR(20), 
	                    introductory_price_duration VARCHAR(20), 
	                    marketing_opt_in_duration VARCHAR(20),
	                    subscriber_id BIGINT NOT NULL, 
	                    customer_price DECIMAL(12,2) NOT NULL, 
	                    developer_proceeds DECIMAL(12,2) NOT NULL, 
	                    is_price_preserved TINYINT(1),
	                    is_proceed_higher TINYINT(1), 
	                    is_purchased_from_news TINYINT(1),
	                    device VARCHAR(20), 
	                    country_code VARCHAR(2),
	                    is_subscriber_id_reset TINYINT(1),
	                    is_refund TINYINT(1),
	                    purchase_date DATE NULL,  
	                    units INT,
	                	FOREIGN KEY (app_apple_id) REFERENCES apps(id),
	                	FOREIGN KEY (subscription_apple_id) REFERENCES subscriptions(id),
	                	FOREIGN KEY (country_code) REFERENCES country_to_currency(country_code),
	                	PRIMARY KEY (date, app_apple_id, subscription_apple_id, 
	                				subscriber_id, 
	                				customer_price, developer_proceeds,
	                				device, country_code,
	                				is_refund
							)
	                    )   
				''')

