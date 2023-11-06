list_of_countries = [ 'Afghanistan', 'Albania', 'Algeria', 'Andorra',
                      'Angola', 'Antigua and Barbuda', 'Argentina', 'Armenia',
                      'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain',
                      'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin',
                      'Bhutan', 'Bolivia', 'Bosnia and Herzegovina', 'Botswana',
                      'Brazil', 'Brunei', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Cabo Verde',
                      'Cambodia', 'Cameroon', 'Canada', 'Central African Republic',
                      'Chad', 'Chile', 'China', 'Colombia', 'Comoros',
                      'Congo, Democratic Republic of the', 'Congo, Republic of the',
                      'Costa Rica', 'Cote dIvoire', 'Croatia', 'Cuba', 'Cyprus',
                      'Czechia', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic',
                      'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea',
                      'Eritrea', 'Estonia', 'Eswatini', 'Ethiopia', 'Fiji', 'Finland',
                      'France', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana',
                      'Greece', 'Grenada', 'Guatemala', 'Guinea', 'Guinea-Bissau',
                      'Guyana', 'Haiti', 'Honduras', 'Hungary', 'Iceland', 'India',
                      'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 'Italy',
                      'Jamaica', 'Japan', 'Jordan', 'Kazakhstan', 'Kenya', 'Kiribati',
                      'Kosovo', 'Kuwait', 'Kyrgyzstan', 'Laos', 'Latvia', 'Lebanon',
                      'Lesotho', 'Liberia', 'Libya', 'Liechtenstein', 'Lithuania',
                      'Luxembourg', 'Madagascar', 'Malawi', 'Malaysia', 'Maldives',
                      'Mali', 'Malta', 'Marshall Islands', 'Mauritania', 'Mauritius',
                      'Mexico', 'Micronesia', 'Moldova', 'Monaco', 'Mongolia',
                      'Montenegro', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia',
                      'Nauru', 'Nepal', 'Netherlands', 'New Zealand', 'Nicaragua',
                      'Niger', 'Nigeria', 'North Korea', 'North Macedonia', 'Norway',
                      'Oman', 'Pakistan', 'Palau', 'Palestine', 'Panama',
                      'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Poland', 'Portugal',
                      'Qatar', 'Romania', 'Russia', 'Rwanda', 'Saint Kitts and Nevis',
                      'Saint Lucia', 'Saint Vincent and the Grenadines', 'Samoa',
                      'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal',
                      'Serbia', 'Seychelles', 'Sierra Leone', 'Singapore', 'Slovakia',
                      'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa', 'South Korea',
                      'South Sudan', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname',
                      'Sweden', 'Switzerland', 'Syria', 'Taiwan', 'Tajikistan',
                      'Tanzania', 'Thailand', 'Timor-Leste', 'Togo', 'Tonga',
                      'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan', 'Tuvalu',
                      'Uganda', 'Ukraine', 'United Arab Emirates (UAE)', 'United Kingdom',
                      'United States of America (USA)', 'Uruguay', 'Uzbekistan',
                      'Vanuatu', 'Vatican City (Holy See)', 'Venezuela', 'Vietnam',
                      'Yemen', 'Zambia', 'Zimbabwe' ]
list_of_genders = ['Male', 'Female']
list_of_ages = ['0-16', '17-25', '26-35', '36-45', '46-55', '56-65', '66-75', '76+']
list_of_incomes = ['0-10k', '10k-20k', '20k-40k', '40k-60k', '60k-100k', '100k-150k', '150k-250k', '250k+']
# initialize parameters
INSTANCE_CONNECTION_NAME = f"wbh-project-398814:us-central1:hw6" # i.e demo-project:us-central1:demo-instance
print(f"Your instance connection name is: {INSTANCE_CONNECTION_NAME}")
DB_USER = "root"
DB_PASS = "1q2w3e"
DB_NAME = "dbhw6"

from google.cloud.sql.connector import Connector
import sqlalchemy

# initialize Connector object
connector = Connector()

# function to return the database connection object
def getconn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )
    return conn

# create connection pool with 'creator' argument to our connection object function
pool = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn,
)
# connect to connection pool
with pool.connect() as db_conn:
  # create ratings table in our sandwiches database
    # db_conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS Request;"))
    # db_conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS Country;"))
    # db_conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS Request_fail;"))
    db_conn.execute(
        sqlalchemy.text(
            "CREATE TABLE IF NOT EXISTS Country ( "
            "country_name VARCHAR(255) UNIQUE NOT NULL, "
            "is_banned BOOLEAN, "
            "PRIMARY KEY (country_name));"

        )
    )
    db_conn.execute(
        sqlalchemy.text(
            "CREATE TABLE IF NOT EXISTS Request ("
            "request_id INT AUTO_INCREMENT NOT NULL,"
            "country_name VARCHAR(255),"
            "client_ip VARCHAR(20) NOT NULL,"
            "gender VARCHAR(20),"
            "age VARCHAR(20),"
            "income VARCHAR(20),"
            "time_of_day TIME,"
            "requested_file VARCHAR(255) NOT NULL,"
            "PRIMARY KEY (request_id),"
            # "UNIQUE KEY (country_id,client_ip,gender,age,income,time_of_day,requested_file),"
            "FOREIGN KEY (country_name) REFERENCES Country(country_name));"     
        )
    )
    db_conn.execute(
        sqlalchemy.text(
            "CREATE TABLE IF NOT EXISTS Request_fail  ("
            "request_id INT AUTO_INCREMENT NOT NULL,"
            "time_of_day TIME,"
            "requested_file VARCHAR(255) NOT NULL,"
            "fail_code VARCHAR(10),"
            "PRIMARY KEY (request_id));"    
        )
    )
    # commit transaction (SQLAlchemy v2.X.X is commit as you go)
    db_conn.commit()
    # insert_stmt = sqlalchemy.text(
    # "INSERT INTO Country ( country_name,is_banned) VALUES (  :name,:banned)",
    # )
    # for i in range(len(list_of_countries)):
    #     if list_of_countries[i] in ['North Korea', 'Iran', 'Cuba', 'Myanmar', 'Iraq', 'Libya', 'Sudan', 'Zimbabwe', 'Syria']:
    #         db_conn.execute(insert_stmt, parameters={"banned": True, "name": list_of_countries[i]})

    #     else:
    #         db_conn.execute(insert_stmt, parameters={ "banned": False, "name": list_of_countries[i]})

    # db_conn.commit()

    import datetime

    now = datetime.datetime.now()

    formatted_time = now.strftime('%Y-%m-%d %H:%M:%S')


    # insert_stmt = sqlalchemy.text(
    # "INSERT IGNORE INTO Request ( country_name,client_ip,gender,age,income,time_of_day,requested_file) VALUES (:country_name,:client_ip,:gender,:age,:income,:time_of_day,:requested_file)",
    # )

    # db_conn.execute(insert_stmt, parameters={"country_name":"North Korea" , "client_ip": "8.8.8.8","gender":list_of_genders[0],"age":list_of_ages[0],"income":list_of_incomes[0],"time_of_day":formatted_time,"requested_file":"01.html"})    
    # db_conn.execute(insert_stmt, parameters={"country_name": "North Korea", "client_ip": "8.8.8.8","gender":list_of_genders[0],"age":list_of_ages[0],"income":list_of_incomes[0],"time_of_day":formatted_time,"requested_file":"01.html"})
    # db_conn.commit()
    # # query and fetch ratings table
    # results = db_conn.execute(sqlalchemy.text("SELECT * FROM Request")).fetchall()

    # # show results
    # for row in results:
    #     print(row)
    
    def resetsql():
        with pool.connect() as db_conn:
            db_conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS Request;"))
            db_conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS Country;"))
            db_conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS Request_fail;"))
            db_conn.execute(
                sqlalchemy.text(
                    "CREATE TABLE Country ( "
                    "country_name VARCHAR(255) UNIQUE NOT NULL, "
                    "is_banned BOOLEAN, "
                    "PRIMARY KEY (country_name));"

                )
            )
            db_conn.execute(
                sqlalchemy.text(
                    "CREATE TABLE Request ("
                    "request_id INT AUTO_INCREMENT NOT NULL,"
                    "country_name VARCHAR(255),"
                    "client_ip VARCHAR(20) NOT NULL,"
                    "gender VARCHAR(20),"
                    "age VARCHAR(20),"
                    "income VARCHAR(20),"
                    "time_of_day TIME,"
                    "requested_file VARCHAR(255) NOT NULL,"
                    "PRIMARY KEY (request_id),"
                    # "UNIQUE KEY (country_id,client_ip,gender,age,income,time_of_day,requested_file),"
                    "FOREIGN KEY (country_name) REFERENCES Country(country_name));"     
                )
            )
            db_conn.execute(
                sqlalchemy.text(
                    "CREATE TABLE Request_fail ("
                    "request_id INT AUTO_INCREMENT NOT NULL,"
                    "time_of_day TIME,"
                    "requested_file VARCHAR(255) NOT NULL,"
                    "fail_code VARCHAR(10),"
                    "PRIMARY KEY (request_id));"    
                )
            )
            # commit transaction (SQLAlchemy v2.X.X is commit as you go)
            db_conn.commit()

def update_request(country_name,client_ip,gender,age,income,time_of_day,requested_file):
    with pool.connect() as db_conn:
        insert_stmt = sqlalchemy.text(
        "INSERT IGNORE INTO Request ( country_name,client_ip,gender,age,income,time_of_day,requested_file) VALUES (:country_name,:client_ip,:gender,:age,:income,:time_of_day,:requested_file)",
        )
        db_conn.execute(insert_stmt, parameters={"country_name":country_name , "client_ip": client_ip,"gender":gender,"age":age,"income":income,"time_of_day":time_of_day,"requested_file":requested_file})    
        db_conn.commit()




def update_request_fail(time_of_day,requested_file,fail_code):
    with pool.connect() as db_conn:
        insert_stmt = sqlalchemy.text(
        "INSERT IGNORE INTO Request_fail (time_of_day,requested_file,fail_code) VALUES (:time_of_day,:requested_file,:fail_code)",
        )
        db_conn.execute(insert_stmt, parameters={"time_of_day":time_of_day,"requested_file":requested_file,"fail_code":fail_code})    
        db_conn.commit()

def get_request_fail():
    with pool.connect() as db_conn:

        results = db_conn.execute(sqlalchemy.text("SELECT * FROM Request_fail")).fetchall()

        # show results
        return results
def get_request():
    with pool.connect() as db_conn:

        results = db_conn.execute(sqlalchemy.text("SELECT * FROM Request")).fetchall()

        # show results
        return results
def get_request_engine():
    with pool.connect() as db_conn:

        results = db_conn.execute(sqlalchemy.text("SELECT * FROM Request")).fetchall()

        # show results
        return results,pool
