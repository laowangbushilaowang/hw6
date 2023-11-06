import pandas as pd
import sqlfunction
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn import svm

# import data
data,engine=sqlfunction.get_request_engine()
# df = pd.read_sql(data,engine)
df=pd.DataFrame(list(data))
#data process
print(df.shape)

# encode
le = LabelEncoder()
df[['ip1','ip2','ip3','ip4']]=df['client_ip'].str.split(".",expand=True)
df['country_name_encoded'] = le.fit_transform(df['country_name'])
df['ip1'] = df['ip1'].astype(int)
df['ip2'] = df['ip2'].astype(int)
df['ip3'] = df['ip3'].astype(int)
df['ip4'] = df['ip4'].astype(int)
# dataset
X = df[['ip1','ip2','ip3','ip4']]
y = df['country_name_encoded']
print(X)
print(y)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# model
clf = RandomForestClassifier()
clf.fit(X_train, y_train)

# pred
y_pred = clf.predict(X_test)

# eval
accuracy = accuracy_score(y_test, y_pred)
print(f"Model accuracy: {accuracy * 100:.2f}%")
