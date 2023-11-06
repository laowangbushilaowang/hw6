import pandas as pd
import sqlfunction
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
from sklearn import svm

# import data
data,engine=sqlfunction.get_request_engine()
# df = pd.read_sql(data,engine)
df=pd.DataFrame(list(data))
#data process
print(df.shape)

# encode
le = LabelEncoder()
df['country_name_encoded'] = le.fit_transform(df['country_name'])
df['gender_encoded'] = le.fit_transform(df['gender'])
df['age_encoded'] = le.fit_transform(df['age'])
# dataset
X = df[['country_name_encoded','gender_encoded','age_encoded']]
y = df['income']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# model
clf = svm.SVC()
clf.fit(X_train, y_train)

# pred
y_pred = clf.predict(X_test)

# eval
accuracy = accuracy_score(y_test, y_pred)
print(f"Model accuracy: {accuracy * 100:.2f}%")
