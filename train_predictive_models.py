import os
import pandas as pd
from deltalake import DeltaTable
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import accuracy_score, mean_absolute_error, classification_report

print("🦁 Step 1: Loading feature data from the Gold Layer...")
# 1. Read your live Gold Delta Table into a Pandas DataFrame

try:
    gold_table_path = "storage/gold_fault_features"
    dt = DeltaTable(gold_table_path)
    df = dt.to_pandas()
except Exception as e:
    print(f"❌ Error loading Gold Table: {e}")
    print("💡 Make sure your simulator and streaming engines have generated data first!")
    exit()

if df.empty:
    print("❌ The Gold table is currently empty. Generate some stream data first!")
    exit()

print(f"✅ Successfully loaded {len(df)} records from the Gold Layer.")

print("\n⚙️ Step 2: Engineering targets for Classification & Regression...")
# 2. Synthetic labeling for local training:
# Classification Target: 1 if vibration anomaly or avg temp is high, else 0
df['is_failing'] = ((df['vibration_anomaly_score'] > 1.5) | (df['avg_engine_temp'] > 95.0)).astype(int)

# Regression Target: Remaining Useful Life (RUL) in hours 
# Mocking RUL inversely proportional to the current anomaly score and temperature
df['remaining_useful_life'] = (100 - (df['avg_engine_temp'] * 0.5) - (df['vibration_anomaly_score'] * 10)).clip(lower=0)

# Define our Features (X) and our separate Targets (y)
feature_cols = ['avg_engine_temp', 'vibration_anomaly_score']
X = df[feature_cols]
y_class = df['is_failing']
y_reg = df['remaining_useful_life']

# Split data into 80% Training and 20% Testing
X_train, X_test, y_train_class, y_test_class = train_test_split(X, y_class, test_size=0.2, random_state=42)
_, _, y_train_reg, y_test_reg = train_test_split(X, y_reg, test_size=0.2, random_state=42)

print("\n🤖 Step 3: Training the Classification Model (Will it break?)...")
# Initialize and train the Random Forest Classifier
clf = RandomForestClassifier(n_estimators=100, random_state=42)
clf.fit(X_train, y_train_class)
class_preds = clf.predict(X_test)

# Evaluate Classification
accuracy = accuracy_score(y_test_class, class_preds)
print(f"🎯 Classification Model Accuracy: {accuracy * 100:.2f}%")
print("\n📋 Detailed Classification Report:")
print(classification_report(y_test_class, class_preds))

print("\n📉 Step 4: Training the Regression Model (When will it break?)...")
# Initialize and train the Random Forest Regressor
reg = RandomForestRegressor(n_estimators=100, random_state=42)
reg.fit(X_train, y_train_reg)
reg_preds = reg.predict(X_test)

# Evaluate Regression
mae = mean_absolute_error(y_test_reg, reg_preds)
print(f"⏱️ Regression Mean Absolute Error: {mae:.2f} hours")
print("ℹ️ (This means our RUL predictions are off by an average of only +/- hours!)")

print("\n🎉 Both models have been successfully trained and evaluated against live Gold data!")