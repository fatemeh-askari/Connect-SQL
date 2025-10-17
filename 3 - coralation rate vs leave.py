from db_connect import get_connection
import pandas as pd
from sklearn.linear_model import LinearRegression
from scipy.stats import pearsonr 
import numpy as np
from datetime import datetime
import pyodbc


conn = get_connection()

query = """
SELECT 
    e.BusinessEntityID,
    e.JobTitle,
    e.VacationHours,
    e.SickLeaveHours,
    e.HireDate,
    p.Rate,
    p.RateChangeDate
FROM HumanResources.Employee e
JOIN HumanResources.EmployeePayHistory p
    ON e.BusinessEntityID = p.BusinessEntityID
WHERE p.RateChangeDate = (
    SELECT MAX(RateChangeDate)
    FROM HumanResources.EmployeePayHistory p2
    WHERE p2.BusinessEntityID = e.BusinessEntityID
);
"""

df = pd.read_sql(query, conn)
conn.close()


df['total_leave'] = df['VacationHours'] + df['SickLeaveHours']
X = df[['Rate']]
y = df['total_leave']


model = LinearRegression()
model.fit(X, y)

slope = model.coef_[0]
intercept = model.intercept_
r2 = model.score(X, y)



r_value, p_value = pearsonr(df['Rate'], df['total_leave'])

print("ضریب همبستگی (r):", r_value)
print("مقدار معنی‌داری (sig):", p_value)
print("ضریب رگرسیون (Slope):", slope)
print("عرض از مبدا (Intercept):", intercept)
print("R²:", r2)

if p_value < 0.05:
    print(" فرض H0 رد می‌شود → رابطه معنی‌دار است.")
else:
    print(" فرض H0 پذیرفته می‌شود → رابطه معنی‌دار نیست.")


conn = get_connection()
cursor = conn.cursor()


create_table_query = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ExpertAnalysis' AND xtype='U')
BEGIN
    CREATE TABLE HumanResources.ExpertAnalysis (
        MetricID INT IDENTITY(1,1) PRIMARY KEY,
        MetricName NVARCHAR(200) UNIQUE,
        Value FLOAT,
        Description NVARCHAR(500),
        Method NVARCHAR(100),
        UpdateTime DATETIME DEFAULT GETDATE()
    )
END
"""
cursor.execute(create_table_query)
conn.commit()


metrics = []

# --- Correlation (r) ---
if abs(r_value) < 0.1:
    corr_desc = "رابطه خطی بین حقوق و مجموع مرخصی تقریبا وجود ندارد"
elif abs(r_value) < 0.3:
    corr_desc = "رابطه خطی ضعیف بین حقوق و مجموع مرخصی وجود دارد"
elif abs(r_value) < 0.5:
    corr_desc = "رابطه خطی متوسط بین حقوق و مجموع مرخصی وجود دارد"
else:
    corr_desc = "رابطه خطی قوی بین حقوق و مجموع مرخصی وجود دارد"

metrics.append({
    "MetricName": "Correlation Rate vs Total Leave",
    "Value": r_value,
    "Description": f"ضریب همبستگی پیرسون = {r_value:.3f}. {corr_desc}",
    "Method": "Pearson correlation"
})

# --- p-value ---
if p_value < 0.05:
    sig_desc = "با توجه به سطح معنی داری پنج صدم فرض اچ صفر رد می‌شود؛ رابطه معنی‌دار است"
else:
    sig_desc = " با توجه به سطح معنی داری پنج صدم فرض اچ صفر پذیرفته می‌شود؛ رابطه معنی‌دار نیست"

metrics.append({
    "MetricName": "Significance (p-value) Rate vs Total Leave",
    "Value": p_value,
    "Description": f"p-value = {p_value:.3f}. {sig_desc}",
    "Method": "Statistical significance test"
})

# --- Slope ---
if abs(slope) < 0.1:
    slope_desc = "شیب رگرسیون نزدیک صفر است؛ تغییر حقوق تأثیر قابل توجهی روی مرخصی ندارد"
elif slope > 0:
    slope_desc = "شیب مثبت؛ افزایش حقوق با افزایش مرخصی همراه است"
else:
    slope_desc = "شیب منفی؛ افزایش حقوق با کاهش مرخصی همراه است"

metrics.append({
    "MetricName": "Regression Slope Rate vs Total Leave",
    "Value": slope,
    "Description": f"شیب رگرسیون = {slope:.3f}. {slope_desc}",
    "Method": "Linear Regression"
})

# --- R² ---
if r2 < 0.1:
    r2_desc = "بسیار کم است؛ حقوق تقریباً هیچ توضیحی درباره مرخصی نمی‌دهد"
elif r2 < 0.3:
    r2_desc = "کم است؛ حقوق اندکی می‌تواند مرخصی را توضیح دهد"
elif r2 < 0.5:
    r2_desc = "متوسط است؛ بخشی از تغییرات مرخصی توسط حقوق توضیح داده می‌شود"
else:
    r2_desc = "بالا است؛ تغییرات مرخصی تا حد زیادی توسط حقوق توضیح داده می‌شود"

metrics.append({
    "MetricName": "Regression R2 Rate vs Total Leave",
    "Value": r2,
    "Description": f"ضریب تعیین R² = {r2:.3f}. {r2_desc}",
    "Method": "Linear Regression"
})




for metric in metrics:
    upsert_query = """
    MERGE HumanResources.ExpertAnalysis AS target
    USING (SELECT ? AS MetricName) AS source
    ON target.MetricName = source.MetricName
    WHEN MATCHED THEN 
        UPDATE SET Value = ?, Description = ?, Method = ?, UpdateTime = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (MetricName, Value, Description, Method, UpdateTime)
        VALUES (?, ?, ?, ?, GETDATE());
    """
    cursor.execute(upsert_query,
                   metric["MetricName"], metric["Value"], metric["Description"], metric["Method"],
                   metric["MetricName"], metric["Value"], metric["Description"], metric["Method"])

conn.commit()
cursor.close()
conn.close()

print("SAVED RESULTS!!!")

