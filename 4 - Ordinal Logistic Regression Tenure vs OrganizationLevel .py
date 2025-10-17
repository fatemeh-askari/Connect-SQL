from db_connect import get_connection
import pandas as pd
from datetime import datetime
import pyodbc
from mord import LogisticIT  
from scipy.stats import spearmanr


conn = get_connection()

query = """
SELECT 
    BusinessEntityID,
    HireDate,
    OrganizationLevel
FROM HumanResources.Employee;
"""

df_employee = pd.read_sql(query, conn)
conn.close()


print("قبل از حذف NaN ها:")
print(df_employee[['OrganizationLevel', 'HireDate']].isna().sum())

df_employee = df_employee.dropna(subset=['OrganizationLevel', 'HireDate'])

print("بعد از حذف NaN ها:")
print(df_employee[['OrganizationLevel', 'HireDate']].isna().sum())


df_employee['HireDate'] = pd.to_datetime(df_employee['HireDate'])
df_employee['Tenure_years'] = (datetime.now() - df_employee['HireDate']).dt.days / 365


X = df_employee[['Tenure_years']]
y = df_employee['OrganizationLevel'].astype(int)

model = LogisticIT()
model.fit(X, y)

coef = model.coef_[0]
thresholds = model.theta_
predicted = model.predict(X)

spearman_corr, pval = spearmanr(df_employee['Tenure_years'], df_employee['OrganizationLevel'])
accuracy = (predicted == y).mean()

print("Spearman correlation:", spearman_corr)
print("P-value:", pval)
print("Coefficient (Tenure effect):", coef)
print("Model Accuracy:", accuracy)


conn = get_connection()
cursor = conn.cursor()

create_table_query = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ExpertAnalysis' AND xtype='U')
BEGIN
    CREATE TABLE HumanResources.ExpertAnalysis (
        MetricID INT IDENTITY(1,1) PRIMARY KEY,
        MetricName NVARCHAR(200) UNIQUE,
        Value FLOAT,
        Description NVARCHAR(1000),
        Method NVARCHAR(100),
        UpdateTime DATETIME DEFAULT GETDATE()
    )
END
"""
cursor.execute(create_table_query)
conn.commit()


metrics = [
    {
        "MetricName": "Spearman Correlation Tenure vs OrganizationLevel",
        "Value": spearman_corr,
        "Method": "Spearman Rank Correlation"
    },
    {
        "MetricName": "Ordinal Regression Coefficient",
        "Value": coef,
        "Method": "Ordinal Logistic Regression"
    },
    {
        "MetricName": "Ordinal Regression Accuracy",
        "Value": accuracy,
        "Method": "Ordinal Logistic Regression"
    }
]

for metric in metrics:
    if "Correlation" in metric["MetricName"]:
        if abs(metric["Value"]) < 0.1:
            description = (
                "شاخص همبستگی رتبه‌ای اسپیرمن مقدار {:.3f} دارد. "
                "این مقدار نشان می‌دهد بین سابقه کاری و سطح سازمانی رابطه معنی‌داری وجود ندارد، "
                "و تغییر سطح سازمانی صرفا با سابقه توضیح داده نمی‌شود."
            ).format(metric["Value"])
        else:
            direction = "مثبت" if metric["Value"] > 0 else "منفی"
            description = (
                "شاخص همبستگی اسپیرمن {:.3f} است که نشان‌دهنده رابطه {} بین سابقه کاری و سطح سازمانی است. "
                "به‌طور کلی کارکنانی که سابقه بیشتری دارند، تمایل به ارتقای سطح سازمانی بیشتری دارند."
            ).format(metric["Value"], direction)

    elif "Coefficient" in metric["MetricName"]:
        if metric["Value"] > 0:
            description = (
                "ضریب رگرسیون ترتیبی برابر {:.3f} است و نشان می‌دهد افزایش سابقه کاری "
                "احتمال ارتقا به سطح بالاتر سازمانی را کمی افزایش می‌دهد. "
                "با این حال شدت اثر متوسط و نیازمند بررسی متغیرهای تکمیلی است."
            ).format(metric["Value"])
        else:
            description = (
                "ضریب رگرسیون ترتیبی برابر {:.3f} است که نشان می‌دهد بین سابقه کاری و احتمال ارتقا "
                "رابطه معکوس ضعیفی وجود دارد، هرچند ممکن است از نظر آماری معنی‌دار نباشد."
            ).format(metric["Value"])

    elif "Accuracy" in metric["MetricName"]:
        if metric["Value"] >= 0.7:
            description = (
                "مدل ترتیبی دقت {:.3f} دارد که نشان می‌دهد پیش‌بینی سطح سازمانی بر اساس سابقه کاری "
                "با دقت نسبتا بالایی انجام می‌شود."
            ).format(metric["Value"])
        elif metric["Value"] >= 0.6:
            description = (
                "دقت مدل ترتیبی {:.3f} است؛ این مقدار اندکی بالاتر از حد تصادفی بوده و "
                "نشان می‌دهد سابقه کاری اثر محدودی بر پیش‌بینی ارتقا دارد."
            ).format(metric["Value"])
        else:
            description = (
                "دقت مدل ترتیبی {:.3f} است که پایین محسوب می‌شود؛ "
                "بنابراین سابقه کاری به تنهایی شاخص مناسبی برای پیش‌بینی ارتقا نیست."
            ).format(metric["Value"])
    else:
        description = "تحلیل تکمیلی"


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
    cursor.execute(
        upsert_query,
        metric["MetricName"], metric["Value"], description, metric["Method"],
        metric["MetricName"], metric["Value"], description, metric["Method"]
    )


conn.commit()
cursor.close()
conn.close()

print("SAVED RESULTS!!!")
