import os
import datetime as dt
import numpy as np
import pandas as pd

DATA_DIR = "data"
REPORTS_DIR = "reports"
ATTENDANCE_FILE = os.path.join(DATA_DIR, "cleaned_attendance.csv")
TASK_FILE = os.path.join(DATA_DIR, "cleaned_task.csv")
EMP_FILE = os.path.join(DATA_DIR, "cleaned_employee.csv")

LATE_AFTER = "08:15:00"  # late login threshold
os.makedirs(REPORTS_DIR, exist_ok=True)

today_str = dt.datetime.utcnow().strftime("%Y-%m-%d")

attendance = pd.read_csv(ATTENDANCE_FILE)
tasks = pd.read_csv(TASK_FILE)
employees = pd.read_csv(EMP_FILE)

attendance["absentee_flag"] = np.where(attendance["clock_in"].isna() | attendance["clock_out"].isna(), 1, 0)


attendance["work_hours"] = np.where(
    attendance["absentee_flag"] == 0,
    (attendance["clock_out"] - attendance["clock_in"]).dt.total_seconds() / 3600.0,
    0.0
)
late_threshold = pd.to_datetime(LATE_AFTER, format="%H:%M:%S", errors="coerce").time()
attendance["late_login_flag"] = np.where(
    attendance["absentee_flag"] == 0,
    attendance["clock_in"].dt.time > late_threshold,
    False
).astype(int)


if "status" in tasks.columns:
    tasks["tasks_completed"] = np.where(tasks["status"].astype(str).str.lower() == "completed".lower(), 1, 0)
else:
    tasks["tasks_completed"] = 0

df = attendance.merge(tasks[["employee_id", "tasks_completed"]], on="employee_id", how="left")
df = df.merge(employees, on="employee_id", how="left")


df["tasks_completed"] = df["tasks_completed"].fillna(0).astype(int)
df["work_hours"] = df["work_hours"].fillna(0.0)


df["productivity_score"] = np.where(df["work_hours"] > 0, df["tasks_completed"] / df["work_hours"], 0.0)


dept_kpis = ( df.groupby("department", dropna=False)
      .agg(
          avg_work_hours=("work_hours", "mean"),
          avg_productivity_score=("productivity_score", "mean"),
          late_logins_count=("late_login_flag", "sum"),
          absentees_count=("absentee_flag", "sum"),
          total_tasks_completed=("tasks_completed", "sum")
      )
      .reset_index()
)


dept_kpis["avg_work_hours"] = dept_kpis["avg_work_hours"].round(2)
dept_kpis["avg_productivity_score"] = dept_kpis["avg_productivity_score"].round(2)

absences_by_emp = (
    df.groupby(["employee_id"], dropna=False)
      .agg(
          total_absences=("absentee_flag", "sum"),
          department=("department", "first"),
          first_name=("first_name", "first"),
          last_name=("last_name", "first"),
      )
      .reset_index()
      .sort_values("total_absences", ascending=False)
      .head(5)
)

lowest_depts = (
    dept_kpis.sort_values("avg_productivity_score", ascending=True)
             .head(5)
)

]
