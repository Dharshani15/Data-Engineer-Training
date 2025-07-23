//Creating DB
use EmployeeDB;

//Inserting Task Feedback
db.taskFeedback.insertMany([
  {
    employee_id: 2,
    feedback_date: "2024-01-17",
    feedback: "Resolved server downtime quickly, excellent job.",
    task_id: 1
  },
  {
    employee_id: 3,
    feedback_date: "2024-01-17",
    feedback: "Financial report needs more detailed analysis.",
    task_id: 2
  },
  {
    employee_id: 4,
    feedback_date: "2024-01-18",
    feedback: "Marketing campaign design was creative and effective.",
    task_id: 3
  },
  {
    employee_id: 5,
    feedback_date: "2025-03-12",
    feedback: "Workstation setup pending due to hardware delays.",
    task_id: 4
  },
  {
    employee_id: 1,
    feedback_date: "2023-07-03",
    feedback: "Employee training conducted smoothly.",
    task_id: 5
  },
  {
    employee_id: 6,
    feedback_date: "2025-03-13",
    feedback: "Screening candidates with good potential.",
    task_id: 6
  },
  {
    employee_id: 7,
    feedback_date: "2023-12-07",
    feedback: "Payroll data updated successfully.",
    task_id: 7
  }
]);

// Create indexes 
db.taskFeedback.createIndex({ employee_id: 1 });
db.taskFeedback.createIndex({ feedback_date: 1 });
