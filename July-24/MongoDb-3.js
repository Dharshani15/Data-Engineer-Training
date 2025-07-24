use jobs

db.jobs.insertMany([
  { job_id: 1, title: "Software Engineer", company: "TechNova", location: "Bangalore", salary: 1200000, job_type: "remote", posted_on: new Date("2025-07-10") },
  { job_id: 2, title: "Data Analyst", company: "DataVibe", location: "Mumbai", salary: 850000, job_type: "hybrid", posted_on: new Date("2024-05-15") },
  { job_id: 3, title: "Frontend Developer", company: "WebCraft", location: "Chennai", salary: 950000, job_type: "on-site", posted_on: new Date("2023-11-20") },
  { job_id: 4, title: "DevOps Engineer", company: "InfraHub", location: "Hyderabad", salary: 1100000, job_type: "remote", posted_on: new Date("2025-07-01") },
  { job_id: 5, title: "ML Engineer", company: "TechNova", location: "Pune", salary: 1500000, job_type: "remote", posted_on: new Date("2025-06-30") }
])

db.applicants.insertMany([
  { applicant_id: 101, name: "Ananya Reddy", skills: ["Python", "MongoDB", "Flask"], experience: 2, city: "Hyderabad", applied_on: new Date("2025-07-02") },
  { applicant_id: 102, name: "Ravi Kumar", skills: ["Java", "Spring", "SQL"], experience: 4, city: "Delhi", applied_on: new Date("2024-09-12") },
  { applicant_id: 103, name: "Priya Sharma", skills: ["MongoDB", "React", "Node.js"], experience: 3, city: "Mumbai", applied_on: new Date("2025-06-28") },
  { applicant_id: 104, name: "Mohit Sinha", skills: ["C++", "Python", "Linux"], experience: 5, city: "Hyderabad", applied_on: new Date("2023-10-22") },
  { applicant_id: 105, name: "Neha Agarwal", skills: ["Excel", "Tableau", "SQL"], experience: 1, city: "Chennai", applied_on: new Date("2025-07-03") }
])

db.applications.insertMany([
  { applicant_id: 101, job_id: 1, application_status: "interview scheduled", interview_scheduled: true, feedback: "Strong backend skills" },
  { applicant_id: 101, job_id: 4, application_status: "applied", interview_scheduled: false, feedback: "" },
  { applicant_id: 102, job_id: 2, application_status: "rejected", interview_scheduled: false, feedback: "Mismatch in skillset" },
  { applicant_id: 103, job_id: 1, application_status: "interview scheduled", interview_scheduled: true, feedback: "Excellent frontend experience" },
  { applicant_id: 105, job_id: 2, application_status: "applied", interview_scheduled: false, feedback: "" }
])


//1. Find all remote jobs with a salary greater than 10,00,000.
db.jobs.find({ job_type: "remote",salary: { $gt: 1000000 }})

//2. Get all applicants who know MongoDB.
db.applicants.find({skills: "MongoDB"})

//3. Show the number of jobs posted in the last 30 days.
db.jobs.countDocuments({posted_on: { $gte: new Date(new Date().setDate(new Date().getDate() - 30)) }})

//4. List all job applications that are in ‘interview scheduled’ status.
db.applications.find({ application_status: "interview scheduled"})

//5. Find companies that have posted more than 2 jobs.
db.jobs.aggregate([
  { $group: { _id: "$company", jobCount: { $sum: 1 } } },
  { $match: { jobCount: { $gt: 2 } } }])

//6. Join applications with jobs to show job title along with the applicant’s name.
db.applications.aggregate([
  {
    $lookup: {
      from: "jobs",
      localField: "job_id",
      foreignField: "job_id",
      as: "job_info"
    }
  },
  {
    $lookup: {
      from: "applicants",
      localField: "applicant_id",
      foreignField: "applicant_id",
      as: "applicant_info"
    }
  },
  {
    $project: {
      _id: 0,
      job_title: { $arrayElemAt: ["$job_info.title", 0] },
      applicant_name: { $arrayElemAt: ["$applicant_info.name", 0] },
      application_status: 1
    }
  }
])

//7. Find how many applications each job has received.
db.applications.aggregate([
  { $group: { _id: "$job_id", total_applications: { $sum: 1 } } },
  {
    $lookup: {
      from: "jobs",
      localField: "_id",
      foreignField: "job_id",
      as: "job_info"
    }
  },
  {
    $project: {
      job_title: { $arrayElemAt: ["$job_info.title", 0] },
      total_applications: 1
    }
  }
])

//8. List applicants who have applied for more than one job.
db.applications.aggregate([
  { $group: { _id: "$applicant_id", count: { $sum: 1 } } },
  { $match: { count: { $gt: 1 } } },
  {
    $lookup: {
      from: "applicants",
      localField: "_id",
      foreignField: "applicant_id",
      as: "applicant_info"
    }
  },
  {
    $project: {
      name: { $arrayElemAt: ["$applicant_info.name", 0] },
      job_applications: "$count"
    }
  }
])

//9. Show the top 3 cities with the most applicants.
db.applicants.aggregate([
  { $group: { _id: "$city", total: { $sum: 1 } } },
  { $sort: { total: -1 } },
  { $limit: 3 }
])

//10. Get the average salary for each job type (remote, hybrid, on-site).
db.jobs.aggregate([
  {
    $group: {
      _id: "$job_type",
      average_salary: { $avg: "$salary" }
    }
  }
])

//11. Update the status of one application to "offer made".
db.applications.updateOne(
  { application_status: "interview scheduled" },
  { $set: { application_status: "offer made" } }
)

//12. Delete a job that has not received any applications.
db.jobs.deleteOne({
  job_id: { $nin: db.applications.distinct("job_id") }
})

//13. Add a new field shortlisted to all applications and set it to false.
db.applications.updateMany(
  {},
  { $set: { shortlisted: false } }
)

//14. Increment experience of all applicants from "Hyderabad" by 1 year.
db.applicants.updateMany(
  { city: "Hyderabad" },
  { $inc: { experience: 1 } }
)

//15. Remove all applicants who haven’t applied to any job.
db.applicants.deleteMany({
  applicant_id: { $nin: db.applications.distinct("applicant_id") }
})
