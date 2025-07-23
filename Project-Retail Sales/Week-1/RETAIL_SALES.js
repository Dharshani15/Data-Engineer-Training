//Creating DB
use retail

//Inserting Values or storing campaign feedback
db.campaignFeedback.insertMany([
  {
    campaign_id: 1,
    product_id: 1,
    region: "West",
    feedback: "Excellent response in Mumbai",
    effectiveness_score: 9,
    date: ISODate("2024-06-01")
  },
  {
    campaign_id: 2,
    product_id: 2,
    region: "South",
    feedback: "Average turnout",
    effectiveness_score: 6,
    date: ISODate("2024-07-10")
  },
  {
    campaign_id: 3,
    product_id: 3,
    region: "North",
    feedback: "Sales dropped slightly",
    effectiveness_score: 4,
    date: ISODate("2024-08-15")
  },
  {
    campaign_id: 4,
    product_id: 4,
    region: "East",
    feedback: "Very successful with high demand",
    effectiveness_score: 10,
    date: ISODate("2025-01-01")
  }
])

//Creating Indexes

db.campaignFeedback.createIndex({ product_id: 1 })
db.campaignFeedback.createIndex({ region: 1 })


















