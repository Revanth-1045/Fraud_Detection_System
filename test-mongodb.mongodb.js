// MongoDB CRUD Operations Scenarios
// 1. Hospital Management System
// 2. Library Management System
// 3. Twitter Dataset

// ==========================================
// SCENARIO 1: HOSPITAL MANAGEMENT SYSTEM
// ==========================================
use('hospital_db');

// 1.1 Create Collections & Insert Data (Create)
// Patients
db.patients.drop(); // Clear existing
db.patients.insertMany([
    { _id: 1, name: "John Doe", age: 45, ailment: "Flu", doctor_id: 101, admission_date: new Date("2023-10-01") },
    { _id: 2, name: "Jane Smith", age: 30, ailment: "Fracture", doctor_id: 102, admission_date: new Date("2023-10-05") },
    { _id: 3, name: "Alice Brown", age: 60, ailment: "Diabetes", doctor_id: 101, admission_date: new Date("2023-09-20") }
]);

// Doctors
db.doctors.drop();
db.doctors.insertMany([
    { _id: 101, name: "Dr. House", specialty: "Diagnostician" },
    { _id: 102, name: "Dr. Wilson", specialty: "Oncology" }
]);

// Appointments
db.appointments.drop();
db.appointments.insertMany([
    { patient_id: 1, doctor_id: 101, date: new Date("2023-10-10"), status: "Scheduled" },
    { patient_id: 2, doctor_id: 102, date: new Date("2023-10-12"), status: "Scheduled" }
]);

// 1.2 Queries (Read)
// Find all patients with age > 40
console.log("Patients over 40:");
const patientsOver40 = db.patients.find({ age: { $gt: 40 } }).toArray();
console.log(patientsOver40);

// Find patients assigned to Dr. House (101)
console.log("Patients of Dr. House:");
const housesPatients = db.patients.find({ doctor_id: 101 }).toArray();
console.log(housesPatients);

// 1.3 Updates (Update)
// Update John Doe's ailment
db.patients.updateOne({ name: "John Doe" }, { $set: { ailment: "Recovered Flu" } });

// 1.4 Deletes (Delete)
// Discharge (delete) Alice Brown
db.patients.deleteOne({ name: "Alice Brown" });


// ==========================================
// SCENARIO 2: LIBRARY MANAGEMENT SYSTEM
// ==========================================
use('library_db');

// 2.1 Create Collections & Insert Data
// Books
db.books.drop();
db.books.insertMany([
    { isbn: "978-0131103627", title: "C Programming Language", author: "Kernighan & Ritchie", copies: 5 },
    { isbn: "978-0132350884", title: "Clean Code", author: "Robert C. Martin", copies: 3 },
    { isbn: "978-0201633610", title: "Design Patterns", author: "Gamma et al.", copies: 2 }
]);

// Members
db.members.drop();
db.members.insertMany([
    { member_id: "M001", name: "Hermione Granger", joint_date: new Date("2023-01-01") },
    { member_id: "M002", name: "Ron Weasley", joint_date: new Date("2023-02-15") }
]);

// 2.2 Queries
// Find books by Robert C. Martin
console.log("Books by Robert C. Martin:");
const uncleBobBooks = db.books.find({ author: "Robert C. Martin" }).toArray();
console.log(uncleBobBooks);

// Check availability of 'Design Patterns'
console.log("Copies of Design Patterns:");
const designPatternCopies = db.books.find({ title: "Design Patterns" }, { copies: 1, _id: 0 }).toArray();
console.log(designPatternCopies);

// 2.3 Updates
// Borrow a book (decrease copies)
db.books.updateOne({ title: "Clean Code" }, { $inc: { copies: -1 } });

// 2.4 Deletes
// Remove a lost book
db.books.deleteOne({ isbn: "978-0131103627" });


// ==========================================
// SCENARIO 3: TWITTER DATASET
// ==========================================
use('twitter_db');

// 3.1 Create Collections & Insert Data
// Users
db.users.drop();
db.users.insertMany([
    { user_handle: "@tech_guru", name: "Tech Guru", followers: 5000 },
    { user_handle: "@news_daily", name: "Daily News", followers: 12000 }
]);

// Tweets
db.tweets.drop();
db.tweets.insertMany([
    { _id: 1, user_handle: "@tech_guru", content: "MongoDB is awesome! #database #nosql", likes: 10, retweets: 2, timestamp: new Date() },
    { _id: 2, user_handle: "@news_daily", content: "Breaking: AI takes over the world. #ai #tech", likes: 500, retweets: 200, timestamp: new Date() },
    { _id: 3, user_handle: "@tech_guru", content: "Learning Spark next. #bigdata", likes: 5, retweets: 0, timestamp: new Date() }
]);

// 3.2 Queries
// Search tweets with hashtags #tech
console.log("Tweets with #tech:");
const techTweets = db.tweets.find({ content: /#tech/ }).toArray();
console.log(techTweets);

// Get timeline for @tech_guru
console.log("Timeline for @tech_guru:");
const guruTimeline = db.tweets.find({ user_handle: "@tech_guru" }).sort({ timestamp: -1 }).toArray();
console.log(guruTimeline);

// 3.3 Updates
// Like a tweet (increment likes)
db.tweets.updateOne({ _id: 1 }, { $inc: { likes: 1 } });

// 3.4 Deletes
// Delete a controversial tweet
db.tweets.deleteOne({ _id: 2 });

// Final Verification
show dbs;

