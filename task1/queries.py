from mongo_connect import connectMongo
import pymongo
import json
import pprint

# Load in data
dummy_fitness = json.loads(open('dummy-fitness.json').read())
initial = json.loads(open('initial.json').read())
user1001_new = json.loads(open('user1001-new.json').read())
collection = connectMongo()


# Reset DBs

collection.delete_many({})
collection.insert_many(initial)

total_employees = collection.find().sort("uid")
print("Initial Employees :")
for employee in total_employees:
    pprint.pprint(employee)

# ------------------------------------------------------------------------------------------------------------------
print("")
#WQ1. Add the data in dummy-fitness.json to the MongoDB database
collection.insert_many(dummy_fitness)
total_employees = collection.find().sort("uid")
print("Given Data Inserted:")
for emp in total_employees:
    pprint.pprint(emp)

print("")

#WQ2. Update the database with data from user1001-new.json.
collection.update_one(
    {"uid": user1001_new["uid"]},
    {"$set": user1001_new}
)

total_employees = collection.find().sort("uid")
print("Employee Data after updating it")
for emp in total_employees:
    pprint.pprint(emp)
print("")

# RQ1. Count the number of employees whose data is in the AggieFit database.
print("Total number of users:" + str(collection.count_documents({})))
print("")

# RQ2. Retrieve employees who have been tagged as "irregular".
print("Irregular Employees:")
irregular_employees = collection.find(
    {"tags": {"$all": ["irregular"]}}
).sort("uid")

for emp in irregular_employees:
    pprint.pprint(emp)
print("")

# RQ3. Retrieve employees that have a goal step count less than or equal to 1500 steps.
print("Employees with a Step Count Goal Less Than 1500:")
Step_1500 = collection.find(
    {"goal.stepGoal": {"$lte": 1500}}).sort("uid")

for emp in Step_1500:
    pprint.pprint(emp)

print("")

# RQ4. Aggregate the total activity duration for each employee. If the employee does not
#      have activity duration in their data, you can report their total activity duration as 0.
print("Total  Activity:")
activity_dur = collection.aggregate([
    {
        "$project": {
            "_id": "$uid",
            "totalDuration": {"$sum": "$activityDuration"}
        }
    }
])

for employee in activity_dur:
    pprint.pprint(employee)

