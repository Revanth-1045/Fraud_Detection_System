# MongoDB VS Code Extension - Connection Guide

## ✅ Extension Status
The **MongoDB for VS Code** extension is already installed.

## How to Connect to MongoDB

### Method 1: Using the MongoDB Extension (Recommended)

1. **Open the MongoDB View**:
   - Click on the MongoDB icon in the VS Code sidebar (looks like a leaf)
   - OR press `Ctrl+Shift+P` and type "MongoDB: Focus on MongoDB View"

2. **Add New Connection**:
   - Click the "+" button next to "Connections"
   - Choose "Connect with Connection String"

3. **Enter Connection String**:
   ```
   mongodb://admin:admin123@localhost:27017/?authSource=admin
   ```

4. **Save the Connection**:
   - Give it a name like "Local MongoDB"
   - Click "Connect"

### Method 2: Advanced Connection Settings

If you prefer to configure manually:

1. Click "+" next to Connections
2. Choose "Advanced Connection Settings"
3. Fill in:
   - **Hostname**: `localhost`
   - **Port**: `27017`
   - **Authentication**: Username/Password
   - **Username**: `admin`
   - **Password**: `admin123`
   - **Authentication Database**: `admin`

## Using MongoDB in VS Code

Once connected, you can:

### 1. Browse Databases
- Expand the connection in the sidebar
- View all databases and collections

### 2. Run Queries
- Right-click on a collection → "View Documents"
- Create a new MongoDB Playground (`Ctrl+Shift+P` → "MongoDB: Create MongoDB Playground")

### 3. Create MongoDB Playground Files

Example playground (`test.mongodb.js`):
```javascript
// Select the database to use
use('testdb');

// Insert a document
db.users.insertOne({
  name: "John Doe",
  email: "john@example.com",
  age: 30
});

// Find documents
db.users.find({ age: { $gte: 18 } });

// Update a document
db.users.updateOne(
  { name: "John Doe" },
  { $set: { age: 31 } }
);

// Delete a document
db.users.deleteOne({ name: "John Doe" });
```

To run the playground:
- Click the "▶ Play Button" at the top right
- Or right-click → "Run All"

## Quick Test

Let's verify your connection with a simple test:

1. Create a new file: `test-connection.mongodb.js`
2. Add this code:
```javascript
// Switch to test database
use('testdb');

// Insert a test document
db.testCollection.insertOne({
  message: "Hello from VS Code!",
  timestamp: new Date()
});

// Retrieve the document
db.testCollection.find();
```
3. Click the Play button to execute

## Connection String Reference

```
mongodb://admin:admin123@localhost:27017/?authSource=admin
```

**Components**:
- Protocol: `mongodb://`
- Username: `admin`
- Password: `admin123`
- Host: `localhost`
- Port: `27017`
- Auth Database: `authSource=admin`

## Troubleshooting

### Connection Failed
1. Verify MongoDB is running:
   ```powershell
   docker ps --filter "name=mongodb"
   ```

2. Check MongoDB logs:
   ```powershell
   docker logs mongodb
   ```

3. Restart MongoDB:
   ```powershell
   docker restart mongodb
   ```

### Extension Not Showing
- Restart VS Code
- Check if extension is enabled: `Ctrl+Shift+X` → Search "MongoDB"

---
**Connection String**: `mongodb://admin:admin123@localhost:27017/?authSource=admin`
