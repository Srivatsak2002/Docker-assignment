
const { Client } = require('pg');

// Create a new client instance
const client = new Client({
    host: "localhost",
    user: "postgres",
    port: 5432,
    password: "postgres",
    database: "obsrv"
});



// Attempt to connect to the PostgreSQL server
(async () => {
    try {
        await client.connect();
        console.log("Connected to PostgreSQL server");
    } catch (error) {
        console.error("Failed to connnect to PostgreSQL server:");
        
        // Optionally, handle the error here
    }
})();

// Export the client for use in other modules
module.exports = client;

    
